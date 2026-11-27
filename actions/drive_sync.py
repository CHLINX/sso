#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only Google Drive bootstrap with duplicate-name SHA256 resolution."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import requests
from pathlib import Path
from typing import Any

BASE_NAME = "相對價格因子與前瞻報酬研究資料_v2.31.csv.gz"
BASE_SHA256 = "113a4f7cd5c94d7bbd3cb78427963dde2a6c249d66a25a66fab2c408958de71c"
DRIVE_API = "https://www.googleapis.com/drive/v3"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_MIME = "application/vnd.google-apps.folder"
INCREMENT_PREFIX = "全市場標準化增量_"
MAX_BASE_CANDIDATES = 20


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def required_env() -> dict[str, str]:
    return {
        "GOOGLE_DRIVE_CREDENTIALS_JSON": os.getenv("GOOGLE_DRIVE_CREDENTIALS_JSON", "").strip(),
        "SSO_BASE_FILE_ID": os.getenv("SSO_BASE_FILE_ID", "").strip(),
        "SSO_INCREMENT_FOLDER_ID": os.getenv("SSO_INCREMENT_FOLDER_ID", "").strip(),
        "FUGLE_API": (os.getenv("FUGLE_API") or os.getenv("FUGLE_API_KEY") or "").strip(),
        "GITHUB_TOKEN": os.getenv("GITHUB_TOKEN", "").strip(),
    }


def error_label(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return f"HTTP {status}" if status is not None else type(exc).__name__


def make_session(cfg: dict[str, str]):
    try:
        info = json.loads(cfg["GOOGLE_DRIVE_CREDENTIALS_JSON"])
    except Exception as exc:
        raise RuntimeError("GOOGLE_DRIVE_CREDENTIALS_JSON 不是有效 JSON") from exc
    if info.get("type") != "service_account" or not info.get("client_email"):
        raise RuntimeError("Google Drive 憑證必須是 service account JSON")
    try:
        from google.oauth2.service_account import Credentials
        from google.auth.transport.requests import AuthorizedSession
    except ImportError as exc:
        raise RuntimeError("缺少 google-auth 套件；請安裝 actions/requirements.txt") from exc
    credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
    return AuthorizedSession(credentials), str(info["client_email"])


def drive_get(session, url: str, params: dict[str, Any] | None = None, stream: bool = False):
    """Google Drive GET with bounded retries for transient network/API failures."""
    max_attempts = 6
    for attempt in range(max_attempts):
        try:
            response = session.get(
                url, params=params or {}, timeout=(20, 120), stream=stream
            )
        except requests.exceptions.RequestException:
            if attempt + 1 >= max_attempts:
                raise
            time.sleep(min(2 ** attempt, 16))
            continue

        if response.status_code in (401, 403):
            status = response.status_code
            response.close()
            raise RuntimeError(
                f"Google Drive 權限不足 HTTP {status}；"
                "請確認 Drive API 已啟用且 service account 有 Viewer 權限"
            )

        if response.status_code == 429 or response.status_code in (500, 502, 503, 504):
            status = response.status_code
            if attempt + 1 >= max_attempts:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After", "")
            try:
                delay = min(max(float(retry_after), 0.0), 30.0) if retry_after else min(2 ** attempt, 16)
            except ValueError:
                delay = min(2 ** attempt, 16)
            response.close()
            time.sleep(delay)
            continue

        response.raise_for_status()
        return response

    raise RuntimeError("Google Drive GET 重試次數已用盡")


def get_metadata(session, file_id: str) -> dict[str, Any]:
    return drive_get(session, f"{DRIVE_API}/files/{file_id}", {
        "fields": "id,name,mimeType,size,modifiedTime,md5Checksum",
        "supportsAllDrives": "true",
    }).json()


def list_named_base_files(session) -> list[dict[str, Any]]:
    """Enumerate every exact-name base file visible to the service account."""
    results: list[dict[str, Any]] = []
    page_token = None
    query_name = BASE_NAME.replace("\\", "\\\\").replace("'", "\\'")
    while True:
        params = {
            "q": f"name = '{query_name}' and trashed = false",
            "pageSize": 1000,
            "orderBy": "modifiedTime desc",
            "corpora": "allDrives",
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
            "fields": "nextPageToken,incompleteSearch,files(id,name,mimeType,size,modifiedTime,md5Checksum)",
        }
        if page_token:
            params["pageToken"] = page_token
        payload = drive_get(session, f"{DRIVE_API}/files", params).json()
        if payload.get("incompleteSearch"):
            raise RuntimeError("Google Drive 回報 incompleteSearch=true，無法保證已檢查所有同名檔")
        results.extend(payload.get("files", []))
        if len(results) > MAX_BASE_CANDIDATES:
            raise RuntimeError(
                f"同名基礎檔超過安全上限 {MAX_BASE_CANDIDATES}，停止自動辨識"
            )
        page_token = payload.get("nextPageToken")
        if not page_token:
            return results


def download_file(session, file_id: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".part")
    response = drive_get(
        session, f"{DRIVE_API}/files/{file_id}",
        {"alt": "media", "supportsAllDrives": "true"}, stream=True,
    )
    try:
        with temp.open("wb") as stream:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    stream.write(chunk)
        temp.replace(target)
    finally:
        response.close()
        temp.unlink(missing_ok=True)


def resolve_base_file(session, pinned_id: str, scratch: Path) -> dict[str, Any]:
    """Try pinned ID and every discovered duplicate; only exact SHA256 is accepted."""
    search_error = None
    try:
        listed = list_named_base_files(session)
    except Exception as exc:
        listed = []
        search_error = error_label(exc)

    candidate_ids: list[str] = []
    if pinned_id:
        candidate_ids.append(pinned_id)
    for item in listed:
        candidate_id = str(item.get("id", ""))
        if candidate_id and candidate_id not in candidate_ids:
            candidate_ids.append(candidate_id)

    checks = []
    verified_ids: list[str] = []
    for index, candidate_id in enumerate(candidate_ids):
        row: dict[str, Any] = {
            "file_name_matches": False,
            "download_ok": False,
            "sha256_matches": False,
        }
        try:
            metadata = get_metadata(session, candidate_id)
            row["file_name_matches"] = (
                metadata.get("name") == BASE_NAME
                and metadata.get("mimeType") != FOLDER_MIME
            )
            if not row["file_name_matches"]:
                row["result"] = "NAME_OR_TYPE_MISMATCH"
            else:
                target = scratch / f"base_candidate_{index}.csv.gz"
                download_file(session, candidate_id, target)
                actual_sha = sha256_file(target)
                row["download_ok"] = True
                row["sha256"] = actual_sha
                row["sha256_matches"] = actual_sha == BASE_SHA256
                row["bytes"] = target.stat().st_size
                row["result"] = "SHA256_MATCH" if row["sha256_matches"] else "SHA256_MISMATCH"
                if row["sha256_matches"]:
                    verified_ids.append(candidate_id)
                target.unlink(missing_ok=True)
        except Exception as exc:
            row["result"] = error_label(exc)
        checks.append(row)

    # Fail closed if the duplicate search failed: the script cannot prove it
    # examined all visible exact-name candidates, even if the pinned ID matched.
    selected_id = None
    if not search_error and verified_ids:
        selected_id = pinned_id if pinned_id in verified_ids else verified_ids[0]

    return {
        "ok": selected_id is not None,
        "selected_id": selected_id,
        "candidate_count": len(candidate_ids),
        "verified_count": len(verified_ids),
        "pinned_id_verified": pinned_id in verified_ids,
        "search_error": search_error,
        "candidate_checks": checks,
    }


def list_children(session, folder_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page_token = None
    while True:
        params = {
            "q": f"'{folder_id}' in parents and trashed = false",
            "pageSize": 1000,
            "orderBy": "name",
            "fields": "nextPageToken,files(id,name,mimeType,size,modifiedTime,md5Checksum)",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if page_token:
            params["pageToken"] = page_token
        payload = drive_get(session, f"{DRIVE_API}/files", params).json()
        items.extend(payload.get("files", []))
        page_token = payload.get("nextPageToken")
        if not page_token:
            return items


def walk_increment_files(session, root_id: str, max_files: int = 500, max_depth: int = 15):
    found = []
    visited: set[str] = set()

    def walk(folder_id: str, rel: tuple[str, ...], depth: int):
        if folder_id in visited:
            return
        if depth > max_depth:
            raise RuntimeError(f"Google Drive 目錄深度超過限制：{max_depth}")
        visited.add(folder_id)
        for item in list_children(session, folder_id):
            name = str(item.get("name", ""))
            if item.get("mimeType") == FOLDER_MIME:
                walk(str(item["id"]), rel + (name,), depth + 1)
            elif name.startswith(INCREMENT_PREFIX) and (
                name.endswith(".csv") or name.endswith(".csv.gz")
            ):
                found.append((item, rel))
                if len(found) > max_files:
                    raise RuntimeError(f"增量檔超過安全上限 {max_files}，停止下載")

    walk(root_id, tuple(), 0)
    found.sort(key=lambda pair: (
        str(pair[0].get("name", "")), str(pair[0].get("modifiedTime", ""))
    ))
    return found


def write_result(out: Path, payload: dict[str, Any], ready: bool | None = None) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "drive_sync_result.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    output = os.getenv("GITHUB_OUTPUT")
    if output and ready is not None:
        with open(output, "a", encoding="utf-8") as stream:
            stream.write(f"ready={'true' if ready else 'false'}\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight-only", action="store_true")
    mode.add_argument("--sync", action="store_true")
    parser.add_argument("--output", default="/tmp/sso-f04-input")
    args = parser.parse_args()

    out = Path(args.output)
    cfg = required_env()
    missing = [key for key, value in cfg.items() if not value]
    if missing:
        payload = {
            "status": "CONFIGURATION_REQUIRED",
            "missing_secret_names": missing,
            "read_only": True,
            "note": "僅列出缺少的 Secret 名稱，不讀取或輸出任何 Secret 值。",
        }
        write_result(out, payload, False)
        print(json.dumps(payload, ensure_ascii=False))
        return 0 if args.preflight_only else 2

    session, service_email = make_session(cfg)
    base_resolution = None
    base_error = None
    folder_meta = None
    folder_error = None
    increments = []
    listing_error = None

    with tempfile.TemporaryDirectory(prefix="sso-base-candidates-") as temp_dir:
        try:
            base_resolution = resolve_base_file(
                session, cfg["SSO_BASE_FILE_ID"], Path(temp_dir)
            )
        except Exception as exc:
            base_error = error_label(exc)

        try:
            folder_meta = get_metadata(session, cfg["SSO_INCREMENT_FOLDER_ID"])
            if folder_meta.get("mimeType") != FOLDER_MIME:
                folder_error = "ID 指向的項目不是 Google Drive 資料夾"
        except Exception as exc:
            folder_error = error_label(exc)

        if folder_meta is not None and folder_error is None:
            try:
                increments = walk_increment_files(session, cfg["SSO_INCREMENT_FOLDER_ID"])
            except Exception as exc:
                listing_error = error_label(exc)

        checks = {
            "base_file": {
                "ok": bool(base_resolution and base_resolution["ok"]),
                "name": BASE_NAME,
                "expected_sha256": BASE_SHA256,
                "candidate_count": base_resolution.get("candidate_count", 0) if base_resolution else 0,
                "verified_candidate_count": base_resolution.get("verified_count", 0) if base_resolution else 0,
                "pinned_id_verified": base_resolution.get("pinned_id_verified", False) if base_resolution else False,
                "search_error": base_resolution.get("search_error") if base_resolution else base_error,
                "candidate_checks": base_resolution.get("candidate_checks", []) if base_resolution else [],
                "error": base_error,
            },
            "increment_folder": {
                "ok": folder_meta is not None and folder_error is None,
                "name": folder_meta.get("name") if folder_meta else None,
                "mime_type": folder_meta.get("mimeType") if folder_meta else None,
                "error": folder_error,
            },
            "increment_listing": {
                "ok": folder_meta is not None and folder_error is None and listing_error is None,
                "file_count": len(increments),
                "error": listing_error or (
                    "未執行：資料夾不可讀取" if folder_meta is None or folder_error else None
                ),
            },
        }
        ready = all(row["ok"] for row in checks.values())

        if args.preflight_only:
            payload = {
                "status": "PREFLIGHT_PASS" if ready else "PREFLIGHT_FAILED",
                "service_account": service_email,
                "checks": checks,
                "increment_file_count": len(increments),
                "read_only": True,
                "note": (
                    "唯讀搜尋同名基礎檔、下載候選檔並驗證 SHA256，"
                    "列增量資料夾清單；不下載行情增量、不呼叫 Fugle、不凍結 Top10。"
                ),
            }
            write_result(out, payload, ready)
            print(json.dumps(payload, ensure_ascii=False))
            return 0

        if not ready:
            blocked_payload = {
                "status": "SYNC_BLOCKED_PREFLIGHT",
                "service_account": service_email,
                "checks": checks,
                "increment_file_count": len(increments),
                "read_only": True,
                "note": "唯讀預檢未通過，未進行正式同步；未呼叫 Fugle、未凍結 Top10。",
            }
            write_result(out, blocked_payload, False)
            raise RuntimeError(f"唯讀預檢未通過：{json.dumps(checks, ensure_ascii=False)}")

        selected_id = base_resolution["selected_id"]
        payload: dict[str, Any] = {
            "status": "SYNC_STARTED",
            "service_account": service_email,
            "base_file": {
                "name": BASE_NAME,
                "expected_sha256": BASE_SHA256,
                "verified_candidate_count": base_resolution["verified_count"],
                "pinned_id_verified": base_resolution["pinned_id_verified"],
            },
            "increment_root": {k: folder_meta.get(k) for k in ("name", "modifiedTime")},
            "increment_file_count": len(increments),
            "increment_files": [
                {
                    "name": row[0]["name"],
                    "modifiedTime": row[0].get("modifiedTime"),
                    "size": row[0].get("size"),
                    "folder_path": list(row[1]),
                }
                for row in increments
            ],
            "read_only": True,
        }
        data_root = out / "data"
        base_path = data_root / "base" / BASE_NAME
        increment_dir = data_root / "increments"
        download_file(session, selected_id, base_path)
        actual_sha = sha256_file(base_path)
        if actual_sha != BASE_SHA256:
            base_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"v2.31 基礎檔 SHA256 不符，拒絕執行：actual={actual_sha}; expected={BASE_SHA256}"
            )

        downloaded = []
        for item, folders in increments:
            safe_parts = [re.sub(r"[^\w.\-() ]", "_", str(part)) for part in folders]
            target = increment_dir.joinpath(*safe_parts, str(item["name"]))
            download_file(session, str(item["id"]), target)
            downloaded.append({
                "name": item["name"],
                "path": str(target),
                "bytes": target.stat().st_size,
                "sha256": sha256_file(target),
                "modifiedTime": item.get("modifiedTime"),
            })
        payload.update({
            "status": "SYNC_PASS",
            "base_local_path": str(base_path),
            "base_sha256": actual_sha,
            "base_bytes": base_path.stat().st_size,
            "increment_local_dir": str(increment_dir),
            "downloaded_increment_files": downloaded,
            "downloaded_increment_count": len(downloaded),
            "total_increment_bytes": sum(row["bytes"] for row in downloaded),
        })
        write_result(out, payload, True)
        print(json.dumps({k: v for k, v in payload.items() if k != "increment_files"}, ensure_ascii=False))
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[DRIVE SYNC FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
