#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only Google Drive bootstrap for SSO F04 GitHub Actions."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests

BASE_NAME = "相對價格因子與前瞻報酬研究資料_v2.31.csv.gz"
BASE_SHA256 = "113a4f7cd5c94d7bbd3cb78427963dde2a6c249d66a25a66fab2c408958de71c"
DRIVE_API = "https://www.googleapis.com/drive/v3"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_MIME = "application/vnd.google-apps.folder"
INCREMENT_PREFIX = "全市場標準化增量_"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def required_env() -> dict[str, str]:
    return {
        "GOOGLE_DRIVE_CREDENTIALS_JSON": os.getenv("GOOGLE_DRIVE_CREDENTIALS_JSON", "").strip(),
        "SSO_BASE_FILE_ID": os.getenv("SSO_BASE_FILE_ID", "").strip(),
        "SSO_INCREMENT_FOLDER_ID": os.getenv("SSO_INCREMENT_FOLDER_ID", "").strip(),
        "FUGLE_API": (os.getenv("FUGLE_API") or os.getenv("FUGLE_API_KEY") or "").strip(),
        "GITHUB_TOKEN": os.getenv("GITHUB_TOKEN", "").strip(),
    }


def missing_config(cfg: dict[str, str]) -> list[str]:
    return [key for key, value in cfg.items() if not value]


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
        raise RuntimeError("缺少 google-auth 套件；請先安裝 actions/requirements.txt") from exc
    credentials = Credentials.from_service_account_info(info, scopes=SCOPES)
    return AuthorizedSession(credentials), str(info["client_email"])


def drive_get(session, url: str, params: dict[str, Any] | None = None, stream: bool = False):
    response = session.get(url, params=params or {}, timeout=(20, 120), stream=stream)
    if response.status_code in (401, 403):
        raise RuntimeError(f"Google Drive 權限不足 HTTP {response.status_code}；請確認 Drive API 已啟用且 service account 有 Viewer 權限")
    response.raise_for_status()
    return response


def get_metadata(session, file_id: str) -> dict[str, Any]:
    return drive_get(session, f"{DRIVE_API}/files/{file_id}", {
        "fields": "id,name,mimeType,size,modifiedTime,md5Checksum",
        "supportsAllDrives": "true",
    }).json()


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
            elif name.startswith(INCREMENT_PREFIX) and (name.endswith(".csv") or name.endswith(".csv.gz")):
                found.append((item, rel))
                if len(found) > max_files:
                    raise RuntimeError(f"增量檔超過安全上限 {max_files}，停止下載")

    walk(root_id, tuple(), 0)
    found.sort(key=lambda x: (str(x[0].get("name", "")), str(x[0].get("modifiedTime", ""))))
    return found


def download_file(session, file_id: str, target: Path):
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".part")
    response = drive_get(session, f"{DRIVE_API}/files/{file_id}", {
        "alt": "media", "supportsAllDrives": "true",
    }, stream=True)
    with temp.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                f.write(chunk)
    temp.replace(target)


def write_result(out: Path, payload: dict[str, Any], ready: bool | None = None):
    out.mkdir(parents=True, exist_ok=True)
    (out / "drive_sync_result.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    output = os.getenv("GITHUB_OUTPUT")
    if output and ready is not None:
        with open(output, "a", encoding="utf-8") as f:
            f.write(f"ready={'true' if ready else 'false'}\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--output", default="/tmp/sso-f04-input")
    args = parser.parse_args()
    if args.preflight_only == args.sync:
        parser.error("請且只指定 --preflight-only 或 --sync")

    out = Path(args.output)
    cfg = required_env()
    missing = missing_config(cfg)
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
    base_meta = get_metadata(session, cfg["SSO_BASE_FILE_ID"])
    if base_meta.get("name") != BASE_NAME:
        raise RuntimeError(f"SSO_BASE_FILE_ID 指向錯誤檔案：{base_meta.get('name')!r}")
    if base_meta.get("mimeType") == FOLDER_MIME:
        raise RuntimeError("SSO_BASE_FILE_ID 指向資料夾，不是基礎 CSV 檔案")
    folder_meta = get_metadata(session, cfg["SSO_INCREMENT_FOLDER_ID"])
    if folder_meta.get("mimeType") != FOLDER_MIME:
        raise RuntimeError("SSO_INCREMENT_FOLDER_ID 不是 Google Drive 資料夾")
    increments = walk_increment_files(session, cfg["SSO_INCREMENT_FOLDER_ID"])
    payload: dict[str, Any] = {
        "status": "PREFLIGHT_PASS" if args.preflight_only else "SYNC_STARTED",
        "service_account": service_email,
        "base_file": {k: base_meta.get(k) for k in ("name", "size", "modifiedTime", "md5Checksum")},
        "increment_root": {k: folder_meta.get(k) for k in ("name", "modifiedTime")},
        "increment_file_count": len(increments),
        "increment_files": [
            {"name": x[0]["name"], "modifiedTime": x[0].get("modifiedTime"),
             "size": x[0].get("size"), "folder_path": list(x[1])}
            for x in increments
        ],
        "read_only": True,
    }
    if args.preflight_only:
        write_result(out, payload, True)
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    data_root = out / "data"
    base_path = data_root / "base" / BASE_NAME
    increment_dir = data_root / "increments"
    download_file(session, cfg["SSO_BASE_FILE_ID"], base_path)
    actual_sha = sha256_file(base_path)
    if actual_sha != BASE_SHA256:
        base_path.unlink(missing_ok=True)
        raise RuntimeError(f"v2.31 基礎檔 SHA256 不符，拒絕執行：actual={actual_sha}; expected={BASE_SHA256}")

    downloaded = []
    for item, folders in increments:
        safe_parts = [re.sub(r"[^\w.\-() ]", "_", str(p)) for p in folders]
        target = increment_dir.joinpath(*safe_parts, str(item["name"]))
        download_file(session, str(item["id"]), target)
        downloaded.append({
            "name": item["name"], "path": str(target), "bytes": target.stat().st_size,
            "sha256": sha256_file(target), "modifiedTime": item.get("modifiedTime"),
        })
    payload.update({
        "status": "SYNC_PASS",
        "base_local_path": str(base_path),
        "base_sha256": actual_sha,
        "base_bytes": base_path.stat().st_size,
        "increment_local_dir": str(increment_dir),
        "downloaded_increment_files": downloaded,
        "downloaded_increment_count": len(downloaded),
        "total_increment_bytes": sum(x["bytes"] for x in downloaded),
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
