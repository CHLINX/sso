# TWSE 2026-11-11 CSV Round-trip 差異診斷

Run ID：20261112_150003_1b9d

## 診斷範圍

- 原始 v1.5.8 執行資料夾：/content/drive/MyDrive/看股最速適/SSO_Market_Data_Continuation_TWSE_TPEX_v1/v1.5.8_官方行情即時驗證與增量續接_20261112_144440_6842
- 官方日期：2026-11-11
- 官方端點：TWSE MI_INDEX
- 查詢類型：ALLBUT0999
- 官方解析筆數：1390
- 官方唯一代號：1390
- 官方原始 JSON SHA256：e4ff525e7611335616ce3d3a28b3a2efadf822f531271e26b2cb98a6e696df63
- 官方 gzip SHA256：a67de6bb81d7e73aac3c17b8f54a8030f952924867dd86f643364262ad88e0fc

## 診斷結果

本次只分析 CSV 轉寫後的欄位差異。
不修改既有資料，不重新下載行情。

## root CSV

{
  "path": "/content/drive/MyDrive/看股最速適/SSO_Market_Data_Continuation_TWSE_TPEX_v1/v1.5.8_官方行情即時驗證與增量續接_20261112_144440_6842/修正_TWSE_20261111.csv",
  "sha256": "b12be818de20557c2241d06c96d78070bc4dcf7c15fb02f961b607902c15439b",
  "rows": 1390,
  "unique_codes": 1390,
  "missing_codes": 0,
  "extra_codes": 0,
  "duplicate_codes": 0,
  "duplicate_rows": 0,
  "missing_schema_fields": [
    "price_change"
  ],
  "field_mismatch_counts": {
    "price_change": 1390
  },
  "field_examples": {
    "price_change": [
      {
        "file": "root",
        "code": "00400A",
        "field": "price_change",
        "official": 0.49,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00401A",
        "field": "price_change",
        "official": 0.39,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00402A",
        "field": "price_change",
        "official": 0.08,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00403A",
        "field": "price_change",
        "official": 0.32,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00404A",
        "field": "price_change",
        "official": 0.27,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00405A",
        "field": "price_change",
        "official": 0.29,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00406A",
        "field": "price_change",
        "official": 0.31,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00407A",
        "field": "price_change",
        "official": 0.31,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00408A",
        "field": "price_change",
        "official": 0.27,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "root",
        "code": "00409A",
        "field": "price_change",
        "official": 0.18,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      }
    ]
  },
  "empty_code_rows": 0,
  "missing_code_examples": [],
  "extra_code_examples": []
}

## nested CSV

{
  "path": "/content/drive/MyDrive/看股最速適/SSO_Market_Data_Continuation_TWSE_TPEX_v1/v1.5.8_官方行情即時驗證與增量續接_20261112_144440_6842/corrections/官方歷史行情/TWSE/20261111.csv",
  "sha256": "b12be818de20557c2241d06c96d78070bc4dcf7c15fb02f961b607902c15439b",
  "rows": 1390,
  "unique_codes": 1390,
  "missing_codes": 0,
  "extra_codes": 0,
  "duplicate_codes": 0,
  "duplicate_rows": 0,
  "missing_schema_fields": [
    "price_change"
  ],
  "field_mismatch_counts": {
    "price_change": 1390
  },
  "field_examples": {
    "price_change": [
      {
        "file": "nested",
        "code": "00400A",
        "field": "price_change",
        "official": 0.49,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00401A",
        "field": "price_change",
        "official": 0.39,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00402A",
        "field": "price_change",
        "official": 0.08,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00403A",
        "field": "price_change",
        "official": 0.32,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00404A",
        "field": "price_change",
        "official": 0.27,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00405A",
        "field": "price_change",
        "official": 0.29,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00406A",
        "field": "price_change",
        "official": 0.31,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00407A",
        "field": "price_change",
        "official": 0.31,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00408A",
        "field": "price_change",
        "official": 0.27,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      },
      {
        "file": "nested",
        "code": "00409A",
        "field": "price_change",
        "official": 0.18,
        "exported": null,
        "difference": "VALUE_MISMATCH"
      }
    ]
  },
  "empty_code_rows": 0,
  "missing_code_examples": [],
  "extra_code_examples": []
}

## root / nested 差異

0 筆

## 所有欄位差異

2780 筆

## 結論

DIAGNOSIS_ONLY_NOT_APPROVED_FOR_SSO

此診斷報告不能單獨視為 TWSE 修正通過。
必須先確認差異僅屬非行情欄位，或重新修正輸出格式並重跑 round-trip。
