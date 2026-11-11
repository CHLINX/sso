# SSO v1.2.9｜多週期成熟淨報酬驗證

- 執行時間：`2026-11-11T16:28:16.123401+08:00`
- 官方普通股市場－代號鍵：`1,951`
- v1.2.7 原九日 Q5：`3,446`
- v1.2.8 官方普通股 Q5：`4,148`
- 週期：`(1, 3, 5, 10)`
- 全部配對：`33,332`
- 完成淨報酬：`18,605`
- H=10 與 v1.2.8 舊版 747 筆逐筆核對通過。
- Q5 定義及成本公式沿用，不重新訓練。
- 未成熟訊號不計算報酬。
- 不把重複週期當成獨立樣本。
- 不硬湊十檔股票。

## 輸出檔案
- `00_來源檔案與版本核驗.csv` — 1,723 bytes — SHA256 `fff148a98c4ce6f357716ce6b9396c7517bd06abbefc304de465b8ebec66e65c`
- `01_v126原始九日逐欄重現核對.csv` — 7,482 bytes — SHA256 `baa479de88fc3a5b1f2bc5e73955c2b94cbf658cfed9c296f37d015bc2cd8a60`
- `02_v127原凍結Q5訊號明細.csv` — 2,092,018 bytes — SHA256 `1c9be51aeb3963aee6536109e03e229bf5d2036575fc9d0bb82b975d2b43a10b`
- `03_v127原凍結Q5官方普通股訊號.csv` — 410,475 bytes — SHA256 `93e137eaa3158e9df0f42d4b9200b15af1a1404bfe4294fd04fe7f5c17f07acc`
- `04_v128官方普通股Q5特徵與訊號明細.csv` — 2,053,099 bytes — SHA256 `0109c1df3dd6e262572dad068082c1cf29ac830dc95ff48e36a1dfe239a8f8b7`
- `05_v128官方普通股每日Q5摘要.csv` — 1,281 bytes — SHA256 `aebaa599850727e6e07bd5bb779b9a56d20f3db969d9c08e0017406693e9c49e`
- `06_v128官方普通股Q5出現頻率.csv` — 140,848 bytes — SHA256 `96580875cfdca2787b42a56f6017aa00bce227af09a484ec565d998a0fa5f121`
- `07_v128最新交易日官方普通股Q5名單.csv` — 33,685 bytes — SHA256 `df0b543aee8a102659d0a2f86068d55019c78bce08ca69e70fa2fdb4cfcdda46`
- `08_v127與v128Q5名單差異.csv` — 255,983 bytes — SHA256 `63f5ac1fa28b70c365ac823cb751fb2bb310602d245535d453d6b2f4fb69532b`
- `09_每日Q5訊號數對照.csv` — 329 bytes — SHA256 `12df40a5f91bfd8d39e29440d3c3d978bd15b4ccb66565484f819d1c7f3f0665`
- `10_兩種Q5口徑四週期完整觀察排程.csv` — 11,213,433 bytes — SHA256 `ad99d4102f3fd1449f9360f6bcf2fa8862ceeaff7c07f2e6d14f5bcd7586115a`
- `11_H1H3H5H10已成熟逐筆淨報酬.csv` — 6,871,409 bytes — SHA256 `5c6851c9c1b03ef3f21aafa2b1c40daf4ad692d811cb4b925a33fd25e46271ec`
- `12_各週期整體績效摘要.csv` — 3,223 bytes — SHA256 `1b888969afa67bf3053a02fe9bc36ccb175314119685e88484e9023ea6a2bb91`
- `13_各訊號日週期成熟概況.csv` — 10,493 bytes — SHA256 `7a4af6343393aee619c511e50eedff6e3901ccd6743e17289a662748a44c137f`
- `14_個股各週期成熟報酬彙總.csv` — 1,701,232 bytes — SHA256 `1849e04be17a64c998fb999eb3112c8016b13f86a3a7c7a2499ec762f7ec6463`
- `15_跨不同訊號日重複入選個股.csv` — 1,157,470 bytes — SHA256 `2e9041e5a131111fc603c940aea56a7676d69abc10963c0e778be0417c13db9a`
- `16_個股跨週期表現總表.csv` — 1,104,842 bytes — SHA256 `550f88cc7723529055ff39fc39a26bb77f899daa483ae8e40aaa48712cb1c4b8`
- `17_H10與v128舊版逐筆相容性核對.csv` — 514 bytes — SHA256 `4f3a204ea2c995b0e5049480eee748e3aa4dfd36a75d867ffc999ffa8f280f3f`
- `18_價格銜接與暖機例外.csv` — 10,726 bytes — SHA256 `d354a33596be35cffc2e84bb5d0469afe5ec0a2d16aac8073a5d204b28d77e13`
- `19_執行摘要.txt` — 2,746 bytes — SHA256 `ed73b72fed62ab2e1a38b9fc658766063534349fd97f4fbf786d2a545294df35`
- `20_v129多週期成熟淨報酬驗證結論.json` — 13,129 bytes — SHA256 `cab81febfd874291174b83b1a7364643bc195c851d1b379ba5cc52c03939c40e`
- `SSO_v1.2.9_官方普通股Q5多週期成熟淨報酬驗證.py` — 75,518 bytes — SHA256 `efb3f90629c00c1f5f290cc1637e334ce68d1fe0c7ef1415cbf01494dfe934ac`
