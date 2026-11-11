# SSO v1.3.1｜歷史 OHLC 與逐日證券分類來源精確驗證

- 執行時間：`2026-11-11T23:47:12.006744+08:00`
- 完整 OHLC 跨日期候選：`6,406`
- 2020 年前 OHLC 候選：`1,249`
- CFI 多日期/有效期間分類候選：`0`
- CFI 靜態名錄：`8`

判定採內容欄位及資料列，不以檔名/路徑推定 OHLC 或分類。
listed_date 不視為分類有效日期。SHA256 對解壓後內容計算。
Drive 原始來源唯讀。GitHub 僅新增 audit branch，不更新 main。

## 成果檔案 SHA256
- `00_其他實體檔案清單.csv` — 32,996 bytes — `7d2ca3f24c657ffec88151a6e0956d805d68be4ecab606a20fbdf73507fd236b`
- `01_ZIP容器清單.csv` — 26,483 bytes — `f9bc4d87bdb4e78b6039cbafc0d8154061c123f102d15fd6bc3830d2c350bfdd`
- `02_ZIP內表格成員完整清單.csv` — 1,549,953 bytes — `0d4f7f0ec251dd288f02d35c006f5fdd5003a0e546fb9ceb93214ba591b8f1e5`
- `03_表格內容及SHA256精確檢查.csv` — 8,981,019 bytes — `963c67dec49f911eb4405e68ce8f63c108964720f7e7824f263f96fc9c0f4839`
- `04_JSON結構化內容檢查.csv` — 5,160,214 bytes — `ecafadc378730c22fb0a9778d514fc7e1b16fc09c873b39bbe6a191f9fef70de`
- `05_解壓後內容SHA256去重群組.csv` — 5,812,460 bytes — `52849b46fb7660aba5dbf39ea8bcc745f3af393ab4be75bcbf6f8a74dd4c30b5`
- `06_完整OHLC跨日期來源候選.csv` — 6,001,087 bytes — `7d3413e28c75b2fe1b2a2c90c79281c8abc078fe25fe267d96789423af74d6e9`
- `07_完整OHLC唯一內容來源.csv` — 3,427,776 bytes — `47c1e710c71046cc5aa58671458f36a93cd3a406b63973a81baf831881cb860c`
- `08_內容證實2020年前OHLC來源.csv` — 1,321,758 bytes — `1459a8887653d4d16eabb5682ec42a40bb7fa271923ff649bc1f8b1956bd5875`
- `09_CFI多日期或有效期間歷史分類候選.csv` — 839 bytes — `4ce0bbd51991e257ba64a87ac48435f1f77b368520abbc93fefaf17515dd76cc`
- `10_CFI靜態名錄及單日分類來源.csv` — 52,746 bytes — `db714c16414ac4e12ae209b373dac9881766ae5d05ccf6a3d49f34aec665335f`
- `11_部分價格欄位及未達完整OHLC來源.csv` — 28,694 bytes — `c73d5a4b6d88e478d493b5e8391e7e0745497ca970e1e3b4a0c3ae64e0e32819`
- `12_ZIP容器索引錯誤.csv` — 4 bytes — `b42f2099187886def637d6aa840022266e05cb6c987a9394e708e23cd505eb46`
- `13_v131精確驗證結論.json` — 1,680 bytes — `0f10432fc49e5aeb01adf3ae96d781e6e28c5ec3dfe51b5ae8f9b03120443b83`
- `14_執行摘要.txt` — 2,253 bytes — `aefdc443f62827148278cb2b58ffd020e056bc995ab9ead10fd67e4842d5c75e`
- `SSO_v1.3.1_歷史OHLC與逐日證券分類來源精確驗證.py` — 82,658 bytes — `c37dc299110f27838c0f2ed98cd4be7b9ad4df2f4efe1af23764b0ce14ed413e`
