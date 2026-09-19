# Ecom Crawler — dữ liệu sản phẩm song ngữ Trung–Việt từ 1688.com

Crawler phân tán chạy song song trên máy của từng thành viên (`worker_quang`,
`worker_nhat_anh`, `worker_huy`). Mỗi máy claim việc từ một Central Queue
(Firebase Realtime Database), điều khiển Chrome thật để lấy dữ liệu từ 1688,
ghi JSONL vào `data/raw/`, và đẩy bản parquet lên Hugging Face Hub.

Nguồn: 1688.com (sàn B2B nội địa Trung Quốc). Tiêu đề và bảng thuộc tính do
người bán Trung Quốc tự viết; bản tiếng Việt là **máy dịch của Alibaba** cho
các sản phẩm thuộc pool xuyên biên giới (~55-60% sản phẩm). Phần "详情描述" của
1688 là ảnh, không có bản dịch, nên không nằm trong kho ngữ liệu.

## Lịch sử thay đổi gần đây

**17/09 (đã 2 ngày, Huy đang chạy bản này từ sáng 18/09)** — viết lại toàn
bộ crawler sang song ngữ 1 lượt:

- Mỗi dòng có cả `title_zh`/`title_vi` và `description_zh`/`description_vi`
  (bảng thuộc tính + SKU, ghép theo `fid`), không còn crawl zh trước vi sau.
  Xem mục 1.
- Kéo theo: **queue đổi tên** (`queue`→`queue_search_zh`,
  `queue_detail`→`queue_detail_bi` + `queue_detail_zh` là sổ sản phẩm không
  có bản Việt) và **file raw đổi tên** (`1688_zh_*`/`1688detail_*` →
  `1688search_zh_*`/`1688_bilingual_*`/`1688_mono_zh_*`) — đây không phải 2
  thay đổi riêng, chỉ là hệ quả bắt buộc của việc đổi schema. Dữ liệu crawl
  bằng bản trước ngày này không tương thích, không dùng lại được.
- Nếu máy ai đó **vẫn còn chạy nhánh cũ hơn ngày này** (không phải trường
  hợp của Huy) thì cần pull lại — code cũ trỏ tên queue cũ sẽ không nhận
  được việc gì cả.

**19/09 (hôm nay, 2 việc độc lập, không đụng code crawl ở trên):**

- **Chốt phạm vi 8 category**: `fashion, electronics, shoes, bags, beauty,
  mother_baby, food, home`, trần **2.500 sản phẩm/category** (`bags` đã đủ).
  Bỏ `auto, office, sports` (ít tiêu biểu cho TMĐT xuyên biên giới hơn) —
  phần `auto` đã crawl (1.701 dòng) vẫn giữ làm dữ liệu bổ sung, không xoá.
  Chỉ xoá bớt `pending` trong Firebase, **không sửa gì trong engine crawl**.
  **Đừng seed thêm category ngoài 8 cái này.** Chi tiết: mục 7.
- **Cách xem dữ liệu đúng**: `data/snapshot/*.parquet` trên HF là bảng sạch
  để xem (đọc theo schema có từ 17/09, mọi cột `meta` đã tách ra, không phải
  JSON string) — mục 8. `data/raw/`, `data/bronze/` trên HF chỉ là bản sao
  lưu thô, không dùng để xem/phân tích.

## 1. Kiến trúc và luồng dữ liệu

```
Firebase Realtime DB
  queue_search_zh   trang tìm kiếm (từ khoá × trang)      -> giai đoạn search
  queue_detail_bi   trang chi tiết, kèm meta tìm kiếm      -> giai đoạn detail (zh+vi)
  queue_detail_zh   sổ ghi các sản phẩm KHÔNG có bản Việt   (giai đoạn detail tự ghi vào)
        │ claim (transaction) / release / mark_done
   ┌────┴──────────┬───────────────┐
worker_quang  worker_nhat_anh  worker_huy
   │  search:  Search1688Engine  -> data/raw/1688search_zh_*.jsonl   (chỉ raw)
   │  detail:  Detail1688Engine  -> có bản Việt:   data/raw/1688_bilingual_*.jsonl
   │                                                HF data/bronze/bilingual_zh_vi/
   │                             -> không bản Việt: data/raw/1688_mono_zh_*.jsonl
   │                                                HF data/bronze/mono_zh/
   ▼
HF: data/bronze/<folder>/category=<c>/crawled_date=<d>/<worker>_<run_id>_batch_XXX.parquet
HF: data/raw/<tên shard>.jsonl      <- scripts/mirror_raw_to_hf.py sau mỗi giai đoạn
```

Giai đoạn detail lấy **cả hai ngôn ngữ trong cùng một lượt** cho mỗi sản phẩm:
fetch bản `vi` trước (cookie `oversealanguage=vi`); nếu trang trả về tiếng
Trung thì sản phẩm không có bản dịch → dòng đơn ngữ, xong trong một request;
nếu có tiếng Việt thì fetch thêm bản `zh` và ghép hai bản theo `fid` (mã
thuộc tính của 1688, giống nhau ở hai ngôn ngữ).

Mọi text đều được chuẩn hoá trước khi ghi (`core/textnorm.py`): bỏ HTML/entity,
Unicode NFC, chữ/số fullwidth → ASCII (`Ｔ恤` → `T恤`), bỏ ký tự vô hình, gộp
khoảng trắng. 1688 phục vụ UTF-8; chỉ **từ khoá trên URL tìm kiếm** là GBK
(`%C4%D0%CA%BF` = 男士) vì đó là cách 1688 mã hoá — `meta.keyword` là dạng đọc được.

| Class | File | Trách nhiệm |
|---|---|---|
| `QueueManager` | `core/queue_manager.py` | Claim bằng Firebase transaction, mark_done/release, dead-letter sau `MAX_CRAWL_ATTEMPTS`, phục hồi lock mồ côi; `meta` của item được trả về cho engine. |
| `Search1688Engine` | `core/engine_1688_search.py` | Chrome thật + profile `.pw_profile`. Mở trang chủ 1688 **một lần**, gọi API `getOfferList` qua `window.lib.mtop` cho từng (từ khoá, trang): ~3-5s/60 sản phẩm. Gặp CAPTCHA thì chờ người xử lý; bị bóp tốc độ thì tự nghỉ. |
| `Detail1688Engine` | `core/engine_1688_detail.py` | Fetch trang chi tiết từ trong trang (`fetch()` cùng cookie), chế độ `zh+vi` như mô tả trên; ~8-12s/sản phẩm với delay 4-8s. |
| `Search1688Parser` | `parsers/parser_1688_search.py` | JSON mtop → bản ghi tìm kiếm: `price` float, `sales` int (`3.0万+件` → 30000), trường thiếu → `null`. |
| `Detail1688Parser` | `parsers/parser_1688_detail.py` | HTML chi tiết → `DetailPage` (tiêu đề, thuộc tính theo fid, SKU, danh mục 1688, địa điểm, công ty, giá); `build_record` ghép zh+vi thành một dòng. |
| `RawJsonlWriter` / `DataPackager` / `HuggingFaceUploader` | `core/` | Append JSONL `ensure_ascii=False`; gom 1000 bản ghi/parquet; upload có backoff, đường dẫn có `run_id` nên worker khởi động lại không ghi đè. Upload lỗi **không** nhả item về queue. |
| `Worker` | `core/worker.py` | claim → crawl → chuẩn hoá → ghi raw (theo sink song ngữ/đơn ngữ) → mark_done → upload; ghi sản phẩm không có bản Việt vào `queue_detail_zh`. |

### Schema một dòng (giai đoạn detail)

```json
{"product_id": "922936641103",
 "title_zh": "时尚流行拼色个性流行潮流豹纹设计质感斜挎包女单肩包女跨境批发",
 "title_vi": "Thời trang thời trang phối màu cá tính ... túi đeo vai nữ bán buôn xuyên biên giới",
 "description_zh": "材质: PU; 箱包潮流款式: 小方包; 颜色: 咖啡色, 棕色, 黑色, 白色; 货号: AC316; ...",
 "description_vi": "Chất liệu: PU; Túi xách kiểu dáng thời thượng: Túi vuông nhỏ; Màu sắc: Màu cà phê, Màu nâu, Màu đen, Màu trắng; Số mặt hàng: AC316; ...",
 "category": "bags", "source_site": "1688",
 "url": "https://detail.1688.com/offer/922936641103.html",
 "crawl_time": "2026-09-17T12:22:15+00:00", "worker": "worker_nhat_anh",
 "meta": {"keyword": "女包", "page": 1, "search_url": "https://s.1688.com/selloffer/offer_search.htm?keywords=%C5%AE%B0%FC&beginPage=1",
          "price": 17.0, "sales": 30000, "province": "河北省", "city": "保定市",
          "biz_type": "生产加工", "shop": "保定驰誉箱包制造有限公司", "is_ad": false,
          "category_1688": "女士单肩包", "category_id_1688": "201554511",
          "has_vi": true, "n_attributes_zh": 19, "n_attributes_vi": 19, "n_attributes_aligned": 19,
          "attributes_zh": [{"name": "材质", "values": ["PU"]}, "..."],
          "attributes_vi": [{"name": "Chất liệu", "values": ["PU"]}, "..."],
          "description_extra_zh": "", "description_images": 35}}
```

- `description_*` = bảng thuộc tính + tuỳ chọn SKU, dạng `tên: giá trị; ...`,
  **cùng tập fid, cùng thứ tự** ở hai ngôn ngữ (song song từng cặp). Thuộc tính
  chỉ có ở một bên vẫn nằm đủ trong `meta.attributes_zh/vi`.
- Dòng đơn ngữ: `title_vi`, `description_vi` rỗng, `has_vi=false`, nằm ở
  shard/folder `mono_zh`.
- `province/city/shop` lấy từ kết quả tìm kiếm, thiếu thì lấy từ trang chi tiết
  (`location`, `companyName`); `biz_type` thiếu → `null` (không có nguồn khác).
- `description_extra_zh`: chữ (nếu có) trong khối ảnh 详情, thường là giới thiệu
  công ty, chỉ tiếng Trung.

## 2. Cài đặt (mỗi máy)

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

Máy phải có **Google Chrome** cài sẵn (engine dùng `channel="chrome"`, Chromium
headless đi kèm Playwright bị Alibaba chặn 100%).

## 3. Cấu hình `.env`

`cp .env.example .env` rồi điền:

- `WORKER_ID`: một trong `worker_quang`, `worker_nhat_anh`, `worker_huy`.
- `HF_TOKEN`, `HF_REPO_ID` (dạng `ntquang0410/zh-vie_ecom`, không phải URL git).
- `FIREBASE_CRED_PATH`: file JSON service account (mỗi người tự tải, không commit).
- `FIREBASE_DB_URL`. `FIREBASE_QUEUE_PATH`/`CRAWLER_ENGINE`/`SITE_LANGUAGE` do
  `scripts/run_pipeline.py` đặt theo giai đoạn, không cần sửa tay.
- `BROWSER_PROFILE_DIR=.pw_profile`, `HUMAN_WAIT_SECONDS=300`, `RAW_DATA_DIR=data/raw`,
  `PLAYWRIGHT_MIN/MAX_DELAY_SECONDS=4/8` (2-5s dính CAPTCHA sau ~30 trang chi tiết).

Không commit `.env`, file service account, `.pw_profile/` (chứa cookie đăng nhập),
`data/`. Tất cả đã có trong `.gitignore`.

## 4. Firebase rules (một người làm một lần)

`python scripts/firebase_rules.py --apply` (hoặc dán `firebase/database.rules.json`
vào Realtime Database → Rules → Publish). Thiếu `.indexOn: ["status"]` là mọi
worker lỗi ngay khi claim. **Không sửa/xoá node trên Console khi worker đang chạy.**

## 5. Đăng nhập 1688 (một lần cho mỗi máy)

1688 chỉ cho khách ẩn danh xem ~4 trang tìm kiếm rồi bắt đăng nhập. Mỗi thành
viên cần một tài khoản 1688/Taobao (đăng ký được bằng số điện thoại Việt Nam), rồi:

```bash
python scripts/login_1688.py
```

Chrome mở trang đăng nhập; đăng nhập, kéo slider nếu được hỏi. Script tự nhận
ra khi xong và lưu phiên vào `.pw_profile`.

Ngôn ngữ nội dung đi theo cookie `oversealanguage`, engine tự ép trước mỗi
request nên không cần chỉnh tài khoản. Nếu worker dừng với
`AccountLanguageError`, chạy `python scripts/set_1688_language.py zh` rồi chạy lại.

## 6. Crawl mẫu để duyệt format

```bash
python scripts/sample_crawl.py --rows 100
```

Không đụng Firebase/HF: tìm 1 trang cho mỗi danh mục, lấy 100 sản phẩm chia đều,
crawl song ngữ, ghi `data/sample/sample_100.jsonl` và `.csv` (mở được bằng Excel).

## 7. Chạy thật

```bash
python scripts/seed_1688_keywords.py keywords_1688.txt --dry-run   # xem trước
python scripts/seed_1688_keywords.py keywords_1688.txt              # một người, một lần
python scripts/run_pipeline.py search detail                         # mọi máy
```

Runner chạy `main.py` cho từng giai đoạn đến khi queue cạn, tự khởi động lại
worker khi nó thoát (CAPTCHA không ai kéo, rớt mạng), tự seed `queue_detail_bi`
từ raw tìm kiếm khi sang giai đoạn detail, và mirror raw JSONL lên HF sau mỗi
giai đoạn. Cửa sổ Chrome hiện ra và tự chạy; nếu log ra `CAPTCHA / LOGIN WALL`,
kéo slider trong cửa sổ đó.

Trên Windows, `run_crawl.bat` gói sẵn lệnh trên (giai đoạn detail, delay 6-10s)
để chạy trong cửa sổ PowerShell riêng, không phụ thuộc VS Code/Claude — bấm
đúp hoặc `cd ecom_crawler; .\run_crawl.bat`. Đóng cửa sổ đó là dừng crawl,
dữ liệu đã ghi không mất, chạy lại là tiếp tục.

Nếu queue tìm kiếm bị mất: `python scripts/reseed_1688_search.py keywords_1688.txt`
dựng lại từ raw, không crawl trùng. Một profile Chrome không chạy được hai engine
cùng lúc; ba máy thì chia nhau queue, không chia giai đoạn.

### Phạm vi 8 category, trần 2.500/category

`queue_detail_bi` đã được cắt (19/09) chỉ còn 8 category:
`fashion, electronics, shoes, bags, beauty, mother_baby, food, home`, tối đa
2.500 sản phẩm/category (`bags` đã vượt trần, không cần thêm). Ai seed lại từ
đầu hoặc thêm từ khoá mới thì chạy lại để giữ đúng trần, đừng để queue phình
lại như cũ (từng lên tới 65k item do 2 máy seed trùng):

```bash
python scripts/cap_queue_by_category.py --dry-run   # xem trước sẽ xoá gì
python scripts/cap_queue_by_category.py              # xoá pending thừa, không đụng done/processing
```

`dedupe_queue.py` xử lý một vấn đề khác (cùng 1 URL bị seed 2 lần do 2 máy
seed từ raw riêng) — chạy nếu nghi ngờ có trùng, không phải quy trình thường
xuyên.

## 8. Xem dữ liệu / đồng bộ lên Hugging Face

`data/raw/` (log thô, mọi lần ghi) là nguồn sự thật, nhưng lẫn cả dòng tìm
kiếm (chỉ tiêu đề) với dòng chi tiết (song ngữ đầy đủ) và không phải cột
kiểu dữ liệu chuẩn — **không dùng để xem hay phân tích**. Dùng snapshot:

```bash
python scripts/mirror_raw_to_hf.py     # sao lưu data/raw/*.jsonl lên HF (an toàn dữ liệu)
python scripts/snapshot_to_hf.py       # gộp thành 2 bảng sạch để xem/phân tích
```

`snapshot_to_hf.py` đọc toàn bộ `1688_bilingual_*`/`1688_mono_zh_*` cục bộ,
tách `meta` thành cột kiểu dữ liệu thật (`price` float, `sales`/`n_attributes_*`
int, `has_vi`/`is_ad` bool, `province`/`shop`/`category_1688` string — lọc/sort
được ngay, không cần `json.loads`), rồi ghi đè lên:

- `data/snapshot/bilingual_zh_vi.parquet` — sản phẩm có cả 2 ngôn ngữ.
- `data/snapshot/mono_zh.parquet` — sản phẩm chỉ có tiếng Trung.

Hai file này hiện trên [trang HF](https://huggingface.co/datasets/ntquang0410/zh-vie_ecom)
ở tab **Files and versions** (bấm vào file là xem được bảng luôn, không cần
đợi Dataset Viewer tự build — viewer tự động hay gộp lẫn cả 73k dòng tìm kiếm
vào 1 bảng chung, không nên dùng). Chạy 2 lệnh trên sau mỗi lần dừng crawl dài
(không bắt buộc mỗi lô nhỏ) để mọi người xem được dữ liệu mới nhất.

## 9. Việc còn lại

- `data/processed/`: lọc đuôi số lạ trong bản dịch (`Không.2`), dedup MinHash,
  tách dev/test theo `product_id`; không đụng `data/raw/`.
- Engine/parser cho tiếng Việt bản địa (Shopee/Tiki/Lazada) cắm vào cùng queue.
