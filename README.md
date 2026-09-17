# Ecom Crawler — thu thập tiêu đề sản phẩm tiếng Trung từ 1688.com

Crawler phân tán chạy song song trên máy của từng thành viên (`worker_quang`,
`worker_nhat_anh`, `worker_huy`). Mỗi máy claim **trang tìm kiếm 1688** từ một
Central Queue (Firebase Realtime Database), điều khiển Chrome thật để bắt API
danh sách sản phẩm, ghi JSONL vào `data/raw/`, và đẩy bản parquet lên
Hugging Face Hub làm bản sao.

Nguồn: 1688.com (sàn B2B nội địa Trung Quốc). Tiêu đề do người bán Trung Quốc
tự viết, không qua máy dịch. Alibaba.com **không** dùng được: chỉ có tiêu đề
tiếng Anh.

## 1. Kiến trúc

```
Firebase Realtime DB  (queue: pending / processing / done / failed)
        │ claim (transaction) / release / mark_done
   ┌────┴──────────┬───────────────┐
worker_quang  worker_nhat_anh  worker_huy
   │ QueueManager -> Search1688Engine (Chrome + profile, bắt XHR getOfferList)
   │      -> Search1688Parser -> RawJsonlWriter (data/raw/*.jsonl, ack ngay)
   │      -> DataPackager (1000/parquet) -> HuggingFaceUploader (bản sao)
   ▼
data/raw/1688_zh_<YYYYMMDD>_<worker>_<NNN>.jsonl   <- nguồn sự thật, không sửa
HF: data/bronze/category=<c>/crawled_date=<d>/<worker>_batch_XXX.parquet
```

| Class | File | Trách nhiệm |
|---|---|---|
| `QueueManager` | `core/queue_manager.py` | Claim bằng Firebase transaction, mark_done/release, dead-letter sau `MAX_CRAWL_ATTEMPTS`, phục hồi lock mồ côi. |
| `Search1688Engine` | `core/engine_1688_search.py` | Chrome thật + profile `.pw_profile`. Mở trang chủ 1688 **một lần**, rồi gọi API `getOfferList` qua thư viện `window.lib.mtop` có sẵn trên trang (`page.evaluate`) cho từng (từ khoá, trang): ~1-2s/60 sản phẩm, không điều hướng thêm. Delay ngẫu nhiên 1-3s giữa các lần gọi. Gặp CAPTCHA/đăng nhập (kể cả CAPTCHA do API trả về) thì dừng chờ người xử lý trong cửa sổ. Bỏ qua các trang sau khi từ khoá đã cạn (`hasMore=false`). Từ chối chạy nếu tài khoản đang trả tiêu đề máy dịch. |
| `Search1688Parser` | `parsers/parser_1688_search.py` | JSON mtop → `ProductRecord`. Bỏ markup highlight, giữ emoji. Từ khoá trong URL hàng đợi được giải mã **GBK**. |
| `RawJsonlWriter` | `core/raw_writer.py` | Append JSONL `ensure_ascii=False`, xoay shard mỗi `RAW_SHARD_MAX_MB`. |
| `DataPackager` / `HuggingFaceUploader` | `core/` | Gom 1000 bản ghi thành parquet, upload có backoff. Upload lỗi **không** nhả trang về queue, chỉ giữ file parquet lại. |
| `Worker` | `core/worker.py` | Vòng lặp claim → crawl → ghi raw → mark_done → upload. |

Schema mỗi bản ghi (CLAUDE.md mục 4):

```json
{"product_id": "901883549775", "title_zh": "中德BYB22000短袖夏季男女生纯棉T恤衫...",
 "category": "fashion", "source_site": "1688",
 "url": "https://detail.1688.com/offer/901883549775.html",
 "crawl_time": "2026-09-16T08:12:03+00:00", "worker": "worker_nhat_anh",
 "meta": {"keyword": "男士T恤", "page": 1, "price": "20", "sales": "全网3600+件",
          "province": "江西", "city": "抚州市", "biz_type": "生产加工",
          "shop": "江西中德服饰", "is_ad": true, "search_url": "..."}}
```

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
- `FIREBASE_DB_URL`, `FIREBASE_QUEUE_PATH`.
- `CRAWLER_ENGINE=1688_search`, `BROWSER_PROFILE_DIR=.pw_profile`, `HUMAN_WAIT_SECONDS=300`.
- `RAW_DATA_DIR=data/raw`.

Không commit `.env`, file service account, `.pw_profile/` (chứa cookie đăng nhập),
`data/`. Tất cả đã có trong `.gitignore`.

## 4. Firebase rules (một người làm một lần)

Dán `firebase/database.rules.json` vào Realtime Database → Rules → Publish.
Thiếu `.indexOn: ["status"]` là mọi worker lỗi ngay khi claim.

## 5. Đăng nhập 1688 (một lần cho mỗi máy)

1688 chỉ cho khách ẩn danh xem ~4 trang tìm kiếm rồi chuyển sang trang đăng
nhập. Mỗi thành viên cần một tài khoản Taobao (đăng ký được bằng số điện thoại
Việt Nam tại world.taobao.com), rồi:

```bash
python scripts/login_1688.py
```

Chrome mở trang đăng nhập, đăng nhập bằng mật khẩu hoặc quét QR bằng app Taobao,
kéo slider nếu được hỏi. Script tự nhận ra khi xong và lưu phiên vào `.pw_profile`.

### Ngôn ngữ nội dung (`SITE_LANGUAGE`)

Tài khoản đăng ký bằng số điện thoại ngoài Trung Quốc bị 1688 coi là người
mua nước ngoài và **tự dịch máy** tiêu đề/thuộc tính sang ngôn ngữ trong cookie
`oversealanguage` (en/vi/...). Các API của 1688 tuân theo cookie này tại thời
điểm gọi, còn server thì đặt lại cookie theo hồ sơ tài khoản mỗi lần tải trang.
Engine vì thế **ép cookie trước mỗi lần gọi** theo biến `SITE_LANGUAGE` trong
`.env`:

- `SITE_LANGUAGE=zh` (mặc định): tiêu đề tiếng Trung gốc của người bán — kho
  ngữ liệu chính. Ghi vào `title_zh`, file `1688_zh_*.jsonl`.
- `SITE_LANGUAGE=vi`: bản **máy dịch của Alibaba** sang tiếng Việt. Ghi vào
  `title_vi`, file `1688_vi_*.jsonl`. Chỉ dùng làm dữ liệu bạc (train vòng 1,
  ứng viên từ điển thuật ngữ), gắn nhãn `source=alibaba_mt`, **không bao giờ**
  dùng cho dev/test. Chất lượng: đọc hiểu được nhưng có đủ 3 dấu hiệu máy dịch
  (trật tự từ Trung, liệt kê máy móc, thuật ngữ word-by-word).

Nếu 1688 trả nội dung sai ngôn ngữ 3 lần liên tiếp, worker dừng với
`AccountLanguageError`; khi đó chỉnh lại hồ sơ tài khoản bằng
`python scripts/set_1688_language.py zh` (script mở bảng "Target Market,
Language & Currency", chọn ngôn ngữ, Save, và kiểm chứng bằng chính API).

## 6. Seed hàng đợi (một người, một lần)

`keywords_1688.txt`: mỗi dòng `category<TAB>keyword`. Mỗi từ khoá sinh
`--pages` trang (mặc định 34, vì 1688 chặn ở ~2000 kết quả × 60/trang).

```bash
python scripts/seed_1688_keywords.py keywords_1688.txt --dry-run   # xem trước
python scripts/seed_1688_keywords.py keywords_1688.txt
```

## 7. Chạy worker (mọi máy)

```bash
python main.py
```

Cửa sổ Chrome hiện ra và tự chạy. Nếu log in ra `CAPTCHA / LOGIN WALL`, xử lý
trong cửa sổ đó (kéo slider / đăng nhập lại), worker tự tiếp tục. Quá
`HUMAN_WAIT_SECONDS` không ai xử lý thì worker dừng, trang đang giữ sẽ được
máy khác claim lại sau `LOCK_TIMEOUT_SECONDS`.

Dữ liệu nằm ở `data/raw/` ngay sau mỗi lô — kể cả khi upload HF thất bại.

## 8. Pass 2 — thuộc tính / mô tả sản phẩm (`CRAWLER_ENGINE=1688_detail`)

Trang chi tiết 1688 cho thêm **bảng thuộc tính 商品属性** (20-50 cặp
`tên: giá trị`, ví dụ `主面料成分: 棉; 版型: 宽松型; 颜色: 黑色, 白色`) và tên danh
mục do 1688 gán (`leafCategoryName`, ví dụ `男式T恤`). Phần "详情描述" gần như
chỉ là ảnh, engine chỉ giữ lại chữ nếu có.

Cách chạy (sau khi pass tìm kiếm đã có dữ liệu trong `data/raw/1688_zh_*.jsonl`):

```bash
python scripts/seed_1688_details.py --per-category 2000   # hoặc bỏ --per-category để lấy hết
# trong .env: CRAWLER_ENGINE=1688_detail
python main.py
```

Kết quả ghi vào `data/raw/1688detail_zh_*.jsonl`, mỗi dòng một sản phẩm, nối với
dữ liệu tiêu đề qua `product_id`. `meta.attributes` là dict thuộc tính,
`meta.attributes_text` là dạng chuỗi `k: v; k: v`, `meta.category_1688` là danh
mục của 1688. Script seed tự bỏ qua sản phẩm đã có trong file detail, chạy lại
nhiều lần được.

Cùng một profile Chrome không chạy được hai engine song song: chạy xong pass
tìm kiếm rồi mới chạy pass chi tiết (hoặc mỗi máy một pass). Tốc độ ~3-5 giây
mỗi sản phẩm; 1688 bóp tốc độ sau vài trăm request liên tục, engine tự nghỉ.

## 9. Việc còn lại

- Dedup MinHash/LSH trên `data/processed/`, không đụng `data/raw/`.
- Engine/parser cho phía tiếng Việt (Shopee/Tiki/Lazada) cắm vào cùng queue.
