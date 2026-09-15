# Ecom Crawler — Distributed Master-Worker Crawler → Hugging Face Hub

Hệ thống crawler phân tán chạy song song trên **4 máy** (`worker_quang`,
`worker_nhat_anh`, `worker_huy`). Mỗi máy claim URL
từ một **Central Queue** (Firebase Realtime Database), cào dữ liệu bất đồng bộ
bằng `asyncio`/`aiohttp`, gom đủ 1.000 sản phẩm thành 1 file `.parquet`, rồi
đẩy trực tiếp lên Hugging Face Hub theo cấu trúc Hive-partitioned.

## 1. Kiến trúc

```
┌─────────────────────┐
│ Firebase Realtime DB │  <- Central Queue: pending / processing / done / failed
└─────────┬────────────┘
          │ claim (atomic transaction) / release / mark_done
   ┌──────┴──────┬──────────────┬
   ▼             ▼              ▼ 
worker_quang  worker_nhat_anh  worker_huy
   │ QueueManager -> CrawlerEngine (asyncio+aiohttp) -> DataPackager (1000/parquet)
   │      -> HuggingFaceUploader (exponential backoff on 409/429)
   ▼
Hugging Face Hub: data/bronze/category=<c>/crawled_date=<d>/<worker_id>_batch_XXX.parquet
```

| Class | File | Trách nhiệm |
|---|---|---|
| `QueueManager` | [core/queue_manager.py](core/queue_manager.py) | Lock/claim URL bằng Firebase transaction, mark done/release, dead-letter sau N lần thất bại, tự phục hồi lock "mồ côi" (worker crash). |
| `CrawlerEngine` | [core/crawler_engine.py](core/crawler_engine.py) | Tải & parse trang song song bằng `asyncio`/`aiohttp`, giới hạn concurrency bằng semaphore, retry lỗi mạng. |
| `DataPackager` | [core/data_packager.py](core/data_packager.py) | Gom sản phẩm vào buffer RAM, ghi `.parquet` mỗi khi đủ 1.000 (hoặc flush phần còn dư khi hết queue). |
| `HuggingFaceUploader` | [core/hf_uploader.py](core/hf_uploader.py) | `HfApi.upload_file` + exponential backoff cho `409 Conflict`/`429 Rate Limit`/5xx. |
| `Worker` | [core/worker.py](core/worker.py) | Điều phối toàn bộ vòng lặp, chỉ ack `Done` sau khi upload thành công; upload lỗi -> nhả URL về `Pending`. |
| `GenericProductParser` | [parsers/generic_parser.py](parsers/generic_parser.py) | Parser mẫu (OpenGraph/H1/meta description/table). **Cần viết parser riêng cho từng sàn TMĐT thật** (kế thừa `BaseProductParser`). |

## 2. Cài đặt

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Thiết lập biến môi trường (mỗi thành viên tự làm trên máy mình)

1. Copy file mẫu:
   ```bash
   cp .env.example .env
   ```
2. Mở `.env` và điền:
   - `WORKER_ID`: đúng định danh được cấp cho bạn — một trong
     `worker_quang`, `worker_nhat_anh`, `worker_huy`.
   - `HF_TOKEN`: token **Write** của Hugging Face
     (Settings → Access Tokens → New token). **Không chia sẻ token qua
     chat/email**; nếu lỡ để lộ, vào https://huggingface.co/settings/tokens
     revoke ngay và tạo token mới.
   - `HF_REPO_ID`: dataset repo đích, ví dụ `your-org/ecom-products`.
   - `FIREBASE_CRED_PATH`: đường dẫn tới file JSON service-account Firebase
     (tải từ Firebase Console → Project Settings → Service accounts →
     Generate new private key). **Không commit file này** — đã có trong
     `.gitignore`.
   - `FIREBASE_DB_URL`: URL Realtime Database của project (dạng
     `https://<project>-default-rtdb.<region>.firebasedatabase.app`).
   - Các tham số còn lại (`BATCH_SIZE`, `MAX_CONCURRENT_REQUESTS`, ...) có
     giá trị mặc định hợp lý, chỉnh nếu cần.
3. **Không bao giờ** commit `.env` hoặc file credential Firebase vào git —
   cả hai đã được thêm vào [.gitignore](.gitignore).

## 4. Thiết lập Firebase Realtime Database Rules (chỉ cần làm 1 lần cho cả nhóm)

Vì `QueueManager` dùng `order_by_child("status")` để lọc URL `pending`/
`processing`, Firebase **bắt buộc** phải khai báo index `.indexOn` cho
trường `status`; nếu thiếu, mọi worker sẽ gặp lỗi:

```
firebase_admin.exceptions.InvalidArgumentError: Index not defined, add ".indexOn": "status", for path "/queue", to the rules
```

Cách khắc phục (chỉ 1 thành viên cần làm, vì cả 4 máy dùng chung 1 Firebase
project/database):

1. Mở [Firebase Console](https://console.firebase.google.com/) → chọn project
   → **Realtime Database** → tab **Rules**.
2. Dán nội dung file [firebase/database.rules.json](firebase/database.rules.json)
   vào ô rules (ghi đè nội dung cũ):
   ```json
   {
     "rules": {
       "queue": {
         ".indexOn": ["status"],
         ".read": false,
         ".write": false
       }
     }
   }
   ```
3. Bấm **Publish**.

> Lưu ý: `.read`/`.write: false` chỉ chặn truy cập công khai (client SDK/API
> key). Worker của bạn dùng Admin SDK với service-account credential nên vẫn
> có toàn quyền đọc/ghi bình thường — Admin SDK luôn bỏ qua Security Rules.
>
> Nếu bạn đổi `FIREBASE_QUEUE_PATH` trong `.env` thành giá trị khác `queue`,
> nhớ đổi khóa `"queue"` trong rules cho khớp.

## 5. Seed URL vào Central Queue (chạy 1 lần, từ 1 máy bất kỳ)

```bash
python scripts/seed_queue.py urls.txt --category electronics
```

`urls.txt`: mỗi dòng một URL sản phẩm cần cào.

## 6. Chạy worker (chạy trên cả 4 máy, song song)

```bash
python main.py
```

Mỗi máy tự claim URL còn `pending`, không cần điều phối thủ công — cơ chế
Firebase transaction đảm bảo không worker nào cào trùng URL.

## 7. Tùy biến parser theo sàn TMĐT thật

`GenericProductParser` chỉ là parser mẫu để chạy demo end-to-end. Với sàn
thật (Shopee, Tiki, Lazada, ...), tạo class mới kế thừa
[`BaseProductParser`](parsers/base_parser.py) và inject vào `CrawlerEngine`
trong [main.py](main.py).

## 8. Cấu trúc dữ liệu trên Hugging Face Hub

```
data/bronze/category=electronics/crawled_date=2026-09-15/worker_thanh_tai_batch_001.parquet
```

Mỗi file `.parquet` chứa các cột: `product_id`, `title`, `description`,
`specs` (JSON string — được serialize để giữ schema ổn định giữa các sản
phẩm có bộ khóa specs khác nhau), `category`, `source_url`, `worker_id`,
`crawled_at`.

## 9. Xử lý xung đột khi upload đồng thời

`HuggingFaceUploader.upload_with_backoff` bắt `HfHubHTTPError`, kiểm tra mã
lỗi (`409`, `429`, và các lỗi 5xx tạm thời), rồi retry với exponential
backoff + jitter (mặc định tối đa 6 lần, delay tăng dần tới `UPLOAD_MAX_DELAY_SECONDS`).
Nếu vẫn thất bại sau khi hết retry, batch đó **không** được ack — toàn bộ URL
trong batch được nhả lại `Pending` (hoặc chuyển sang `Failed`/dead-letter nếu
vượt `MAX_CRAWL_ATTEMPTS`) để không mất dữ liệu và tránh crash worker.

