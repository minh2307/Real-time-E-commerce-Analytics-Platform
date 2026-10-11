# Báo cáo Bài 3: Tối ưu tốc độ CI bằng Gradle Dependency Caching

## 1. Giới thiệu & Mục tiêu
- **Mục tiêu bài thực hành:** Bật tính năng Caching cho Gradle Dependencies trong luồng CI của GitHub Actions (`.github/workflows/ci.yml`) nhằm giảm thời gian tải các thư viện cache Gradle từ Internet mỗi lần luồng CI chạy.
- **Cơ chế hoạt động:** 
  - Tích hợp tham số `cache: 'gradle'` vào action `actions/setup-java@v5`.
  - Action tự động phân tích và tạo mã băm (hash key) dựa trên nội dung các tệp định nghĩa dependencies của Gradle (`build.gradle.kts`, `gradle-wrapper.properties`).
  - Lần chạy đầu tiên (Cold Cache): GitHub Actions tải toàn bộ dependencies từ Maven Central và Gradle Wrapper từ gradle.org, sau đó lưu (save) vào kho lưu trữ cache của GitHub Actions runner.
  - Lần chạy tiếp theo (Warm Cache): GitHub Actions phát hiện cache hit, tự động khôi phục (restore) toàn bộ thư mục `~/.gradle/caches` và `~/.gradle/wrapper` trước khi bước `./gradlew clean test` chạy, giúp loại bỏ hoàn toàn độ trễ tải mạng (network I/O latency).

---

## 2. Kịch bản CI Workflow hoàn chỉnh (`.github/workflows/ci.yml`)

Workflow được cấu hình tại đường dẫn `.github/workflows/ci.yml` (và bản sao nộp bài tại `homework/session_11/ex3/ci.yml`):

```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [ "main", "master" ]
  pull_request:
    branches: [ "main", "master" ]

jobs:
  backend-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: ecommerce
          POSTGRES_PASSWORD: ecommerce_password
          POSTGRES_DB: ecommerce_db
        ports:
          - 15432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4

      - name: Thiết lập môi trường Java JDK 21
        uses: actions/setup-java@v5
        with:
          java-version: '21'
          distribution: 'temurin'
          cache: 'gradle'

      - name: Grant execute permission for gradlew
        working-directory: ./backend
        run: chmod +x gradlew

      - name: Run Backend Tests
        working-directory: ./backend
        run: ./gradlew clean test

  data-pipeline-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        working-directory: ./data-pipeline
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run Pytest
        working-directory: ./data-pipeline
        env:
          PYTHONPATH: ${{ github.workspace }}/data-pipeline
          DATA_DIR: ${{ github.workspace }}/data-pipeline/data
          PYSPARK_PYTHON: python
          PYSPARK_DRIVER_PYTHON: python
        run: |
          pytest tests/
```

---

## 3. Nhật ký kiểm thử & Bằng chứng thực tế trên GitHub Actions

Quá trình kiểm thử được thực hiện qua các lần push thực tế lên repository GitHub (`minh2307/Real-time-E-commerce-Analytics-Platform`).

### 3.1. Thông tin các lần chạy (Workflow Runs)

| Thông số | Lần 1: Chưa có Cache (Tạo Cache) | Lần 2: Đã có Cache (Khôi phục Cache) |
| :--- | :--- | :--- |
| **Run ID** | `38143402370` | `38143724691` |
| **Commit SHA** | `ed5ef12` | `67aa84c` |
| **Commit Message** | `ci: configure postgres service container for backend-tests job` | `docs: add session 11 ex3 report initial draft` |
| **Trạng thái Job** | `completed` (`success`) | `completed` (`success`) |
| **Thời điểm bắt đầu** | 2026-10-11 13:29:56 UTC | 2026-10-11 13:34:36 UTC |
| **Thời điểm kết thúc** | 2026-10-11 13:32:38 UTC | 2026-10-11 13:36:30 UTC |

### 3.2. Bằng chứng Cache Entries trên GitHub Actions Cache Storage

Kiểm tra trực tiếp qua GitHub REST API (`/actions/caches`), hệ thống đã lưu trữ thành công 2 khóa cache:

```json
{
  "total_count": 2,
  "actions_caches": [
    {
      "id": 8797063768,
      "ref": "refs/heads/main",
      "key": "setup-java-Linux-x64-gradle-1ebed3849d59d6101914006c35f36360971754f6feead551b4f1fd32522580bf",
      "created_at": "2026-10-11T13:32:34.837567Z",
      "last_accessed_at": "2026-10-11T13:34:58.457252Z",
      "size_in_bytes": 81421363
    },
    {
      "id": 8797062828,
      "ref": "refs/heads/main",
      "key": "setup-java-Linux-x64-gradle-wrapper-cb10f63b6e80d04c47ba63d0097b1be1d92513baebbf33fb9b91e65214965f4c",
      "created_at": "2026-10-11T13:32:31.735329Z",
      "last_accessed_at": "2026-10-11T13:34:59.780543Z",
      "size_in_bytes": 150685239
    }
  ]
}
```

- **Tổng dung lượng cache lưu trữ:** `81.4 MB` (dependencies) + `150.7 MB` (wrapper) = **~232.1 MB**.
- **Xác thực khôi phục cache:** Trường `last_accessed_at` của cả 2 cache keys được cập nhật vào lúc `13:34:58 UTC`, trùng khớp chính xác với thời điểm thực thi bước **Thiết lập môi trường Java JDK 21** ở lần push thứ 2 (Run `38143724691`).

### 3.3. Dòng log xác nhận khôi phục Cache (Cache Hit)
Trong bước `Thiết lập môi trường Java JDK 21` của Lần push thứ 2:
```text
Received 81421363 of 81421363 (100.0%), 81.4 MBs/sec
Cache restored successfully from key: setup-java-Linux-x64-gradle-1ebed3849d59d6101914006c35f36360971754f6feead551b4f1fd32522580bf
Received 150685239 of 150685239 (100.0%), 150.7 MBs/sec
Cache restored successfully from key: setup-java-Linux-x64-gradle-wrapper-cb10f63b6e80d04c47ba63d0097b1be1d92513baebbf33fb9b91e65214965f4c
```

Trong bước `Post Thiết lập môi trường Java JDK 21` của Lần push thứ 2:
```text
Cache hit occurred on the primary key setup-java-Linux-x64-gradle-1ebed3849d59d6101914006c35f36360971754f6feead551b4f1fd32522580bf, not saving cache.
Cache hit occurred on the primary key setup-java-Linux-x64-gradle-wrapper-cb10f63b6e80d04c47ba63d0097b1be1d92513baebbf33fb9b91e65214965f4c, not saving cache.
```

---

## 4. Bảng so sánh chi tiết thời gian thực thi (Performance Benchmark)

| Thành phần / Bước thực thi (Step) | Lần 1 (Chưa có Cache) | Lần 2 (Đã có Cache) | Chênh lệch (Delta) | Tỷ lệ cải thiện |
| :--- | :---: | :---: | :---: | :---: |
| **Initialize Containers (Postgres)** | 20.0s | 19.0s | -1.0s | ~5% |
| **Run actions/checkout@v4** | 1.0s | 1.0s | 0.0s | 0% |
| **Thiết lập môi trường Java JDK 21** | 1.0s | 3.0s | +2.0s *(restore 232MB)* | — |
| **Grant execute permission for gradlew**| 0.0s | 0.0s | 0.0s | 0% |
| **Run Backend Tests (`./gradlew clean test`)** | **128.0s** | **86.0s** | **-42.0s** | **Nhanh hơn 32.8%** |
| **Post Thiết lập môi trường Java JDK 21** | 6.0s *(Upload cache)* | 1.0s *(Bỏ qua do Hit)* | -5.0s | Nhanh hơn 83.3% |
| **Tổng thời gian Job `backend-tests`** | **162.0s (2m 42s)** | **114.0s (1m 54s)** | **-48.0s** | **Nhanh hơn 29.6%** |

---

## 5. Đánh giá & Kết luận

1. **Hiệu quả tối ưu:**
   - Bước `./gradlew clean test` giảm từ **128 giây** xuống còn **86 giây** (tiết kiệm **42 giây** thời gian tải mạng từ Maven Central).
   - Tổng thời gian hoàn thành của Job `backend-tests` giảm từ **162 giây (2m 42s)** xuống còn **114 giây (1m 54s)**, đạt mức cải thiện **~30%**.
2. **Tiết kiệm băng thông và tài nguyên:**
   - Hơn **232 MB** dữ liệu phụ thuộc không phải tải lại qua Internet mỗi lần developer push code.
   - Giảm thiểu rủi ro nghẽn mạng hoặc lỗi kết nối ngắt quãng (network timeout/rate limit) đến các repository bên ngoài như Maven Central.
3. **Tính ổn định của pipeline:**
   - Việc thiết lập `cache: 'gradle'` trong `actions/setup-java@v5` vừa đơn giản (chỉ 1 dòng cấu hình), vừa tương thích liền mạch với kiến trúc Gradle wrapper và Spring Boot của dự án.
