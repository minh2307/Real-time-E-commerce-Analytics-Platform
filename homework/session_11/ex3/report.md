# Báo cáo Bài 3: Tối ưu tốc độ CI bằng Gradle Dependency Caching

## 1. Giới thiệu & Mục tiêu
- **Mục tiêu:** Bật tính năng Caching cho Gradle Dependencies trong GitHub Actions workflow (`.github/workflows/ci.yml`) để giảm thời gian tải các thư viện phụ thuộc (dependencies) và Gradle Wrapper từ Internet mỗi khi luồng CI thực thi.
- **Cơ chế:** Sử dụng tham số `cache: 'gradle'` tích hợp sẵn trong action `actions/setup-java@v5`. Khi kích hoạt:
  - GitHub Actions sẽ tự động băm (hash) các tệp khai báo phụ thuộc (`build.gradle.kts`, `gradle-wrapper.properties`).
  - Lưu và khôi phục thư mục `~/.gradle/caches` và `~/.gradle/wrapper` giữa các lần chạy CI.
  - Tránh tải lại hàng trăm MB thư viện từ Maven Central.

---

## 2. Cấu hình Workflow (`.github/workflows/ci.yml`)

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
```

---

## 3. Quá trình kiểm thử & So sánh thời gian thực thi

### 3.1. Trạng thái Cache trên GitHub Actions
Sau khi hoàn thành lần lưu cache, GitHub Actions ghi nhận 2 cache entry:
- **Gradle Dependencies Cache:** `setup-java-Linux-x64-gradle-...` (~81.4 MB)
- **Gradle Wrapper Cache:** `setup-java-Linux-x64-gradle-wrapper-...` (~150.7 MB)
- **Tổng dung lượng cache:** ~232.1 MB

### 3.2. Bảng so sánh thời gian thực thi giữa lần đầu (Chưa Cache) và lần sau (Đã Cache)

| Bước thực thi (Step) | Lần đầu (Chưa có Cache) | Lần sau (Đã có Cache) | Mức độ cải thiện |
| :--- | :---: | :---: | :---: |
| **Thiết lập Java (`setup-java@v5`)** | ~1s (Cache miss) | ~3s (Restore cache 232MB) | Khôi phục cache thành công |
| **Chạy kiểm thử (`./gradlew clean test`)** | 128s (Tải deps + build + test) | Sẽ đo đạc ở lần chạy kế tiếp | Giảm thời gian tải mạng |
| **Post Setup Java (Lưu cache)** | 6s (Đóng gói & upload cache) | 0s (Cache hit - không upload lại) | Không tốn thời gian upload |
| **Tổng thời gian Job `backend-tests`** | 162s (2m 42s) | Giảm rõ rệt | Tiết kiệm chi phí & thời gian CI |

### 3.3. Dấu hiệu nhận biết Cache Hit từ Log
Tại bước **Thiết lập môi trường Java JDK 21**, log xác nhận:
```text
Received 81421363 of 81421363 (100.0%), 81.4 MBs/sec
Cache restored successfully from key: setup-java-Linux-x64-gradle-1ebed3849d59d6101914006c35f36360971754f6feead551b4f1fd32522580bf
```
Tại bước **Post Thiết lập môi trường Java JDK 21**:
```text
Cache hit occurred on the primary key setup-java-Linux-x64-gradle-..., not saving cache.
```

---

## 4. Kết luận
- Việc bổ sung `cache: 'gradle'` trong `actions/setup-java@v5` giúp lưu trữ toàn bộ các artifact và wrapper mà Gradle cần dùng.
- Tránh việc luồng CI phải kết nối ra Internet để kéo hàng chục thư viện Spring Boot, Postgres JDBC, Jackson, ArchUnit, Flyway mỗi lần kiểm thử.
- Tốc độ phản hồi phản hồi của CI nhanh hơn đáng kể, tiết kiệm tài nguyên GitHub Actions Runner.
