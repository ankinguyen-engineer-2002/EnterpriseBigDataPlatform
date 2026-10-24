# Step-by-Step Build Guide — Enterprise Commerce Data Platform

> **Mục tiêu dự án:** Xây dựng Data Platform chuẩn doanh nghiệp (`Kafka, Spark, Flink, Delta Lake, dbt, ClickHouse, Airflow, DataHub`), xử lý `>= 100 GB` Batch và tốc độ `200–500 MB/s` (đỉnh `0.5–1 GB/s`).

---

## 📌 BẢN ĐỒ TỔNG THỂ 7 STAGES & CHỐT CHẶN RUBRIC (MASTER ROADMAP — 100/100 ĐIỂM DE)

### Sơ đồ Luồng Thực Thi Tuần Tự (Sequential Build & Cloud Promotion Flow)

```mermaid
flowchart TD
    subgraph S0["STAGE 0: NỀN TẢNG LOCAL & CLOUD TENANCY (ĐÃ XONG 100%)"]
        B1["Bước 1–7: Ubuntu 24.04 ARM64 (OrbStack)\nUser dev + Docker + Git/GitHub + .env"]
        B7_1["Bước 7.1: Oracle Cloud (us-phoenix-1)\nTenancy CloudBigDataPlatform + oci-cloud MCP\n+ Terraform Dry-Run (AD-1 ARM64 + AD-2 AMD)"]
        B1 --> B7_1
    end

    subgraph S1["STAGE 1: STORAGE CORE & CONTAINER OPTIMIZATION (5 ĐIỂM)"]
        B8["Bước 8–8.2: PostgreSQL 16 (WAL=logical)\n+ MinIO S3 (Verified: 878 MB/s Write)"]
        B9["Bước 9–10: Makefile chuẩn hóa lệnh\n(make up/down/ps/logs/test) + Git Push"]
        B11["Bước 11 (R3–R4 | 5đ): Multi-stage Dockerfile\n(uv/wheel + slim non-root, giảm >60% size)"]
        B8 --> B9 --> B11
    end

    subgraph S2["STAGE 2: VECTORIZED GENERATOR (>=100 GB) & CDC INGESTION (12 ĐIỂM)"]
        B12["Bước 12–13: OLTP Schema + Legacy SQL Report\n+ FastAPI Order Service (Pydantic)"]
        B14["Bước 14 (R5–R8 | 8đ): PyArrow/DuckDB Generator\n(>=100GB Batch: Skew 80%, Schema Evol, Dup 2%)"]
        B15["Bước 15 (R9–R10 | 4đ): Kafka KRaft + Debezium CDC\n+ DLQ (Stream Late 5-10m & Dup 1.5%)"]
        B12 --> B14 --> B15
    end

    subgraph S3["STAGE 3: REAL-TIME STREAMING — APACHE FLINK (10 ĐIỂM)"]
        B16["Bước 16–17 (R16–R17 | 5đ): Flink Baseline\n+ Watermarks & Side Outputs (Late 5-10m)"]
        B18["Bước 18–19 (R18–R19 | 5đ): KeyedProcessFunction\n(StateTTL Dedup 1.5%) + Windows -> Delta Bronze"]
        B16 --> B18
    end

    subgraph S4["STAGE 4: BATCH LAKEHOUSE — APACHE SPARK & DELTA/ICEBERG UNIFORM (16 ĐIỂM)"]
        B20["Bước 20 (R11–R15 | 13đ): Spark Bronze -> Silver\n(AQE/Salting Skew, Schema Merge, Dedup 2%)"]
        B21["Bước 21 (R20 | 3đ): Delta Lake Optimization\n(Partitioning, ZSTD, OPTIMIZE, Z-ORDER + Iceberg UniForm)"]
        B20 --> B21
    end

    subgraph S5["STAGE 5: DIMENSIONAL MODELING (dbt) & OLAP (ClickHouse) (13 ĐIỂM)"]
        B22["Bước 22 (R34–R38 | 10đ): dbt Core (staging/int/marts)\nSCD Type 2 + SHA-256 PII + Slim CI + feat_ table"]
        B23["Bước 23 (R21 | 3đ): ClickHouse Serving\n(ReplacingMergeTree, Skipping Index, MatViews)"]
        B22 --> B23
    end

    subgraph S6["STAGE 6: ORCHESTRATION (Airflow 3) & GOVERNANCE (DataHub) (24 ĐIỂM)"]
        B24["Bước 24 (R22–R27 | 12đ): Airflow 3 DAGs (DP1, DP2, DP3)\n+ Custom OOP Plugins (WAPOperator, CHHook + pytest)"]
        B25["Bước 25 (R28–R33 | 12đ): DataHub End-to-End Lineage\n+ PII Classification Tags + Quality Assertions"]
        B24 --> B25
    end

    subgraph S7["STAGE 7: WAP RECONCILIATION, OBSERVABILITY & OCI CLOUD IaC (20 ĐIỂM)"]
        B26["Bước 26 (R39 | 10đ): WAP Zero-Copy Gate (DuckDB)\nĐối chiếu Legacy SQL vs Gold (Lệch != 0 -> RESTORE)"]
        B27["Bước 27 (R40A | 5đ): Soak/Burst Benchmark (0.5-1 GB/s)\n+ Prometheus/Grafana/Loki + Vault + NGINX"]
        B28["Bước 28 (R40B | 5đ): Promote lên Oracle Cloud (us-phoenix-1)\nTerraform VCN/VM + Ansible ZRAM + K3s Helm/ArgoCD"]
        B26 --> B27 --> B28
    end

    subgraph NEXT["MỞ RỘNG SAU KHI XONG DE FOUNDATION (FULL-STACK DATA + ML + AI)"]
        ML["Phase 8 — ML Platform:\nFeast Feature Store + Kubeflow + MLflow + KServe"]
        LLM["Phase 9 — LLM/Agent Platform:\nRAG Ingestion + Vector Store + MCP Tools + Agents"]
    end

    S0 --> S1 --> S2
    S2 --> S3 & S4
    S3 & S4 --> S5 --> S6 --> S7
    S7 -.-> ML & LLM
```

### Bảng Theo Dõi Tiến Độ Từng Bước (Step-by-Step Checklist)

```text
====================================================================================================
[STAGE 0: CHUẨN BỊ MÁY CHỦ LINUX LOCAL & ORACLE CLOUD TENANCY (FOUNDATION)]                 [0 pts]
  ├── Bước 1: Tạo máy chủ Ubuntu 24.04 LTS trên OrbStack (16GB RAM dynamic, 183GB SSD)     [✔ Xong]
  ├── Bước 2: Tạo user 'dev' & cấp Full quyền Admin (sudo NOPASSWD:ALL)                    [✔ Xong]
  ├── Bước 3: Cài đặt git, make, docker.io, docker-compose-v2                              [✔ Xong]
  ├── Bước 4: Nối thư mục dự án (Symlink) & Khởi tạo Git Local (nhánh main)                [✔ Xong]
  ├── Bước 5: Tạo SSH Key ed25519 & Xác thực kết nối GitHub Web                            [✔ Xong]
  ├── Bước 6: Tạo GitHub Repo, giữ khung thư mục (.gitkeep) & Push lần đầu                 [✔ Xong]
  ├── Bước 7: Thiết lập biến môi trường bảo mật (.env.example & .env)                      [✔ Xong]
  └── Bước 7.1: Đăng ký Oracle Cloud (us-phoenix-1: CloudBigDataPlatform), kết nối         [✔ Xong]
                oci-cloud MCP + oci-cli + Terraform (41 OCPU/277GB Trial & Always Free)
        │
        ▼  [Gate 0 Passed: Local Linux ARM64 + GitHub + Oracle Cloud IaC sẵn sàng 100%]
====================================================================================================
[STAGE 1: CONTAINER FOUNDATION, STORAGE CORE & DOCKERFILE OPTIMIZATION]                     [5 pts]
  ├── Bước 8: Viết 'docker-compose.yml' & Bật riêng lẻ PostgreSQL 16 (WAL) + MinIO S3      [✔ Xong]
  ├── Bước 8.1: Nắm vững bộ lệnh vận hành & gỡ lỗi Docker hàng ngày                        [✔ Xong]
  ├── Bước 8.2: Đo Benchmark thực tế trong Linux VM (MinIO 878 MB/s, Postgres 216 MB/s)    [✔ Xong]
  ├── Bước 9: Viết 'Makefile' chuẩn hóa bảng điều khiển lệnh tắt (make up/down/ps/logs)    [✔ Xong]
  ├── Bước 10: Commit & Push mốc hạ tầng lưu trữ cốt lõi lên GitHub                        [✔ Xong]
  └── Bước 11 (R3–R4 | 5đ): Tối ưu Multi-stage Dockerfile (Baseline vs Slim Non-Root)      ◄── ĐANG Ở ĐÂY
        │
        ▼  [Gate 1: Storage Healthy + Bằng chứng giảm >60% kích thước Docker Image]
====================================================================================================
[STAGE 2: COMMERCE DATA GENERATOR (>= 100 GB + STREAM) & CDC INGESTION]                    [12 pts]
  ├── Bước 12: Thiết kế Schema OLTP PostgreSQL (customers, products, orders, payments...) + Legacy SQL Report
  ├── Bước 13: Xây dựng FastAPI Order Service (Pydantic Validation + Healthcheck)
  ├── Bước 14 (R5–R8 | 8đ): Viết Vectorized Data Generator (PyArrow/DuckDB) sinh >=100GB Batch
  │            (Tiêm lỗi chủ đích: Skew 80% HCM/Electronics, Schema Evolution, Duplicate 2%)
  └── Bước 15 (R9–R10 | 4đ): Dựng Kafka (KRaft) + Debezium CDC + Dead-Letter Queue (DLQ)
               (Bắn Stream clickstream trễ 5–10 phút & payment_callbacks trùng lặp 1.5%)
        │
        ▼  [Gate 2: >=100GB Raw Parquet trên MinIO + CDC/Streaming chảy vào Kafka & DLQ]
====================================================================================================
[STAGE 3: REAL-TIME STREAM PROCESSING VỚI APACHE FLINK]                                    [10 pts]
  ├── Bước 16 (R16 | 2đ): Chạy Flink Baseline Job (chưa tối ưu) & thu thập Flink UI metrics
  ├── Bước 17 (R17 | 3đ): Xử lý dữ liệu đến trễ 5–10p (Bounded Watermarks + Allowed Lateness + Side Outputs)
  ├── Bước 18 (R18 | 3đ): Khử trùng lặp 1.5% realtime (KeyedProcessFunction + StateTtlConfig chống tràn RAM)
  └── Bước 19 (R19 | 2đ): Tính toán cửa sổ thời gian (Tumbling/Sliding Windows) & ghi xuống Delta Lake Bronze
        │
        ▼  [Gate 3: Flink UI chứng minh xử lý sạch Late Data & Duplicate không rò rỉ bộ nhớ]
====================================================================================================
[STAGE 4: BATCH PROCESSING VỚI APACHE SPARK & DELTA / ICEBERG UNIFORM LAKEHOUSE]           [16 pts]
  ├── Bước 20 (R11–R15 | 13đ): Spark Job Bronze (raw_) -> Silver (stg_): So sánh trước/sau khi tối ưu
  │            Skew (AQE + Key Salting), Schema Evolution (merge failure_reason) & Offline Dedup (2%)
  └── Bước 21 (R20 | 3đ): Tối ưu lưu trữ Lakehouse trên MinIO (Partitioning, ZSTD, OPTIMIZE Compaction,
               Z-ORDER BY) + Bật Delta UniForm ('delta.universalFormat.enabledFormats' = 'iceberg')
               sinh song song metadata Apache Iceberg chuẩn Banking Việt Nam trên cùng 1 bản Parquet
        │
        ▼  [Gate 4: Spark UI chứng minh hết Skew + Giảm số file nhỏ + Hỗ trợ kép Delta & Iceberg]
====================================================================================================
[STAGE 5: DIMENSIONAL MODELING (dbt), SLIM CI & OLAP SERVING WAREHOUSE (ClickHouse)]       [13 pts]
  ├── Bước 22 (R34–R38 | 10đ): Xây dựng dbt Core (staging/intermediate/marts -> dim_, fact_, obt_, feat_),
  │            SCD Type 2 (dbt snapshot), Jinja PII Masking (SHA-256), Slim CI (state:modified+) & xuất ERD
  └── Bước 23 (R21 | 3đ): Nạp Gold sang ClickHouse (MergeTree/ReplacingMergeTree) & tối ưu
               Primary/Sorting Keys, Data Skipping Indexes, Materialized Views
        │
        ▼  [Gate 5: dbt Slim CI chạy xanh + ClickHouse query nhanh gấp nhiều lần trước khi đánh Index]
====================================================================================================
[STAGE 6: ORCHESTRATION (Airflow 3) & END-TO-END DATA GOVERNANCE (DataHub)]                [24 pts]
  ├── Bước 24 (R22–R27 | 12đ): Dựng Airflow 3, viết Custom OOP Plugins (WAPReconciliationOperator,
  │            ClickHousePublishHook có pytest) & 3 DAGs chuẩn (DP1, DP2, DP3 gồm Ingest + Validate)
  └── Bước 25 (R28–R33 | 12đ): Tích hợp DataHub thu thập End-to-End Lineage, gắn Tag PII/Sensitive
               và hiển thị kết quả Data Quality Assertions trên giao diện DataHub
        │
        ▼  [Gate 6: 3 DAGs DP1/DP2/DP3 xanh mượt + DataHub hiển thị Lineage từ Postgres tới ClickHouse]
====================================================================================================
[STAGE 7: ENTERPRISE OPS, WAP RECONCILIATION, OBSERVABILITY & ORACLE CLOUD IaC]            [20 pts]
  ├── Bước 26 (R39 - Novel #1 | 10đ): Cổng kiểm duyệt WAP Zero-Copy & Đối chiếu di trú Legacy DW
  │            (DuckDB/PyArrow so khớp row_count/sum_amount với báo cáo SQL cũ; lệch != 0 tự động RESTORE TABLE)
  ├── Bước 27 (R40A - Novel #2A | 5đ): Soak Benchmark (200–500 MB/s sustained, 0.5–1 GB/s peak) +
  │            Observability & Security (Prometheus/Grafana/Loki + HashiCorp Vault + NGINX Gateway)
  └── Bước 28 (R40B - Novel #2B | 5đ): Full Cloud Promotion lên Oracle Cloud (us-phoenix-1):
               Terraform (AD-1 ARM64 A1.Flex + AD-2 AMD64 E2.1.Micro) + Ansible (Hardening/ZRAM/Keepalive)
               + K3s Kubernetes + Helm/ArgoCD GitOps (Giữ cụm Always Free sống vĩnh viễn $0/tháng)
        │
        ▼  [Gate 7 (FINAL): Đạt 100/100 điểm DE Rubric — Nền tảng sẵn sàng cắm tiếp ML (Feast) & AI/RAG]
====================================================================================================
```

---

## 💡 QUY TRÌNH BẬT / TẮT MÁY HÀNG NGÀY (DAILY ROUTINE)

| Nhu cầu | Nơi gõ lệnh | Lệnh thực thi | Mục đích |
| :--- | :--- | :--- | :--- |
| **Nghỉ làm / Đi ngủ (Tắt)** | Trong `dev@ubuntu` | `exit` hoặc `Ctrl + D` | Thoát terminal Linux về Mac |
| | Trên Mac | `orb stop ubuntu` | Tắt hẳn máy ảo, giải phóng 100% RAM/CPU |
| **Sáng làm việc (Bật)** | Trên Mac | `orb -m ubuntu -u dev` | Bật máy ảo và đăng nhập vào user `dev` |
| | Trong `dev@ubuntu` | `cd ~/EnterpriseDataPlatform_PRJ2026` | Nhảy vào đúng thư mục dự án |

---

## Chặng 0: Dựng Linux Server, Tạo User & Phân Quyền (Chuẩn Thực Tế VPS/Server)

> **Quy tắc thực tế:** Dù dùng VPS, Cloud VM hay Server công ty, quy trình chuẩn luôn là: **Bật Linux $\rightarrow$ Tạo user riêng (`dev`) thay cho `root` $\rightarrow$ Cài công cụ cần dùng $\rightarrow$ Cấp đúng quyền cho user đó**.

### Bước 1: Tạo máy chủ Ubuntu 24.04 LTS (trên OrbStack)
```bash
orb list                             # Kiểm tra danh sách máy ảo hiện có
orb create ubuntu:noble ubuntu       # Tạo Server Ubuntu 24.04 (tên mã: noble)
```

### Bước 2: Tạo tài khoản `dev` & cấp Full Quyền (`sudo` không cần mật khẩu)
- Khi cấp quyền `sudo` (`ALL=(ALL) NOPASSWD:ALL`), tài khoản `dev` có **100% quyền tối cao trên toàn bộ máy chủ** — bất cứ lệnh hệ thống hay công cụ nào chỉ cần gõ `sudo` ở đầu (hoặc thêm luôn vào nhóm `docker`) là chạy thẳng.
```bash
# 1. Tạo tài khoản 'dev' và cấp luôn quyền nhóm 'sudo' (Full Admin)
orb -m ubuntu -u root useradd -m -s /bin/bash -G sudo dev

# 2. Cho phép 'dev' gõ sudo chạy mọi thứ không bao giờ hỏi mật khẩu
orb -m ubuntu -u root sh -c 'echo "dev ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/dev && chmod 440 /etc/sudoers.d/dev'
```

### Bước 3: Đăng nhập vào `dev@ubuntu` & cài công cụ (`git`, `make`, `docker`)
```bash
orb -m ubuntu -u dev                                    # Đăng nhập vào Server với tài khoản 'dev'
cd ~                                                    # Về thư mục nhà (/home/dev)
df -h / && free -h && nproc                             # Kiểm tra nhanh ổ cứng, RAM, CPU

sudo apt update                                         # Cập nhật danh mục phần mềm
sudo apt install -y git make docker.io docker-compose-v2 # Cài Git, Make, Docker
# Ghi chú: 'docker.io' là engine chạy container đơn lẻ; 'docker-compose-v2' là công cụ điều phối nhiều container qua file yaml.

sudo usermod -aG docker dev && newgrp docker            # Cho phép chạy docker luôn không cần gõ sudo
```

---

## Chặng 1: Khởi Tạo Dự Án, Git & Biến Môi Trường (`.env`)

### Bước 4: Nối thư mục dự án vào `/home/dev` & khởi tạo Git
- **Giải thích lệnh Linux cơ bản:**
  - `ln -s <nguồn> <đích>`: Tạo đường dẫn tắt (Symlink) để Server Linux nhìn thấy thư mục từ máy thật macOS mà không cần copy tốn dung lượng.
  - `cd <đường_dẫn>`: Change Directory — chuyển con trỏ terminal vào thư mục được chỉ định.
  - `ls -la`: List All — liệt kê tất cả file và thư mục (kể cả file ẩn bắt đầu bằng dấu chấm `.`) kèm thông tin quyền, kích thước và ngày tạo.
- **Git (Local) vs GitHub (Cloud):**
  - Git chạy **100% offline trên máy**, chưa kết nối mạng nên **chưa cần đăng nhập tài khoản**.
  - `git config user.name` và `user.email` chỉ là **chữ ký offline** gắn vào mỗi mốc lưu (commit). Khi nào đẩy code lên mạng (`git push`) thì lúc đó mới cần đăng nhập GitHub.
```bash
# 1. Tạo đường dẫn tắt nối dự án từ macOS vào /home/dev của Linux Server
ln -s /Users/MAC/Documents/EnterpriseDataPlatform_PRJ2026 ~/EnterpriseDataPlatform_PRJ2026
cd ~/EnterpriseDataPlatform_PRJ2026                      # Nhảy vào thư mục dự án
ls -la                                                  # Liệt kê kiểm tra toàn bộ file trong dự án

# 2. Khởi tạo Git và các thư mục cấu trúc
git init -b main                                        # Khởi tạo kho theo dõi Git offline, nhánh chính 'main'
mkdir -p src tests infra docs/runbooks                  # Tạo khung thư mục (-p: tự tạo nếu chưa có)

# 3. Khai báo chữ ký tác giả (dùng email GitHub thật của bạn để sau này đẩy lên tự nhận diện)
git config --global user.name "Aric Nguyen"             # Tên hiển thị của bạn
git config --global user.email "you@example.com"        # Email gắn vào commit

# 4. Lưu mốc lịch sử đầu tiên
git status                                              # Xem trạng thái các file (được lọc qua .gitignore)
git add .gitignore README.md assignment docs            # Đưa file vào giỏ chuẩn bị lưu (Staging Area)
git commit -m "chore: initialize data platform repo"    # Đóng dấu lưu mốc lịch sử đầu tiên
```

### Bước 5: Cấu hình SSH Key kết nối GitHub Web
- **Bản chất:** SSH Key là chiếc chìa khóa điện tử giúp máy chủ Linux tự động xác thực danh tính với GitHub mà không cần gõ mật khẩu hay Token mỗi lần đẩy code.
```bash
# 1. Tạo cặp khóa SSH ed25519 (chuẩn mã hóa hiện đại, bấm Enter 3 lần liên tục để chọn mặc định)
ssh-keygen -t ed25519 -C "email-github-cua-ban@gmail.com"

# 2. In khóa công khai (Public Key) ra màn hình
cat ~/.ssh/id_ed25519.pub
# -> Bôi đen copy toàn bộ dòng 'ssh-ed25519 AAAAC3NzaC1...'

# 3. Thêm khóa vào GitHub Web:
# Truy cập https://github.com/settings/keys -> Chọn "New SSH key" -> Dán khóa -> Bấm "Add SSH key"

# 4. Kiểm tra kết nối từ Linux lên GitHub
ssh -T git@github.com
# -> Khi hỏi (yes/no) gõ 'yes'. Hiện 'Hi <username>! You've successfully authenticated...' là chuẩn 100%!
```

### Bước 6: Tạo Repository trên GitHub & Push code lần đầu (Initial Push)
- **Quy trình chuẩn:** 
  1. Lên https://github.com/new tạo repo trống (không tick README hay gitignore).
  2. Gắn địa chỉ Remote (dạng SSH `git@github.com:...`).
  3. Gom toàn bộ file ban đầu vào commit và đẩy lên nhánh `main`.
```bash
# 1. Gom toàn bộ file khung sườn và lưu mốc commit
git add .
git commit -m "feat: initialize enterprise data platform structure and documentation" || true

# 2. Gắn đường dẫn Remote SSH của repo GitHub (thay đúng username của bạn)
git remote remove origin 2>/dev/null || true
git remote add origin git@github.com:ankinguyen-engineer-2002/EnterpriseBigDataPlatform.git

# 3. Đặt tên nhánh chính là 'main' và đẩy toàn bộ lên GitHub
git branch -M main
git push -u origin main
```

### Bước 6.1: Cẩm Nang Lệnh Git Hàng Ngày (Daily Git Workflow - Đủ Dùng, Thực Chiến)
- **Quy trình 1: Lưu code & Đẩy lên GitHub sau mỗi tính năng**
  ```bash
  git status                         # 1. Kiểm tra xem mình vừa tạo/sửa những file nào
  git diff                           # 2. Xem chi tiết từng dòng code vừa thay đổi (bấm 'q' để thoát)
  git add <tên_file>                 # 3. Gom file cụ thể vào giỏ (hoặc 'git add .' để gom hết)
  git commit -m "feat: mo_ta_ngan"   # 4. Đóng dấu commit kèm thông điệp rõ ràng
  git push origin main               # 5. Đẩy code lên GitHub
  ```

- **Quy trình 2: Lấy code mới nhất về máy (khi làm việc nhóm hoặc đổi máy)**
  ```bash
  git pull origin main               # Kéo toàn bộ code mới nhất từ GitHub về máy
  ```

- **Quy trình 3: Kiểm tra lịch sử commit**
  ```bash
  git log --oneline -n 5             # Xem 5 commit gần nhất trên 1 dòng cực gọn
  ```

- **Quy trình 4: "Cứu cánh" khi lỡ tay gõ sai**
  ```bash
  git restore <tên_file>             # Hủy bỏ thay đổi của 1 file khi CHƯA gõ 'git add'
  git restore --staged <tên_file>    # Bỏ file ra khỏi giỏ chuẩn bị commit (sau khi lỡ 'git add')
  ```

### Bước 7: Quản lý biến môi trường bảo mật (`.env.example` & `.env`)
- **Quy chuẩn bảo mật IT:** 
  - Không bao giờ đưa tài khoản/mật khẩu vào code đẩy lên Git.
  - `.env.example`: Chứa khung biến mẫu, **được commit lên Git** để đồng đội biết hệ thống cần những tham số nào.
  - `.env`: Chứa mật khẩu thật chạy cục bộ, **bắt buộc bị `.gitignore` chặn lại**.
```bash
# 1. Tạo file cấu hình mẫu .env.example
cat << 'EOF' > .env.example
COMPOSE_PROJECT_NAME=edp_commerce
POSTGRES_USER=edp_admin
POSTGRES_PASSWORD=edp_secret_2026
POSTGRES_DB=commerce_oltp
POSTGRES_PORT=5432
MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=minio_secret_2026
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
EOF

# 2. Nhân bản sang file cấu hình thật .env để container đọc lúc khởi chạy
cp .env.example .env

# 3. Kiểm tra danh sách file (thấy cả .env và .env.example)
ls -la
```

---

## Chặng 2: Hạ Tầng Lưu Trữ Cốt Lõi (PostgreSQL 16 & MinIO S3)

### 📌 Phân Biệt: Cụm Kiến Trúc Logic vs Container Độc Lập Thực Tế
- **Cụm kiến trúc** là sự phân chia về mặt **tư duy hệ thống** (để biết cái nào làm gì, cái nào dựng trước).
- **Thực tế trên Docker:** **1 Dịch Vụ = 1 Container Riêng Biệt**. Tuyệt đối KHÔNG gộp chung Postgres với MinIO, hay Kafka với Debezium vào cùng 1 container.

| Cụm Kiến Trúc (Tư duy Logic) | Mỗi dịch vụ = 1 Container ĐỘC LẬP HOÀN TOÀN | Cơ chế giao tiếp |
| :--- | :--- | :--- |
| **1. Storage Core (Nơi chứa)** | • Container 1: `edp_postgres` (DB nguồn OLTP)<br>• Container 2: `edp_minio` (Data Lake S3) | **Mạng chung `edp_network`:** Như một Switch mạng LAN ảo nối tất cả container lại với nhau. |
| **2. Streaming Bus (Nơi truyền)** | • Container 3: `edp_kafka` (Message Broker)<br>• Container 4: `edp_debezium` (CDC Connector) | **Giao tiếp nội bộ:** Các container tự gọi nhau bằng tên service (VD: `postgres:5432`, `minio:9000`). |
| **3. Compute Engine (Nơi tính toán)** | • Container 5: `edp_spark_master` (Master)<br>• Container 6: `edp_spark_worker` (Worker)<br>• Container 7: `edp_flink` (Stream Engine) | |
| **4. Serving & Governance (Báo cáo & Quản trị)** | • Container 8: `edp_clickhouse` (OLAP DB)<br>• Container 9: `edp_airflow` (Điều phối)<br>• Container 10: `edp_datahub` (Data Catalog) | |

- **Giải mã Port (`Port_Máy_Thật : Port_Nội_Bộ` - VD: `9001:9001`):**
  - **Port Máy Thật (Bên trái):** Mở ra `localhost` trên Mac để bạn mở trình duyệt xem giao diện Web (VD: `http://localhost:9001` vào MinIO Console).
  - **Port Nội Bộ (Bên phải):** Cổng bên trong container để các container khác tự kết nối ngầm với nhau qua `edp_network` (không cần qua localhost).
- **Thứ tự triển khai:** Dựng Chặng 2 (`Postgres` + `MinIO`) xong chạy khỏe $\rightarrow$ Mới dựng tiếp Chặng 3 (`Kafka` + `Debezium`) $\rightarrow$ Tránh sập máy vì thiếu RAM (OOM) và dễ dàng gỡ lỗi từng con.

---

### Bước 8: Viết `docker-compose.yml` & Bật 2 Container Lưu Trữ Cốt Lõi
- **Bản chất file YAML:** Theo chuẩn Docker Compose Specification quốc tế với 3 từ khóa gốc:
  1. `services:` Khai báo các container độc lập cần chạy (`postgres`, `minio`).
  2. `networks:` Khai báo mạng ảo switch `edp_network` để các container tự tìm thấy nhau qua DNS.
  3. `volumes:` Khai báo ổ cứng ảo lưu trữ vĩnh viễn (`pg_data`, `minio_data`) để xóa container không bị mất dữ liệu.
- **Giải phẫu bộ 3 cờ PostgreSQL CDC, Kiến trúc RAM $\rightarrow$ Disk & Bản chất Replica:**
  - `wal_level=logical`: Bắt Postgres dịch nhật ký nhị phân thô sang ngôn ngữ logic (bảng nào, dòng nào vừa Insert/Update) để Debezium đọc được.
  - `max_replication_slots=10`: Số lượng "chiếc kẹp sách" tối đa đánh dấu vị trí Debezium đang đọc tới đâu (chống mất dữ liệu khi rớt mạng).
  - `max_wal_senders=10`: Số lượng tiến trình tối đa mở ra để bốc dữ liệu WAL gửi qua mạng cho Debezium.
  - *Cơ chế bộ nhớ `Shared Buffers` / `WAL Buffers` (RAM) $\rightarrow$ `pg_wal` / `base/` (Disk):*
    ```text
    ================== TRÊN RAM (SHARED MEMORY) ==================
       [Câu lệnh INSERT / UPDATE]
            │
            ├──────────────────────────────┐
            ▼                              ▼
     [1. Shared Buffers (RAM)]      [2. WAL Buffers (RAM)]
     (Sửa bảng/index ngay trên RAM   (Ghi 1 dòng log ngắn gọn)
      -> Gọi là "Dirty Pages",             │
      CHƯA ghi xuống ổ cứng!)              │ Khi COMMIT: Flush ngay! (Ghi tuần tự siêu nhanh)
            │                              ▼
    ========│======================== TRÊN Ổ CỨNG (DISK) =========
            │                       [File nhật ký: pg_wal/]  ◄── [Replication Slot] ──► Debezium CDC
            │                        (Lưu tạm thời xoay vòng      (Kẹp giữ CẤM XÓA khi
            │                         như camera hành trình)       Debezium chưa đọc xong)
            │ Cứ 5 phút/lần (CHECKPOINT)
            │ Mới gom "Dirty Pages" đổ xuống đĩa 1 thể!
            ▼
     [File bảng thật: base/] (Lưu trạng thái cuối cùng vĩnh viễn)
    ```
  - *Lưu ý thực chiến Production:* `pg_wal` ghi xuống đĩa để chống mất điện khi `Shared Buffers` chưa kịp đổ xuống `base/`. Sau mỗi kỳ `Checkpoint` (5 phút), Postgres sẽ xóa `pg_wal` cũ để giải phóng đĩa. Nhờ có `Replication Slot`, Postgres bị cấm xóa các file `pg_wal` mà Debezium chưa đọc. Nhưng nếu Debezium chết nhiều ngày không ai sửa, `pg_wal` sẽ phình đầy ổ cứng làm sập DB (cần giới hạn bằng `max_slot_wal_keep_size`).
  - *Ý nghĩa Replica trong Big Data:* Ở đâu cũng có Replica (Postgres, Kafka, ClickHouse) nhằm 2 mục đích sống còn: **Chống mất dữ liệu khi sập máy (Fault Tolerance)** và **Chia tải đọc không làm nghẽn máy chính (Read Scaling)**.
  - `${POSTGRES_USER:-edp_admin}`: Cú pháp đọc biến từ `.env`, nếu không có thì lấy giá trị sau dấu `:-`.
  - `volumes: pg_data:/var/lib/...`: Ánh xạ ổ cứng container ra ổ cứng máy chủ để giữ dữ liệu an toàn.
  - `healthcheck`: Kiểm tra sức khỏe định kỳ mỗi 5s, khi sẵn sàng phục vụ mới báo trạng thái `healthy`.
  - `entrypoint` trong MinIO: Tự động tạo sẵn 5 bucket theo chuẩn Medallion (`raw`, `bronze`, `silver`, `gold`, `checkpoints`) ngay khi bật container.

```bash
# 1. Khai báo file docker-compose.yml chi tiết
cat << 'EOF' > docker-compose.yml
services:
  postgres:
    image: postgres:16.4-alpine
    container_name: edp_postgres
    restart: unless-stopped
    command: >
      postgres -c wal_level=logical
               -c max_wal_senders=10
               -c max_replication_slots=10
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-edp_admin}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-edp_secret_2026}
      POSTGRES_DB: ${POSTGRES_DB:-commerce_oltp}
    ports:
      - "${POSTGRES_PORT:-5432}:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-edp_admin} -d ${POSTGRES_DB:-commerce_oltp}"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - edp_network

  minio:
    image: alpine/minio:RELEASE.2025-10-15T17-29-55Z
    container_name: edp_minio
    restart: unless-stopped
    entrypoint: >
      /bin/sh -c "
      mkdir -p /home/minio/raw /home/minio/bronze /home/minio/silver /home/minio/gold /home/minio/checkpoints &&
      exec minio server /home/minio --console-address ':9001'
      "
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minio_admin}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minio_secret_2026}
    ports:
      - "${MINIO_API_PORT:-9000}:9000"
      - "${MINIO_CONSOLE_PORT:-9001}:9001"
    volumes:
      - minio_data:/home/minio
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:9000/minio/health/live || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - edp_network

networks:
  edp_network:
    name: edp_network
    driver: bridge

volumes:
  pg_data:
    name: edp_pg_data
  minio_data:
    name: edp_minio_data
EOF

# 2. Quy trình gỡ lỗi chuyên nghiệp: Bật và nghiệm thu TỪNG CONTAINER ĐƠN LẺ
# (Tránh bật cả cụm để cô lập lỗi ngay từ đầu: con nào chết biết ngay con đó)

# Bước 2.1: Bật và kiểm tra riêng con PostgreSQL 16 (DB nguồn)
sudo docker compose up -d postgres
sudo docker compose ps postgres          # STATUS phải báo 'Up ... (healthy)'

# Bước 2.2: Bật và kiểm tra tiếp con MinIO S3 (Data Lake)
sudo docker compose up -d minio
sudo docker compose ps minio             # STATUS phải báo 'Up ... (healthy)'

# 3. Nghiệm thu thực tế trên trình duyệt web (Verification)
# - Truy cập MinIO Web UI: http://localhost:9001 (User: minio_admin, Pass: minio_secret_2026)
# - Kiểm tra mục Buckets: Thấy đủ 5 bucket Medallion tự động sinh ra!

# 4. Ghi chú kiến trúc tinh tế:
# - Vì sao MinIO có web còn Postgres thì không?
#   + MinIO có cổng 9001 là HTTP Web Server nên Chrome đọc được.
#   + Postgres mở cổng 5432 nhưng chạy giao thức DB nhị phân, muốn xem phải dùng DBeaver kết nối vào localhost:5432.
# - Vì sao có sẵn 5 bucket (raw, bronze, silver, gold, checkpoints)?
#   + Nhờ cờ 'entrypoint' chạy 'mkdir -p' trong docker-compose.yml. MinIO tự động map các thư mục này thành 5 Bucket S3 ngay khi vừa khởi động!
```

### Bước 8.1: Cẩm Nang Lệnh Docker Thực Chiến Hàng Ngày (Daily Docker Cheatsheet)
- **1. Quản lý vòng đời (Bật / Tắt / Xóa):**
  ```bash
  sudo docker compose up -d              # Bật tất cả container chạy ngầm (-d)
  sudo docker compose stop                 # Tạm dừng (Pause) container, không tốn RAM/CPU
  sudo docker compose start                # Bật lại các container vừa tạm dừng
  sudo docker compose down                 # Tắt hẳn và tháo dỡ container (DỮ LIỆU VẪN CÒN NGUYÊN trên volume)
  sudo docker compose down -v              # CẢNH BÁO: Tắt và XÓA SẠCH toàn bộ dữ liệu volume (-v) để làm lại từ đầu
  ```

- **2. Khám bệnh & Soi lỗi (Troubleshooting):**
  ```bash
  sudo docker compose ps                   # Xem danh sách container, trạng thái sống/chết (healthy/unhealthy)
  sudo docker compose logs -f              # Xem log thời gian thực của cả hệ thống (bấm Ctrl+C để thoát)
  sudo docker compose logs -f postgres     # Xem log riêng con postgres để điều tra lỗi
  ```

- **3. Thao tác nâng cao & Quản lý dung lượng:**
  ```bash
  sudo docker exec -it edp_postgres sh     # "Chui" vào bên trong container postgres để gõ lệnh nội bộ (exit để ra)
  sudo docker system df                    # Kiểm tra Docker đang chiếm bao nhiêu GB ổ cứng (Images, Containers, Volumes)
  ```

### Bước 8.2: Bằng Chứng Đo Hiệu Năng Thực Tế Trong Linux VM & Docker Containers (`[Verified]`)
- **Môi trường đo:** Chạy **100% bên trong máy chủ `dev@ubuntu` (OrbStack Linux VM)** và bắn tải trực tiếp vào 2 container `edp_postgres` + `edp_minio` (không phải đo ngoài macOS).

| Hạng mục đo (Trong `dev@ubuntu`) | Khối lượng test | Thời gian | Tốc độ thực tế (`[Verified]`) | Đánh giá so với mục tiêu (`200–500 MB/s`, đỉnh `1 GB/s`) |
| :--- | :--- | :--- | :--- | :--- |
| **1. Ổ đĩa gốc Linux VM (`/home/dev`)** | `2.0 GiB` (`dd oflag=direct`) | `0.55s` | **`3,900 MB/s` (`3.9 GB/s`)** | Vượt xa trần vật lý |
| **2. Thư mục dự án nối sang Mac (`Symlink`)** | `2.0 GiB` (`dd oflag=direct`) | `0.71s` | **`3,000 MB/s` (`3.0 GB/s`)** | Không bị nghẽn I/O qua lớp ảo hóa |
| **3. Container `edp_minio` — Ghi S3 (`PUT`, 8 luồng)** | `1,024 MiB` (`16 x 64MB`) | `1.17s` | **`877.9 MiB/s` (`~920 MB/s`)** | **Đạt thẳng ngưỡng đỉnh (`0.5–1.0 GB/s`)** |
| **4. Container `edp_minio` — Đọc S3 (`GET`, 8 luồng)** | `1,024 MiB` (`16 x 64MB`) | `0.28s` | **`3,692.0 MiB/s` (`~3.7 GB/s`)** | Đọc siêu tốc cho Spark/Flink/ClickHouse |
| **5. Container `edp_postgres` (`wal_level=logical`)** | `2,000,000 dòng` (`558.2 MiB`) | `2.59s` | **`215.9 MiB/s` (`773,554 dòng/s`)** | **Vượt `200 MB/s` ngay trên 1 luồng DB + ghi WAL** |
| **6. Container `edp_postgres` (`pgbench` 16 clients)** | `42,903 giao dịch OLTP` | `10.0s` | **`4,287 TPS` (trễ `3.73 ms`)** | Chịu tải giao dịch OLTP cường độ cao |

---

### Bước 9: Viết `Makefile` — Bảng Điều Khiển Lệnh Tắt Dự Án
- **Chức năng:** Chuẩn hóa toàn bộ câu lệnh vận hành dài dòng thành các phím tắt 1 từ (`make up`, `make down`, `make ps`, `make logs`). Bất kỳ ai clone repo về chỉ cần gõ `make help` là biết cách chạy dự án ngay.
```bash
cat << 'EOF' > Makefile
.PHONY: help up down ps logs restart clean

help:
	@echo "Các lệnh tắt quản lý hạ tầng Enterprise Data Platform:"
	@echo "  make up       - Khởi động toàn bộ container"
	@echo "  make down     - Tắt toàn bộ container (giữ nguyên dữ liệu)"
	@echo "  make ps       - Kiểm tra trạng thái sức khỏe container"
	@echo "  make logs     - Xem log thời gian thực của các container"
	@echo "  make restart  - Khởi động lại toàn bộ dịch vụ"
	@echo "  make clean    - Xóa sạch container và dữ liệu volume"

up:
	sudo docker compose up -d

down:
	sudo docker compose down

ps:
	sudo docker compose ps

logs:
	sudo docker compose logs -f

restart:
	sudo docker compose restart

clean:
	sudo docker compose down -v
EOF

# Kiểm tra ngay lệnh tắt
make help
make ps
```

### Bước 10: Lưu Mốc Lịch Sử Chặng 2 Lên GitHub
```bash
git status
git add docker-compose.yml Makefile docs/runbooks/STEP_BY_STEP_BUILD_GUIDE.md
git commit -m "feat(infra): add PostgreSQL 16 (logical WAL), MinIO S3 Medallion & Makefile"
git push origin main
```
