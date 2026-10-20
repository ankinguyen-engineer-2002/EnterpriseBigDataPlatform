# Step-by-Step Build Guide — Enterprise Commerce Data Platform

> **Mục tiêu dự án:** Xây dựng Data Platform chuẩn doanh nghiệp (`Kafka, Spark, Flink, Delta Lake, dbt, ClickHouse, Airflow, DataHub`), xử lý `>= 100 GB` Batch và tốc độ `200–500 MB/s` (đỉnh `0.5–1 GB/s`).

---

## 📌 BẢN ĐỒ TIẾN ĐỘ DỰ ÁN (PROJECT ROADMAP FLOW)

```text
[Chặng 0: Hạ tầng Linux Server]
  ├── Bước 1: Tạo máy chủ Ubuntu 24.04 trên OrbStack             [✔ Xong]
  ├── Bước 2: Tạo user 'dev' & cấp Full quyền Admin sudo         [✔ Xong]
  └── Bước 3: Cài đặt git, make, docker, docker-compose          [✔ Xong]
        │
        ▼
[Chặng 1: Môi trường & Mã nguồn]
  ├── Bước 4: Nối thư mục dự án (~/EnterpriseDataPlatform_PRJ2026) [✔ Xong]
  ├── Bước 5: Cấu hình SSH Key & kết nối GitHub Web              [✔ Xong]
  └── Bước 6: Tạo cấu hình mật khẩu (.env.example & .env)        [✔ Xong]
        │
        ▼
[Chặng 2: Hạ tầng Lưu Trữ Cốt Lõi (Storage & Ingestion Foundation)] ◄── ĐANG Ở ĐÂY
  ├── Bước 7: Viết 'docker-compose.yml' & bật PostgreSQL 16 + MinIO
  ├── Bước 8: Viết 'Makefile' quản lý lệnh tắt (make up, make down)
  └── Bước 9: Commit & git push mốc Chặng 2 lên GitHub
        │
        ▼
[Chặng 3: Data Generator (>= 100 GB) & Streaming CDC (Kafka/Debezium)]
[Chặng 4: Processing Engine (PySpark, Flink & Delta Lake Medallion)]
[Chặng 5: Analytical Serving (ClickHouse / Trino) & dbt Modeling]
[Chặng 6: Orchestration (Airflow) & Data Governance (DataHub)]
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

### Bước 5: Kết nối GitHub qua SSH Key & Đẩy code lên mạng (Remote)
- **Vì sao dùng SSH Key:** Đây là chuẩn bảo mật cao nhất của GitHub, chỉ cần thiết lập 1 lần là sau này gõ `git push` hay `git pull` không bao giờ bị hỏi mật khẩu hay token.
```bash
# 1. Tạo chìa khóa SSH Key ed25519 (bấm Enter 3 lần liên tục khi được hỏi)
ssh-keygen -t ed25519 -C "email-github-cua-ban@gmail.com"

# 2. In chìa khóa công khai ra màn hình để copy
cat ~/.ssh/id_ed25519.pub
# -> Copy toàn bộ dòng 'ssh-ed25519 AAAAC3NzaC1...' dán vào https://github.com/settings/keys

# 3. Kiểm tra kết nối từ Linux lên GitHub
ssh -T git@github.com
# -> Thấy thông báo: 'Hi <username>! You've successfully authenticated...' là thành công 100%!

# 4. Gắn địa chỉ kho GitHub và đẩy code lên (thay URL kho của bạn vào)
git remote add origin git@github.com:<username>/EnterpriseDataPlatform_PRJ2026.git
git branch -M main
git push -u origin main
```

### Bước 6: Tạo file cấu hình mật khẩu (`.env.example` & `.env`)
- **Vì sao cần:** Không bao giờ viết cứng mật khẩu vào code. `.env.example` là khung mẫu đẩy lên Git; `.env` chứa mật khẩu chạy thật và bị `.gitignore` chặn lại.
```bash
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

cp .env.example .env                 # Copy bản mẫu sang bản chạy thật (.env)
```

---

## Bước 7 (Đang thực hiện tiếp theo): Bật `PostgreSQL 16` + `MinIO` bằng Docker Compose
*(Sẽ cập nhật ngay khi gõ xong)*
