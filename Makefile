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
