FROM python:3.12-slim

# กำหนดที่ทำงานใน container
WORKDIR /app

# คัดลอกไฟล์โค้ดไปยัง container
COPY . /app

# ติดตั้งไลบรารีที่ต้องการ
RUN pip install -r requirements.txt

# ติดตั้ง python-dotenv เพื่อโหลดตัวแปรจากไฟล์ .env
RUN pip install python-dotenv

# กำหนดคำสั่งเริ่มต้น
ENTRYPOINT ["python", "healthcheck.py"]
