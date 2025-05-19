import os
import sys
import subprocess
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from datetime import timedelta,date

# โหลดตัวแปรจากไฟล์ .env
load_dotenv()

# อ่านค่าตัวแปรจาก environment
MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))  # ค่าเริ่มต้น SMTP TLS
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_TO = os.getenv("EMAIL_TO")
EMAIL_FROM = os.getenv("EMAIL_FROM")

today = date.today() - timedelta(days=1)
yesterday = today - timedelta(days=1)

def create_minio_client():
    """สร้าง Minio client"""
    return Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # ใช้ True ถ้า Minio ใช้ HTTPS
    )

def check_file_in_minio(client, bucket_name, filename):
    """ตรวจสอบว่าไฟล์มีอยู่ใน Minio หรือไม่"""
    try:
        client.stat_object(bucket_name, filename)
        print(f"File {filename} found in Minio.")
        return True
    except S3Error as e:
        if e.code == 'NoSuchKey':
            print(f"File {filename} not found in Minio.")
        else:
            print(f"Error checking file in Minio: {e}")
        return False

# def send_alert_email(subject, body):
#     SMTP_SERVER = os.getenv("SMTP_SERVER")
#     SMTP_PORT = 587  # Default to port 587 for TLS
#     EMAIL_USERNAME = os.getenv("EMAIL_USERNAME")
#     EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
#     RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

#     try:
#         # สร้างข้อความอีเมล
#         msg = MIMEMultipart()
#         msg['From'] = EMAIL_USERNAME
#         msg['To'] = RECIPIENT_EMAIL
#         msg['Subject'] = subject
#         msg.attach(MIMEText(body, 'plain'))

#         # สร้างการเชื่อมต่อกับ SMTP server
#         with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
#             server.set_debuglevel(1)  # เปิด debug เพื่อดูการทำงานของ SMTP
#             server.connect(SMTP_SERVER, SMTP_PORT)  # เชื่อมต่อกับ SMTP Server
#             server.ehlo()  # ทักทายเซิร์ฟเวอร์ SMTP
#             server.starttls()  # เริ่มการเชื่อมต่อแบบเข้ารหัส (TLS)
#             server.ehlo()  # ทักทายหลังจาก starttls()
#             server.login(EMAIL_USERNAME, EMAIL_PASSWORD)  # เข้าสู่ระบบ
#             server.send_message(msg)  # ส่งข้อความอีเมล

#         print("Alert email sent successfully.")
#     except Exception as e:
#         print(f"Failed to send email: {e}")

def main():
    """ฟังก์ชันหลัก"""
    if len(sys.argv) != 2:
        print("Usage: python healthcheck.py <filename_identifier>")
        sys.exit(1)

    identifier = sys.argv[1].upper()
    filename = ""
    
    if identifier == 'A'.upper():
        filename = f"WatchList/Blue_plus/{str(today)}/Blue_plus_List_{str(today)}_Day.xlsx"
    elif identifier == 'B'.upper():
        filename = f"WatchList/Blue_plus/{str(today)}/Blue_plus_List_{str(today)}_Night.xlsx"
    elif identifier == 'C'.upper():
        str_today = today.strftime("%Y-%m-%d")
        filename = f"WatchList/Blue_plus_1_Day_Ago/{str(today)}/Blue_plus_List_{str_today}-Day-1.xlsx"
    elif identifier == 'D'.upper():
        filename = f"WatchList/Blue_plus/{str(today)}/Blue_plus_List_{str(today)}_Afternoon.xlsx"
    else:
        filename = ''
    
    # สร้าง Minio client
    client = create_minio_client()

    for attempt in range(1, MAX_RETRIES + 1):
        if check_file_in_minio(client, MINIO_BUCKET_NAME, filename):
            return
        print(f"File {filename} not found in Minio. Attempt {attempt}/{MAX_RETRIES}. Retrying...")
        subprocess.run(["python3", "app.py", identifier])

    # หากยังไม่พบไฟล์หลังจาก retry ครบ
    # if not check_file_in_minio(client, MINIO_BUCKET_NAME, filename):
    #     print(f"File {filename} still not found in Minio after {MAX_RETRIES} retries. Sending alert email.")

    #     # เนื้อหาอีเมล
    #     subject = "[ALERT] ไฟล์ไม่พบใน Minio หลังการตรวจสอบ 3 ครั้ง"
    #     body = f"""
    #     เรียน ผู้ดูแลระบบ,

    #     การตรวจสอบไฟล์ใน Minio ล้มเหลว:
    #     - ไฟล์ที่ค้นหา: {filename}
    #     - Bucket: {MINIO_BUCKET_NAME}
    #     - วันที่ตรวจสอบ: {datetime.datetime.now().strftime('%d %B %Y')}
    #     - สถานะ: ไม่พบไฟล์ใน Minio หลังพยายามตรวจสอบ {MAX_RETRIES} ครั้ง

    #     กรุณาตรวจสอบระบบหรือดำเนินการเพิ่มเติมตามความเหมาะสม

    #     ขอบคุณครับ,
    #     ระบบ Health Check
    #     """
    #     send_alert_email(subject, body)

if __name__ == "__main__":
    main()
