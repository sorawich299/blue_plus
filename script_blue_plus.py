import os
import xlsxwriter
import pandas as pd
import oracledb

from datetime import date
from datetime import timedelta

from minio import Minio
from minio.error import S3Error
from PIL import Image

from io import BytesIO

import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv

load_dotenv()

KEY_IMAGE = os.getenv("KEY_IMAGE")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = os.getenv("ORACLE_DSN")

MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")


def condb():
    try:
        con_ora = oracledb.connect(
            user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
        cursor_ora = con_ora.cursor()
        print('The Oracle database connect succesfully!')
        return con_ora, cursor_ora
    except oracledb.DatabaseError as er:
        print('There is an error in the Oracle database:', er)
        return None, None


def getTransactionBlue_plus(today, time):
    con_ora, cursor_ora = condb()


    sql = """SELECT * FROM (SELECT x.*, d.IMG_BODY, d.IMG_PLATE, d.IMG_BACK, d.PLATE1_ORIGIN, d.PLATE2_ORIGIN, d.PROVINCE_ORIGIN, y.STATUS, y.CREATE_CHANNEL, d.MODEL, d.COLOR,
			ROW_NUMBER() OVER (PARTITION BY x.TRANSACTION_DATE, x.VEHICLE_LICENSE_1 || ' ' || x.VEHICLE_LICENSE_2, x.DESCRIPTION ORDER BY x.TRANSACTION_ID) AS RN
FROM (
SELECT 	a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'MEMBER' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('PARAMDATE 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMDATE PARAMTIME', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE LEFT JOIN CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('PARAMDATE 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMDATE PARAMTIME', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0
UNION
	SELECT  a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'NON-MEMBER' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM 	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM NONMEMBER_SERVICE.MF_NONM_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('PARAMDATE 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMDATE PARAMTIME', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN NONMEMBER_SERVICE.MF_NONM_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE
			LEFT JOIN NONMEMBER_SERVICE.MF_NONM_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('PARAMDATE 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMDATE PARAMTIME', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0 
    UNION 
    SELECT 	a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'ILLIGAL' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM 	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('PARAMDATE 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMDATE PARAMTIME', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('PARAMDATE 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMDATE PARAMTIME', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0
	) x
    LEFT JOIN 
    (
	SELECT * FROM 	(
						SELECT a.STATUS, b.CREATE_CHANNEL, b.TRANSACTION_ID FROM INVOICE_SERVICE.MF_INVOICE a JOIN INVOICE_SERVICE.MF_INVOICE_DETAIL b ON a.INVOICE_NO = b.INVOICE_NO
					UNION
						SELECT a.STATUS, b.CREATE_CHANNEL, b.TRANSACTION_ID FROM INVOICE_SERVICE.MF_INVOICE_NONMEMBER a JOIN INVOICE_SERVICE.MF_INVOICE_DETAIL_NONMEMBER b ON a.INVOICE_NO = b.INVOICE_NO
					)
	) y ON x.TRANSACTION_ID = y.TRANSACTION_ID
	INNER JOIN VPROFI_M9.WATCHLIST_MASTER d ON d.PLATE1 = x.VEHICLE_LICENSE_1 AND d.PLATE2 = x.VEHICLE_LICENSE_2 AND x.DESCRIPTION LIKE d.PROVINCE
	LEFT JOIN VPROFI_M9.WATCHLIST_TYPE e ON d.LIST_TYPE = e.ID
	WHERE e.LIST_NAME = 'Blue_plus' 
	) x WHERE RN= 1
ORDER BY TRANSACTION_DATE"""
    sql = sql.replace('PARAMDATE', str(today)).replace('PARAMTIME', time)
    pdOra = pd.read_sql(sql, con=con_ora)

    return pdOra


def getTransactionBlue_plusYesterday(yesterday, today):
    con_ora, cursor_ora = condb()
    sql = """SELECT * FROM (SELECT x.*, d.IMG_BODY, d.IMG_PLATE, d.IMG_BACK, d.PLATE1_ORIGIN, d.PLATE2_ORIGIN, d.PROVINCE_ORIGIN, y.STATUS, y.CREATE_CHANNEL, d.MODEL, d.COLOR,
			ROW_NUMBER() OVER (PARTITION BY x.TRANSACTION_DATE, x.VEHICLE_LICENSE_1 || ' ' || x.VEHICLE_LICENSE_2, x.DESCRIPTION ORDER BY x.TRANSACTION_ID) AS RN
FROM (
SELECT 	a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'MEMBER' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('PARAMYESTERDAY 17:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMTODAY 11:00:00', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE LEFT JOIN CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('PARAMYESTERDAY 17:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMTODAY 11:00:00', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0
UNION
	SELECT  a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'NON-MEMBER' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM 	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM NONMEMBER_SERVICE.MF_NONM_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('PARAMYESTERDAY 17:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMTODAY 11:00:00', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN NONMEMBER_SERVICE.MF_NONM_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE
			LEFT JOIN NONMEMBER_SERVICE.MF_NONM_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('PARAMYESTERDAY 17:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMTODAY 11:00:00', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0 
    UNION 
    SELECT 	a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'ILLIGAL' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM 	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('PARAMYESTERDAY 17:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMTODAY 11:00:00', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('PARAMYESTERDAY 17:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('PARAMTODAY 11:00:00', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0
	) x
    LEFT JOIN 
    (
	SELECT * FROM 	(
						SELECT a.STATUS, b.CREATE_CHANNEL, b.TRANSACTION_ID FROM INVOICE_SERVICE.MF_INVOICE a JOIN INVOICE_SERVICE.MF_INVOICE_DETAIL b ON a.INVOICE_NO = b.INVOICE_NO
					UNION
						SELECT a.STATUS, b.CREATE_CHANNEL, b.TRANSACTION_ID FROM INVOICE_SERVICE.MF_INVOICE_NONMEMBER a JOIN INVOICE_SERVICE.MF_INVOICE_DETAIL_NONMEMBER b ON a.INVOICE_NO = b.INVOICE_NO
					)
	) y ON x.TRANSACTION_ID = y.TRANSACTION_ID
	INNER JOIN VPROFI_M9.WATCHLIST_MASTER d ON d.PLATE1 = x.VEHICLE_LICENSE_1 AND d.PLATE2 = x.VEHICLE_LICENSE_2 AND x.DESCRIPTION LIKE d.PROVINCE
	LEFT JOIN VPROFI_M9.WATCHLIST_TYPE e ON d.LIST_TYPE = e.ID
	WHERE e.LIST_NAME = 'Blue_plus' 
	) x WHERE RN= 1
ORDER BY TRANSACTION_DATE"""
    sql = sql.replace('PARAMTODAY', str(today))
    sql = sql.replace('PARAMYESTERDAY', str(yesterday))
    pdOra = pd.read_sql(sql, con=con_ora)

    return pdOra


def getSizeColumn(data):
    max_len = max((len(str(value)) for value in data), default=0)
    return max_len


def get_file_path(filename, root_directory="."):
    # Search for the file in the specified root directory and its subdirectories
    for root, dirs, files in os.walk(root_directory):
        if filename in files:
            # If the file is found, return its full path
            return os.path.join(root, filename)

    # If the file is not found, return None
    return None


def excelBlue_plusList(file_name, listData):
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()

    cols = [
        'Transaction ID',
        'วันที่',
        'ป้ายทะเบียน',
        'จังหวัด',
        'ประเภทของรถ',
        'ประเภทของผู้ใช้รถ',
        'รูปรถจาก Transaction',
        'รูปป้ายจาก Transaction',
        'รูปหลังจาก Transaction',
        '',
        'รูปรถจากระบบ',
        'รูปป้ายจากระบบ',
        'รูปหลังจากระบบ',
        '',
        '',
        '',
        '',
        '',
        'ป้ายที่เบียนที่ถูกต้อง'
    ]
    for i, val in enumerate(cols):
        worksheet.write(0, i, val)
    idx = 0
    filename = {}
    minio_client = Minio(MINIO_HOST,
                         access_key=MINIO_ACCESS_KEY,
                         secret_key=MINIO_SECRET_KEY,
                         secure=False)
    for _, row in listData.iterrows():
        # Replace NaN or None with an empty string
        row = row.fillna('')

        body_path = ''
        if row['BODY_PATH_PIC']:
            body_path = 'https://api.mflowthai.com/fileservice/api/v1/downloadFile/' + \
                row['BODY_PATH_PIC'] + '?key='

        plate_path = ''
        if row['PLATE_PATH_PIC']:
            plate_path = 'https://api.mflowthai.com/fileservice/api/v1/downloadFile/' + \
                row['PLATE_PATH_PIC'] + '?key='

        back_path = ''
        if row['BACK_PATH_PIC']:
            back_path = 'https://api.mflowthai.com/fileservice/api/v1/downloadFile/' + \
                row['BACK_PATH_PIC'] + '?key='

        img_body = f'=IMAGE(T{idx+2}&W{idx+2})'
        img_plate = f'=IMAGE(U{idx+2}&W{idx+2},2)'
        img_back = f'=IMAGE(V{idx+2}&W{idx+2})'

        bucket_name = "watchlist-image"

        worksheet.write(idx+1, 0, row['TRANSACTION_ID'] or "")
        worksheet.write(idx+1, 1, str(row['TRANSACTION_DATE']) or "")
        worksheet.write(
            idx+1, 2, row['VEHICLE_LICENSE_1']+' '+row['VEHICLE_LICENSE_2'] or "")
        worksheet.write(idx+1, 3, row['DESCRIPTION'] or "")
        worksheet.write(idx+1, 4, row['VEHICLE_CLASS'] or "")
        worksheet.write(idx+1, 5, row['MEM_TYPE'] or "")
        worksheet.write(idx+1, 6, img_body)
        worksheet.write(idx+1, 7, img_plate)
        worksheet.write(idx+1, 8, img_back)
        body_origin = ''
        if row['IMG_BODY']:
            body_origin = row['IMG_BODY'].replace('body/', '')
            minio_client.fget_object(bucket_name, row['IMG_BODY'], body_origin)
            filename[body_origin] = body_origin
            with Image.open(body_origin) as img:
                _, img_height = img.size

            desired_height = 50
            scale_factor = desired_height / img_height
            worksheet.insert_image('K'+str(idx+2), body_origin, {'x_scale': scale_factor, 'y_scale': scale_factor})
        else:
            worksheet.write(idx+1, 10, body_origin)
            
        plate_origin = ''
        if row['IMG_PLATE']:
            plate_origin = row['IMG_PLATE'].replace('plate/', '')
            minio_client.fget_object(bucket_name, row['IMG_PLATE'], plate_origin)
            filename[plate_origin] = plate_origin
            with Image.open(plate_origin) as img:
                img_width, img_height = img.size

            desired_width = 350
            scale_factor = desired_width / img_width
            worksheet.insert_image('L'+str(idx+2), plate_origin, {'x_scale': scale_factor, 'y_scale': scale_factor})
        else:
            worksheet.write(idx+1, 11, plate_origin)
            
        back_origin = ''
        if row['IMG_BACK']:
            back_origin = row['IMG_BACK'].replace('back/', '')
            minio_client.fget_object(bucket_name, row['IMG_BACK'], back_origin)
            filename[back_origin] = back_origin
            with Image.open(back_origin) as img:
                _, img_height = img.size

            desired_height = 50
            scale_factor = desired_height / img_height
            worksheet.insert_image('M'+str(idx+2), back_origin, {'x_scale': scale_factor, 'y_scale': scale_factor})
        else:
            worksheet.write(idx+1, 12, back_origin)
            
        worksheet.write(idx+1, 18, row['PLATE1_ORIGIN']+' ' +
                        row['PLATE2_ORIGIN']+' '+row['PROVINCE_ORIGIN'] or "")
        worksheet.write(idx+1, 19, body_path)
        worksheet.write(idx+1, 20, plate_path)
        worksheet.write(idx+1, 21, back_path)
        worksheet.write(idx+1, 22, KEY_IMAGE)
        
        
        worksheet.set_row(idx+1, 50)
        idx += 1

    worksheet.set_column('A:A', max(
        [getSizeColumn(listData['TRANSACTION_ID']), getSizeColumn(['Transaction ID'])]))
    worksheet.set_column('B:B', max(
        [getSizeColumn(listData['TRANSACTION_DATE']), getSizeColumn(['วันที่'])]))
    worksheet.set_column('C:C', max([getSizeColumn(
        listData['VEHICLE_LICENSE_1'])+getSizeColumn(listData['VEHICLE_LICENSE_2']), getSizeColumn(['ป้ายทะเบียน'])]))
    worksheet.set_column('D:D', max(
        [getSizeColumn(listData['DESCRIPTION']), getSizeColumn(['จังหวัด'])]))
    worksheet.set_column('E:E', max([getSizeColumn(listData['PLATE1_ORIGIN'])+getSizeColumn(
        listData['PLATE2_ORIGIN'])+getSizeColumn(listData['PROVINCE_ORIGIN']), getSizeColumn(['ป้ายที่เบียนที่ถูกต้อง'])]))
    worksheet.set_column('F:F', max(
        [getSizeColumn(listData['VEHICLE_CLASS']), getSizeColumn(['ประเภทของรถ'])]))
    worksheet.set_column('G:G', max(
        [getSizeColumn(listData['MEM_TYPE']), getSizeColumn(['ประเภทของผู้ใช้รถ'])]))
    worksheet.set_column('H:H', getSizeColumn(['รูปรถจาก Transaction']))
    worksheet.set_column('I:I', getSizeColumn(['รูปป้ายจาก Transaction']))
    worksheet.set_column('J:J', getSizeColumn(['รูปหลังจาก Transaction']))
    worksheet.set_column('K:K', 40)
    worksheet.set_column('L:L', 60)
    worksheet.set_column('M:M', 60)

    worksheet.set_column('I:I', 30)
    worksheet.set_column('M:M', 30)

    worksheet.set_column('S:W', None, None, {'hidden': True})
    worksheet.autofilter('A1:G1')
    workbook.close()


def getTransactionBlue_plusDayAgo(seven_day, today):
    con_ora, cursor_ora = condb()
    str_seven_day = seven_day.strftime("%Y-%m-%d")
    str_today = today.strftime("%Y-%m-%d")

    sql = """
                SELECT * FROM (SELECT x.*, d.IMG_BODY, d.IMG_PLATE, d.IMG_BACK, d.PLATE1_ORIGIN, d.PLATE2_ORIGIN, d.PROVINCE_ORIGIN, y.STATUS, y.CREATE_CHANNEL, d.MODEL, d.COLOR,
			ROW_NUMBER() OVER (PARTITION BY x.TRANSACTION_DATE, x.VEHICLE_LICENSE_1 || ' ' || x.VEHICLE_LICENSE_2, x.DESCRIPTION ORDER BY x.TRANSACTION_ID) AS RN
FROM (
SELECT 	a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'MEMBER' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{} 08:00:00', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE LEFT JOIN CUSTOMER_SERVICE.MF_CUST_MEMBER_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{} 08:00:00', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0
UNION
	SELECT  a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'NON-MEMBER' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM 	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM NONMEMBER_SERVICE.MF_NONM_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{} 08:00:00', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN NONMEMBER_SERVICE.MF_NONM_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE
			LEFT JOIN NONMEMBER_SERVICE.MF_NONM_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{} 08:00:00', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0 
    UNION 
    SELECT 	a.TRANSACTION_ID, a.TRANSACTION_DATE, a.REF_TRANSACTION_ID, a.VEHICLE_LICENSE_1, a.VEHICLE_LICENSE_2, b.DESCRIPTION,
			CASE WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0001' THEN 'C1' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0002' THEN 'C2' WHEN a.VEHICLE_WHEEL_CODE = 'VWHEL0003' THEN 'C3' ELSE 'Unknown' END AS VEHICLE_CLASS,
			'ILLIGAL' AS MEM_TYPE, c.BODY_PATH_PIC, c.PLATE_PATH_PIC, back.BACK_PATH_PIC
	FROM 	( SELECT TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE , count(*)
				FROM VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANSACTION mnt
			WHERE mnt.TRANSACTION_DATE BETWEEN TO_DATE('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{} 08:00:00', 'YYYY-MM-DD HH24:MI:SS')
			GROUP BY TRANSACTION_DATE , VEHICLE_LICENSE_1 , VEHICLE_LICENSE_2 , VEHICLE_PROVINCE
			HAVING COUNT(*) = 1 ) non1
			INNER JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANSACTION a ON  a.TRANSACTION_DATE = non1.TRANSACTION_DATE AND a.VEHICLE_LICENSE_1 = non1.VEHICLE_LICENSE_1 AND a.VEHICLE_LICENSE_2 = non1.VEHICLE_LICENSE_2 AND a.VEHICLE_PROVINCE = non1.VEHICLE_PROVINCE
			INNER JOIN CUSTOMER_SERVICE.MF_CUST_MASTER_VEHICLE_OFFICE b ON a.VEHICLE_PROVINCE = b.CODE LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_ILLEGAL_TRANS_CAMERA c ON a.TRANSACTION_ID = c.TRANSACTION_ID
			LEFT JOIN INVOICE_SERVICE.MF_INVOICE_ROLLBACK e ON a.VEHICLE_LICENSE_1 = e.PLATE1 AND a.VEHICLE_LICENSE_2 = e.PLATE2 AND a.VEHICLE_PROVINCE = e.PROVINCE
			LEFT JOIN VERIFY_ILLEGAL_SERVICE.MF_VEILL_VERIFY_TRANS_CAMERA_BACK back ON a.REF_TRANSACTION_ID = back.REF_TRANSACTION_ID AND back.DELETE_FLAG = 0
	WHERE 	a.TRANSACTION_DATE BETWEEN TO_DATE('{} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('{} 08:00:00', 'YYYY-MM-DD HH24:MI:SS') AND a.FEE_AMOUNT > 0 AND a.DELETE_FLAG = 0
	) x
    LEFT JOIN 
    (
	SELECT * FROM 	(
						SELECT a.STATUS, b.CREATE_CHANNEL, b.TRANSACTION_ID FROM INVOICE_SERVICE.MF_INVOICE a JOIN INVOICE_SERVICE.MF_INVOICE_DETAIL b ON a.INVOICE_NO = b.INVOICE_NO
					UNION
						SELECT a.STATUS, b.CREATE_CHANNEL, b.TRANSACTION_ID FROM INVOICE_SERVICE.MF_INVOICE_NONMEMBER a JOIN INVOICE_SERVICE.MF_INVOICE_DETAIL_NONMEMBER b ON a.INVOICE_NO = b.INVOICE_NO
					)
	) y ON x.TRANSACTION_ID = y.TRANSACTION_ID
	INNER JOIN VPROFI_M9.WATCHLIST_MASTER d ON d.PLATE1 = x.VEHICLE_LICENSE_1 AND d.PLATE2 = x.VEHICLE_LICENSE_2 AND x.DESCRIPTION LIKE d.PROVINCE
	LEFT JOIN VPROFI_M9.WATCHLIST_TYPE e ON d.LIST_TYPE = e.ID
	WHERE e.LIST_NAME = 'Blue_plus' 
	) x WHERE RN= 1
ORDER BY TRANSACTION_DATE
            """.format(str_seven_day, str_today, str_seven_day, str_today, str_seven_day, str_today,str_seven_day, str_today, str_seven_day, str_today, str_seven_day, str_today)
    pdOra = pd.read_sql(sql, con=con_ora)

    return pdOra


def upload_file_to_minio(server_url, access_key, secret_key, bucket_name, folder_name, file_name, file_path):
    # สร้าง MinIO client
    minio_client = Minio(
        server_url,
        access_key=access_key,
        secret_key=secret_key,
        secure=False  # ตั้งค่าเป็น True หากใช้ HTTPS
    )
    
    print(bucket_name)
    print(folder_name)
    print(file_name)
    
    
    
    print(f"File path inside container: {file_path}")
    if not os.path.exists(file_path):
        print(f"Error: File does not exist at {file_path}")
    elif not os.access(file_path, os.R_OK):
        print(f"Error: File is not readable at {file_path}")
    elif os.stat(file_path).st_size == 0:
        print(f"Error: File is empty at {file_path}")
    else:
        pass

    # ตรวจสอบว่ามี Bucket หรือไม่ และสร้าง Bucket ถ้ายังไม่มี
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # อัปโหลดไฟล์ไปยังโฟลเดอร์ที่กำหนดใน Bucket
    try:
        # หมายเหตุ: ชื่อโฟลเดอร์ต้องลงท้ายด้วย '/'
        minio_client.fput_object(bucket_name, f"{folder_name}/{file_name}", file_path)
        print(f"ไฟล์ '{file_name}' ถูกอัปโหลดไปยัง '{folder_name}' ใน Bucket '{bucket_name}' เรียบร้อยแล้ว")
    except S3Error as e:
        print(f"เกิดข้อผิดพลาด: {e}")
        
def download_file_to_dataframe(server_url, access_key, secret_key, bucket_name, object_name):
    """
    ดาวน์โหลดไฟล์จาก MinIO และแปลงเนื้อหาเป็น Pandas DataFrame
    :param server_url: URL ของ MinIO Server (เช่น "play.min.io")
    :param access_key: Key สำหรับเข้าถึง MinIO
    :param secret_key: Secret Key สำหรับเข้าถึง MinIO
    :param bucket_name: ชื่อ Bucket ใน MinIO
    :param object_name: ชื่อไฟล์ (object) ที่จะดาวน์โหลดจาก Bucket
    :return: Pandas DataFrame ที่ได้จากไฟล์
    """
    # สร้าง MinIO client
    minio_client = Minio(
        server_url,
        access_key=access_key,
        secret_key=secret_key,
        secure=False  # ตั้งค่าเป็น True หากใช้ HTTPS
    )

    try:
        # ดึงไฟล์จาก MinIO
        response = minio_client.get_object(bucket_name, object_name)

        # แปลงไฟล์เป็น Pandas DataFrame
        # ใช้ pd.read_excel() สำหรับไฟล์ Excel
        df = pd.read_excel(BytesIO(response.read()), engine='openpyxl')

        # ปิดการเชื่อมต่อ
        response.close()
        response.release_conn()

        print(
            f"ไฟล์ '{object_name}' ถูกดาวน์โหลดและแปลงเป็น DataFrame เรียบร้อยแล้ว")
        return df

    except S3Error as e:
        print(f"เกิดข้อผิดพลาด: {e}")
        return None