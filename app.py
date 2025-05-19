from datetime import timedelta,date
import sys
import os

from script_blue_plus import *
from dotenv import load_dotenv

load_dotenv()
argument = sys.argv[1]

MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")


today = date.today() - timedelta(days=0)
yesterday = today - timedelta(days=1)

print(argument)

# Day
if argument.upper() == 'A':
    
    folder_path = os.path.join('G:/My Drive/WatchList/', 'Blue_plus',str(today))
    folder_path_day_1 = os.path.join('G:/My Drive/WatchList/', 'Blue_plus_1_Day_Ago',str(today))
    os.makedirs(folder_path, exist_ok=True)
    os.makedirs(folder_path_day_1, exist_ok=True)
    file_name = f"Blue_plus_List_{str(today)}_Day.xlsx"
    file_path = os.path.join(folder_path, file_name)
    
    yellowList = getTransactionBlue_plus(today, '11:00:00').sort_values(by='TRANSACTION_DATE')
    
    file_name_day_1 = f"Blue_plus_List_{str(today)}-Day-1.xlsx"
    file_path_day_1 = os.path.join(folder_path_day_1, file_name_day_1)
    
    if os.path.isfile(file_path_day_1):
        blue_plus_day_1 = pd.read_excel(file_path_day_1)
        column_array = blue_plus_day_1['Transaction ID'].values
        yellowList = yellowList[~yellowList['TRANSACTION_ID'].isin(column_array)]
    # yellowListYesterday = getTransactionBlue_plusYesterday(yesterday, today).sort_values(by='TRANSACTION_DATE')
    # merged_df = pd.merge(yellowList, yellowListYesterday,
    #                      how='outer', indicator=True)
    result_df = yellowList.sort_values(by='TRANSACTION_DATE')
    file_save = file_name
    # if os.path.exists('G:/'):
    #     excelBlue_plusList(file_path, result_df)
    #     file_save = file_path
    excelBlue_plusList(file_name, result_df)
    upload_file_to_minio(
        server_url=MINIO_HOST, 
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=MINIO_BUCKET_NAME,
        folder_name="WatchList/Blue_plus/"+str(today), 
        file_name=file_name, 
        file_path=file_save 
    )
    os.remove(file_name)
    print(f"Save {file_path} Successfully")
    # lineNotify("Run Script A Blue_plus List")

# Night
elif argument.upper() == 'B':
    folder_path = os.path.join('G:/My Drive/WatchList/', 'Blue_plus',str(today))
    os.makedirs(folder_path, exist_ok=True)
    
    file_name_night = f"Blue_plus_List_{str(today)}_Night.xlsx"
    file_path = os.path.join(folder_path, file_name_night)
    file_name_day = f"Blue_plus_List_{str(today)}_Day.xlsx"
    file_name_afternoon = f"Blue_plus_List_{str(today)}_Afternoon.xlsx"
        
    file_name_day_1 = f"Blue_plus_List_{str(today)}-Day-1.xlsx"
    
    folder_path_minio = os.path.join("WatchList", "Blue_plus", str(today)).replace("\\", "/")
    folder_path_minio_1 = os.path.join("WatchList", "Blue_plus_1_Day_Ago", str(today)).replace("\\", "/")
    
    
    df_blue_plus_day_1 = download_file_to_dataframe(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, os.path.join(folder_path_minio_1, file_name_day_1).replace("\\", "/"))
    df_blue_plus_day = download_file_to_dataframe(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, os.path.join(folder_path_minio, file_name_day).replace("\\", "/"))
    df_blue_plus_afternoon = download_file_to_dataframe(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, os.path.join(folder_path_minio, file_name_afternoon).replace("\\", "/"))
    
    df_result = pd.concat([df_blue_plus_day_1, df_blue_plus_day, df_blue_plus_afternoon], ignore_index=True)
    
    if not df_result.empty:
        
        column_array = df_result['Transaction ID'].values
        yellowListYesterday = getTransactionBlue_plus(today, '16:00:00').sort_values(by='TRANSACTION_DATE')
        filtered_df = yellowListYesterday[~yellowListYesterday['TRANSACTION_ID'].isin(column_array)]
    else:
        filtered_df = getTransactionBlue_plus(today, '16:00:00').sort_values(by='TRANSACTION_DATE')
    result_df = filtered_df
    
    file_save = file_name_night
    # if os.path.exists('G:/'):
    #     excelBlue_plusList(file_path, result_df)
    #     file_save = file_path
    excelBlue_plusList(file_save, result_df)
    upload_file_to_minio(
        server_url=MINIO_HOST,  # เช่น "play.min.io"
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=MINIO_BUCKET_NAME,
        folder_name="WatchList/Blue_plus"+"/"+str(today),  
        file_name=file_name_night, 
        file_path=file_save  
    )
    os.remove(file_name_night)
    print(f"Save {file_path} Successfully")
    
    
# Day-1
elif argument.upper() == 'C':
    folder_path = os.path.join('G:/My Drive/WatchList/', 'Blue_plus_1_Day_Ago', str(today))
    os.makedirs(folder_path, exist_ok=True)
    seven_day_ago = today - timedelta(days = 2)
    day = today.day

    # List to hold DataFrames
    dfs = []
    

    # Loop through the last 7 days in reverse order
    for i in range(day-2,day):
        # Calculate the date for each iteration
        date = today - timedelta(days=day - i)
        date_str = date.strftime("%Y-%m-%d")
        
        
        # Construct the folder path
        
        folder_path_minio = os.path.join("WatchList", "Blue_plus", date_str).replace("\\", "/")
        folder_path_minio_1 = os.path.join("WatchList", "Blue_plus_1_Day_Ago", date_str).replace("\\", "/")
        
        folder_path2 = os.path.join('G:/My Drive/WatchList/', 'Blue_plus', date_str)
        folder_path3 = os.path.join('G:/My Drive/WatchList/', 'Blue_plus_1_Day_Ago', date_str)
        
        
        
        # Construct the file names
        file_name_night = f"Blue_plus_List_{date_str}_Night.xlsx"
        file_name_day = f"Blue_plus_List_{date_str}_Day.xlsx"
        file_name_afternoon = f"Blue_plus_List_{date_str}_Afternoon.xlsx"
        
        file_name_day_1 = f"Blue_plus_List_{date_str}-Day-1.xlsx"
        
        # Construct the full file paths
        df_path_day_1 = download_file_to_dataframe(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, os.path.join(folder_path_minio_1, file_name_day_1).replace("\\", "/"))
        df_path_day = download_file_to_dataframe(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, os.path.join(folder_path_minio, file_name_day).replace("\\", "/"))
        df_path_afternoon = download_file_to_dataframe(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, os.path.join(folder_path_minio, file_name_afternoon).replace("\\", "/"))
        df_path_night = download_file_to_dataframe(MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, os.path.join(folder_path_minio, file_name_night).replace("\\", "/"))
        
        dfs.append(df_path_day_1)
        dfs.append(df_path_day)
        dfs.append(df_path_afternoon)
        dfs.append(df_path_night)
        
    # Concatenate all DataFrames
    
    final_df = pd.DataFrame()
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
    str_seven_day_ago = seven_day_ago.strftime("%Y-%m-%d")
    str_today = today.strftime("%Y-%m-%d")
    if final_df.empty:
        file_name = "Blue_plus_List_{}-Day-1.xlsx".format(str_today)
        file_path = os.path.join(folder_path, file_name)
        
        # Assuming getTransactionBlue_plusDayAgo and excelBlue_plusList are defined functions
        yellowList = getTransactionBlue_plusDayAgo(seven_day_ago, today)
        filtered_df = yellowList
    else:
        column_array = final_df['Transaction ID'].values
        
        
        file_name = "Blue_plus_List_{}-Day-1.xlsx".format(str_today)
        file_path = os.path.join(folder_path, file_name)
        
        # Assuming getTransactionBlue_plusDayAgo and excelBlue_plusList are defined functions
        
        yellowList = getTransactionBlue_plusDayAgo(seven_day_ago, today)
        filtered_df = yellowList[~yellowList['TRANSACTION_ID'].isin(column_array)]
        
    file_save = file_name
    
    # if os.path.exists('G:/'):
    #     excelBlue_plusList(file_path, filtered_df)
    #     file_save = file_path
    excelBlue_plusList(file_name, filtered_df)
    upload_file_to_minio(
        server_url=MINIO_HOST,  # เช่น "play.min.io"
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=MINIO_BUCKET_NAME,
        folder_name="WatchList/Blue_plus_1_Day_Ago"+"/"+str(str_today),  
        file_name=file_name,  
        file_path=file_save  
    )

    os.remove(file_name)
    print(f"Saved {file_path} Successfully")

# Afternoon
elif  argument.upper() == 'D':
    folder_path = os.path.join('G:/My Drive/WatchList/', 'Blue_plus',str(today))
    folder_path2 = os.path.join('G:/My Drive/WatchList/', 'Blue_plus_1_Day_Ago',str(today))
    os.makedirs(folder_path, exist_ok=True)
    file_name = f"Blue_plus_List_{str(today)}_Afternoon.xlsx"
    file_name2 = f"Blue_plus_List_{str(today)}_Day.xlsx"
    file_name3 = f"Blue_plus_List_{str(today)}-Day-1.xlsx"
    file_path = os.path.join(folder_path, file_name)
    file_path2 = os.path.join(folder_path, file_name2)
    file_path3 = os.path.join(folder_path2, file_name3)
    if os.path.isfile(file_path2):
        yellowList = pd.read_excel(file_path2)
        yellowList2 = pd.read_excel(file_path3)
        column_array = yellowList['Transaction ID'].values
        column_array2 = yellowList2['Transaction ID'].values
        yellowListYesterday = getTransactionBlue_plus(today, '14:00:00').sort_values(by='TRANSACTION_DATE')
        filtered_df = yellowListYesterday[~yellowListYesterday['TRANSACTION_ID'].isin(column_array)]
        filtered_df = filtered_df[~filtered_df['TRANSACTION_ID'].isin(column_array2)]
    else:
        filtered_df = getTransactionBlue_plus(today, '14:00:00').sort_values(by='TRANSACTION_DATE')
    result_df = filtered_df
    file_save = file_name
    # if os.path.exists('G:/'):
    #     excelBlue_plusList(file_path, result_df)
    #     file_save = file_path
    excelBlue_plusList(file_name, result_df)
    upload_file_to_minio(
        server_url=MINIO_HOST,  # เช่น "play.min.io"
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=MINIO_BUCKET_NAME,
        folder_name="WatchList/Blue_plus"+"/"+str(today),  
        file_name=file_name, 
        file_path=file_save  
    )
    os.remove(file_name)
    print(f"Save {file_path} Successfully")
    # lineNotify("Run Script B Blue_plus List")
else:
    pass




