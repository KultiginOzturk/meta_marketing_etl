import pandas as pd
# import boto3
# from pyasn1.type import opentype
# import s3fs
# import sqlalchemy
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
import pymysql

# creds

google_sheets_api_key = {
    "type": "service_account",
    "project_id": "project_id",
    "private_key_id": "private_key_id",
    "private_key": "private_key",
    "client_email": "client_email",
    "client_id": "client_id",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://accounts.google.com/o/oauth2/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "client_x509_cert_url"
}

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_dict(google_sheets_api_key, DEFAULT_SCOPES)

gc = gspread.authorize(credentials)
sh = gc.open_by_url(
    'sh')

host = "host"
user = "admin"
password = "password"
db_name = "db_name"
port = 3306

# brand : [[sheet_id, country]
dict_accounts = {
    # 'brand_1' : [[1262044132, 'UAE']],
    # 'brand_2' : [[682592829,'UAE'],[1739127585,'KSA']],
    'brand_3': [[1838407881, 'KSA'], [495581625, 'UAE'], [1580731341, 'TUR']],
    # 'brand_4' : [[553145781, 'KSA']],
    # 'brand_5' : [[1015295323, 'KSA'],[0,'UAE']],
    # 'brand_6' : [[993505195, 'POL']],
    # 'brand_7' : [[1645093494, 'UAE']],
    'brand_8': [[994441, 'TUR'], [1650771495, "TUR"]],
    'brand_9': [[1971520218, 'TUR']],
    'brand_10': [[123822505, 'TUR']],
    'brand_11': [[748700447, 'TUR']]
}
cnxn = pymysql.connect(host=host, user=user, password=password, db=db_name)
cursor = cnxn.cursor()


def FB_ETL(sheet, brand):
    # delete the data from the table for that brand

    sql = f"DELETE FROM db_name.table_name where brand = '{brand}'"
    try:
        cursor.execute(sql)
    except:
        cnxn = pymysql.connect(host=host, user=user, password=password, db=db_name)
        cursor = cnxn.cursor()
        try:
            # condition to check if the table exists
            cursor.execute(sql)
        except:
            pass

    df = pd.DataFrame()

    for key in range(len(sheet)):
        print("Running for brand: ", brand, " and country: ", sheet[key][1])
        worksheet = sh.get_worksheet_by_id(sheet[key][0])
        list_of_lists = worksheet.get_all_values()
        columns = []

        for i in list_of_lists[0]:
            columns.append(
                i.lower().replace('-', '_').replace(' ', '_').replace('__', '_').replace('::', '_').replace(',', '_'))

        df_temp = pd.DataFrame(list_of_lists[1:], columns=columns)

        df_temp.head()

        ## df = df.iloc[:, [0,1,2,3,4,5,6,7,8,9,10,11,12,13]]

        df_temp = df_temp.rename(
            columns={'data.account_name': 'account_name', 'data.campaign_name': 'campaign_name', 'data.spend': 'spend',
                     'data.reach': 'reach', 'data.impressions': 'impressions', 'data.cpm': 'cpm',
                     'data.clicks': 'clicks',
                     'data.cpc': 'cpc', 'data.date_start': 'date_start', 'data.date_stop': 'date_stop'})

        df_temp['brand'] = brand
        df_temp["country"] = sheet[key][1]

        df_temp.head()
        # merge the dataframes if more than one country
        df = pd.concat([df_temp, df], ignore_index=True)

    return df


# execute the function for each brand
df_main = pd.DataFrame()
for i in dict_accounts:
    df = FB_ETL(dict_accounts[i], i)
    df_main = pd.concat([df_main, df], ignore_index=True)

# insert into the database
engine = create_engine(
    "mysql+mysqlconnector://{user}:{pw}@{host}/{db}".format(user=user, pw=password, host=host, db=db_name))
df_main.to_sql(f'table_name', con=engine, if_exists='replace', index=False)

cnxn = pymysql.connect(host=host, user=user, password=password, db=db_name)
cursor = cnxn.cursor()

cursor.callproc('db_name.table_name')

cnxn.commit()
cursor.close()
print("finished script!")
