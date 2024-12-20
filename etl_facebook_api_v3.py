import time
import mysql.connector
import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
import datetime as dt
# creating time ranges to loop over
start = (dt.datetime.today() - dt.timedelta(days=10)).strftime("%Y-%m-%d")
end = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%Y-%m-%d")

date_range = pd.date_range(start=start, end=end, freq='D').strftime("%Y-%m-%d")
time_ranges = []

for i in date_range:
    time_range = "{'since':'" + i + "','until':'" + i + "'}"
    time_ranges.append(time_range)

brand_1_token = 'brand_1_token'

brand_2_token = 'brand_2_token'

brand_3_token = 'brand_3_token'

# Account Ids to pull data
dict_accounts = {
    'brand_1': [[378810850489088, 'TUR'], brand_1_token],
    # 'JV': [[3273585949618928, 'TUR'], scandi_token],
    # 'FL': [[1196901734546016, 'TUR'], scandi_token],
    # 'LU': [[600719318268673, 'TUR'], scandi_token],
    'brand_2': [[1109620456281826, 'TUR'], brand_2_token],
    # Yeni
    # 'BIO' : [[1598637611068356, 'TUR'], bio_token],
    'brand_3_1': [[805720728328738, 'TUR'], brand_3_token],
    'brand_3_2': [[1203209297536262, 'TUR'], brand_3_token],
    'brand_3_3': [[797722102205018, 'TUR'], brand_3_token],
}

dict_accounts_cpas = {
    'brand_1': [[3507504466043471, 'TUR'], brand_1_token],
    # 'brand_3_3':   [[3318628638395980, 'TUR'], brand_3_token],
}


# There are two seperate functions for the cpas and standart meta accounts. Reason is they have different fields and breakdowns to get correct data
# We can get conversion value, conversion quantity and addToCarts directly from "converted_product_value" and "converted_product_quantity" fields for the cpas acccounts
##For the meta accounts we should add "actions" field for the addToCarts and conversions. To calculate conversion_value we should add "purchase_roas" field
##Standart meta accounts have country breakdown but cpas accounts are all TUR


# function for the cpas accounts
def fb_insights_cpas(access_token, account_id, time_ranges):
    params = {
        'date_preset': 'this_month',
        'fields': ['account_name', 'account_currency', 'ad_name', 'adset_name', 'campaign_name', 'spend', 'reach',
                   'impressions', 'clicks', 'converted_product_value', 'converted_product_quantity', \
                   'inline_link_clicks'],
        'level': 'ad',
        'limit': '4000',
        'time_increment': '1'
    }

    fields = "%2C".join(params["fields"])

    row_data = []

    for time_range in time_ranges:
        # specifying fields, level and time ranges for the url.
        base_url = f"https://graph.facebook.com/v17.0/act_{account_id}/insights?time_range={time_range}&time_increment={params['time_increment']}&fields={fields}&level={params['level']}&limit={params['limit']}&access_token={access_token}"

        r = requests.get(url=base_url)
        if r.status_code == 400:
            time.sleep(5)
            r = requests.get(url=base_url)

        # extracting data in json format
        data = r.json()
        print(r.status_code)
        # print(r)
        try:
            output = data["data"]

        except Exception as e:
            print(data)
            raise e

        for i in output:
            account_name = i["account_name"]
            account_currency = i["account_currency"]
            ad_name = i["ad_name"]
            adset_name = i["adset_name"]
            campaign_name = i["campaign_name"]
            spend = i["spend"]
            reach = i["reach"]
            impressions = i["impressions"]
            clicks = i["clicks"]
            try:
                link_clicks = i['inline_link_clicks']
            except:
                link_clicks = 0

            ss = 0.0
            try:
                for x in i['converted_product_value']:

                    if (x['action_type'] == 'omni_purchase'):
                        ss = x['value']

                    else:
                        pass
            except:
                pass

            add_to_cart = 0
            conversion = 0
            try:
                for x in i['converted_product_quantity']:

                    if (x['action_type'] == 'omni_add_to_cart'):
                        add_to_cart = x['value']

                    elif (x['action_type'] == 'omni_purchase'):
                        conversion = x['value']

                    else:
                        pass
            except:
                pass

            date_start = i["date_start"]
            date_stop = i["date_stop"]

            try:
                roas = float(ss) / float(spend)
            except:
                roas = float(0)

            row_data.append(
                [account_name, account_currency, ad_name, adset_name, campaign_name, spend, reach, impressions, clicks,
                 link_clicks, ss, roas, add_to_cart, conversion, date_start, date_stop])

    df = pd.DataFrame(row_data,
                      columns=["account_name", "currency", "ad_name", "adset_name", "campaign_name", "spend", "reach",
                               "impressions", "clicks", "link_clicks", "sales", 'roas', 'add_to_cart', 'conversion',
                               "date_start", "date_stop"])

    return df


# function for the standart meta accounts
def fb_insights(access_token, account_id, time_ranges):
    params = {
        'date_preset': 'this_month',
        'fields': ['account_name', 'account_currency', 'ad_name', 'adset_name', 'campaign_name', 'spend', 'reach',
                   'impressions', 'clicks', 'purchase_roas', 'actions', 'inline_link_clicks'],
        'level': 'ad',
        'limit': '4000',
        'time_increment': '1'
    }

    fields = "%2C".join(params["fields"])

    row_data = []
    # specifying fields, level and time ranges for the url.
    for time_range in time_ranges:

        base_url = f"https://graph.facebook.com/v17.0/act_{account_id}/insights?time_range={time_range}&time_increment={params['time_increment']}&fields={fields}&level={params['level']}&limit={params['limit']}&access_token={access_token}&breakdowns=country"

        r = requests.get(url=base_url)
        if r.status_code == 400:
            time.sleep(5)
            r = requests.get(url=base_url)

        # extracting data in json format
        print(f'insight response for {account_id}\n{r.text}')
        data = r.json()
        print(r.status_code)
        try:
            output = data["data"]

        except Exception as e:
            print(data)
            raise e

        for i in output:
            country = i['country']
            account_name = i["account_name"]
            account_currency = i["account_currency"]
            ad_name = i["ad_name"]
            adset_name = i["adset_name"]
            campaign_name = i["campaign_name"]
            spend = i["spend"]
            try:
                reach = i["reach"]
            except:
                reach = 0
            try:
                impressions = i["impressions"]
            except:
                impressions = 0
            try:
                clicks = i["clicks"]
            except:
                clicks = 0
            try:
                link_clicks = i['inline_link_clicks']
            except:
                link_clicks = 0

            ss = 0.0
            roas = 0.0
            try:
                for x in i['purchase_roas']:

                    if (x['action_type'] == 'omni_purchase'):
                        roas = x['value']
                        ss = float(spend) * float(roas)

                    else:
                        pass
            except:
                pass

            add_to_cart = 0
            conversion = 0
            try:
                for x in i['actions']:

                    if (x['action_type'] == 'omni_add_to_cart'):
                        add_to_cart = x['value']

                    elif (x['action_type'] == 'omni_purchase'):
                        conversion = x['value']

                    else:
                        pass
            except:
                pass

            date_start = i["date_start"]
            date_stop = i["date_stop"]
            row_data.append(
                [account_name, account_currency, ad_name, adset_name, campaign_name, spend, reach, impressions, clicks,
                 link_clicks, ss, roas, add_to_cart, conversion, date_start, date_stop, country])

    df = pd.DataFrame(row_data,
                      columns=["account_name", "currency", "ad_name", "adset_name", "campaign_name", "spend", "reach",
                               "impressions", "clicks", "link_clicks", "sales", 'roas', 'add_to_cart', 'conversion',
                               "date_start", "date_stop", "country"])

    return df


# CPAS Part of the code, loop iterate over every cpas account and date-ranges to get specified data
print('starting to collect cpas accounts')
df_list = []
for i in dict_accounts_cpas:
    access_token = dict_accounts_cpas[i][-1]

    # loop for each brand country
    for brand in dict_accounts_cpas[i]:
        print(f'cpas fetching for {i} {brand}')
        if len(brand) == 2:
            print(i, brand[1])
            df_temp = fb_insights_cpas(access_token, brand[0], time_ranges)
            df_temp["brand"] = i
            df_temp["country"] = brand[1]
            df_temp['ad_type'] = 'cpas_ad'
            df_list.append(df_temp)
print('cpas accounts collect completed')

df = pd.concat(df_list, ignore_index=True)

# Standart Meta Part of the code, loop iterate over every cpas account and date-ranges to get specified data
print('starting to collect standart accounts')
df_list = []
for i in dict_accounts:
    access_token = dict_accounts[i][-1]

    # loop for each brand country
    for brand in dict_accounts[i]:
        print(f'account fetching for {i} {brand}')
        if len(brand) == 2:
            print(i, brand[1])
            df_temp = fb_insights(access_token, brand[0], time_ranges)
            df_temp["brand"] = i
            # df_temp["country"] = brand[1]
            df_temp['ad_type'] = 'meta_ad'
            df_list.append(df_temp)
print('standart accounts collect completed')

df1 = pd.concat(df_list, ignore_index=True)

# Converting Cpas values to their correct form
df.spend = df.spend.astype(float)
df.sales = df.sales.astype(float)
df.reach = df.reach.astype(float)
df.impressions = df.impressions.astype(float)
df.roas = df.roas.astype(float)
df.add_to_cart = df.add_to_cart.astype(int)
df.conversion = df.conversion.astype(int)
df.clicks = df.clicks.astype(int)
df.link_clicks = df.link_clicks.astype(int)

# Converting standart meta part values to their correct form
df1.spend = df1.spend.astype(float)
df1.sales = df1.sales.astype(float)
df1.reach = df1.reach.astype(float)
df1.impressions = df1.impressions.astype(float)
df1.roas = df1.roas.astype(float)
df1.add_to_cart = df1.add_to_cart.astype(int)
df1.conversion = df1.conversion.astype(int)
df1.clicks = df1.clicks.astype(int)
df1.link_clicks = df1.link_clicks.astype(int)

# Concatenating both tables to have one master table and adjusting dates column
df_consolidated = pd.concat([df, df1], axis=0)

df_consolidated['dates'] = df_consolidated.date_start

df_consolidated.drop(['date_start', 'date_stop'], axis=1, inplace=True)

# Adding channel column, renaming some columns to get standardized names, converting dates column to datetime
df_consolidated.reset_index(drop=True, inplace=True)

df_consolidated['channel'] = np.where(df_consolidated.ad_type == 'cpas_ad', 'Trendyol', 'D2C')

df_consolidated = df_consolidated[
    ['dates', 'ad_type', 'channel', 'brand', 'country', 'account_name', 'campaign_name', 'adset_name', 'ad_name',
     'currency', 'spend', 'reach', 'impressions', 'clicks', "link_clicks", 'sales',
     'roas', 'add_to_cart', 'conversion']]

df_consolidated.rename(columns={'spend': 'cost', 'sales': 'conversion_value'}, inplace=True)

df_consolidated.dates = pd.to_datetime(df_consolidated.dates)

df_consolidated.dates = df_consolidated.dates.apply(lambda x: x.date())

# Jeuvenile and Facelab cpas accounts are same, so seperating values for brand mapping. Also changing TR values to TUR
df_consolidated.loc[
    df_consolidated.campaign_name.str.contains('FL|Facelab') & (df_consolidated.ad_type == 'cpas_ad'), 'brand'] = 'FL'
df_consolidated.loc[df_consolidated.country == 'TR', 'country'] = 'TUR'

host = "host"
user = "admin"
password = "password"
db_name = "db_name"
port = 3306
import pymysql

mydb = mysql.connector.connect(host=host, user=user, port=port, passwd=password, db=db_name)
cursor = mydb.cursor()

query = '''select concat('KILL ',id,'') as 'queue'
from information_schema.processlist
where Command = 'Sleep' and Time > '30' and user = 'admin' '''

kill_list = pd.read_sql(query, con=mydb)

for i in kill_list['queue']:
    cursor.execute(i)

sql = f"drop table if exists db_name.table_name"
cursor.execute(sql)
engine = create_engine(
    "mysql+mysqlconnector://{user}:{pw}@{host}/{db}".format(user=user, pw=password, host=host, db=db_name))
df_consolidated.to_sql(f'table_name', con=engine, if_exists='replace', index=False)

# Update rows procedure
cnxn = pymysql.connect(host=host, user=user, password=password, db=db_name)
cursor = cnxn.cursor()

cursor.callproc('db_name.table_name')

cnxn.commit()
cursor.close()

host = "host"
user = "admin"
password = "password"
db_name = "db_name"
port = 3306
import pymysql

mydb = mysql.connector.connect(host=host, user=user, port=port, passwd=password, db=db_name)
cursor = mydb.cursor()

query = '''select concat('KILL ',id,'') as 'queue'
from information_schema.processlist
where Command = 'Sleep' and Time > '30' and user = 'admin' '''

kill_list = pd.read_sql(query, con=mydb)

for i in kill_list['queue']:
    cursor.execute(i)

sql = f"drop table if exists db_name.table_name"
cursor.execute(sql)
engine = create_engine(
    "mysql+mysqlconnector://{user}:{pw}@{host}/{db}".format(user=user, pw=password, host=host, db=db_name))
df_consolidated.to_sql(f'table_name', con=engine, if_exists='replace', index=False)

# Update rows procedure
cnxn = pymysql.connect(host=host, user=user, password=password, db=db_name)
cursor = cnxn.cursor()

cursor.callproc('db_name.table_name')

cnxn.commit()
cursor.close()