# %pip install python-facebook-api
import datetime as dt
import mysql.connector
import pandas as pd
import requests
from sqlalchemy import create_engine

# creating time ranges to loop over
start = (dt.datetime.today() - dt.timedelta(days=30)).strftime("%Y-%m-%d")
end = dt.datetime.today().strftime("%Y-%m-%d")

start = "2023-09-01"

date_range = pd.date_range(start=start, end=end, freq="D").strftime("%Y-%m-%d").tolist()

freq = 3
loop_len = int(len(date_range) / freq)

time_ranges = []
for i in range(loop_len):
    if i == loop_len - 1:
        # print(date_range[freq*i],date_range[-1])
        time_range = "{'since':'" + date_range[freq * i] + "','until':'" + date_range[-1] + "'}"
    else:
        # print(date_range[freq*i],date_range[freq*i+freq-1])
        time_range = "{'since':'" + date_range[freq * i] + "','until':'" + date_range[freq * i + freq - 1] + "'}"
    time_ranges.append(time_range)

access_token_1 = "access_token_1"
access_token_2 = "access_token_2"

account_id = "account_id"

dict_accounts = {
    'brand 1': [[864107448075747, 'UAE'], [941340933424515, 'KSA'], access_token_1],
    'brand 2': [[824345385317873, 'KSA'], [470947878504528, 'UAE'], [1109620456281826, 'TUR'], access_token_1],
    'brand 3': [[1122027738538743, 'KSA'], access_token_1],
    'brand 4': [[816244976323334, 'KSA'], [426958348912707, 'UAE'], access_token_1],
    'brand 5': [[378810850489088, 'TUR'], [3156786734649298, "TUR"], [3507504466043471, "TUR"], access_token_1],
    'brand 6': [[3318628638395980, 'TUR'], [3273585949618928, 'TUR'], access_token_1],
    'brand 7': [[1196901734546016, 'TUR'], access_token_1],
    'brand 8': [[600719318268673, "TUR"], [179527491644132, "TUR"], access_token_2]
}


def fb_insights(access_token, account_id, time_ranges):
    params = {
        'date_preset': 'this_month',
        'fields': ['account_name', 'account_currency', 'ad_name', 'adset_name', 'campaign_name', 'spend', 'reach',
                   'impressions', 'clicks', 'purchase_roas'],
        'level': 'ad',
        'limit': '4000',
        'time_increment': '1',
        'breakdowns': 'country'

    }
    fields = "%2C".join(params["fields"])

    row_data = []
    # fetching data from facebook api at 3 day frequency
    for time_range in time_ranges:
        # print(time_range)
        base_url = f"https://graph.facebook.com/v16.0/act_{account_id}/insights?time_range={time_range}&breakdowns={params['breakdowns']}&time_increment={params['time_increment']}&fields={fields}&level={params['level']}&limit={params['limit']}&access_token={access_token}"

        r = requests.get(url=base_url)

        # extracting data in json format
        data = r.json()
        try:
            output = data["data"]
            # print(data)
        except:
            print(data)
            continue

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
            country_source = i["country"]
            roas = 0.0
            try:
                for roas_item in i["purchase_roas"]:
                    try:
                        roas += float(roas_item["value"])
                        # print(roas)
                    except:
                        roas += 0.0
                    # print(i)
            except:
                pass
            date_start = i["date_start"]
            date_stop = i["date_stop"]
            row_data.append(
                [account_name, account_currency, country_source, ad_name, adset_name, campaign_name, spend, reach,
                 impressions, clicks, roas, date_start, date_stop])

    df = pd.DataFrame(row_data, columns=["account_name", "account_currency", "country_source", "ad_name", "adset_name",
                                         "campaign_name", "spend", "reach", "impressions", "clicks", "roas",
                                         "date_start", "date_stop"])

    return df


df_list = []
for i in dict_accounts:
    # print(i,dict_accounts[i][-1])
    access_token = dict_accounts[i][-1]

    # loop for each brand country
    for brand in dict_accounts[i]:
        if len(brand) == 2:
            print(i, brand[1])
            df_temp = fb_insights(access_token, brand[0], time_ranges)
            df_temp["brand"] = i
            df_temp["country"] = brand[1]
            df_list.append(df_temp)

df = pd.concat(df_list, ignore_index=True)

host = "host"
user = "admin"
password = "password"
db_name = "db_name"
port = 3306

mydb = mysql.connector.connect(host=host, user=user, port=port, passwd=password, db=db_name)
cursor = mydb.cursor()

query = '''select concat('KILL ',id,'') as 'queue'
from information_schema.processlist
where Command = 'Sleep' and Time > '30' and user = 'admin' '''

kill_list = pd.read_sql(query, con=mydb)

for i in kill_list['queue']:
    cursor.execute(i)

# sql = f"DELETE FROM stg.facebook_api_raw where date(date_start) <= '{start}'"

sql = f"drop table if exists stg.facebook_api_raw"
cursor.execute(sql)

engine = create_engine(
    "mysql+mysqlconnector://{user}:{pw}@{host}/{db}".format(user=user, pw=password, host=host, db=db_name))

df.to_sql(f'table_name', con=engine, if_exists='replace', index=False, chunksize=5000, method='multi')

cursor.close()
print("finished script!")
