# !/usr/bin/env python3
import pandas as pd
import requests
import json
from datetime import datetime
from pymongo import MongoClient
import configparser
from sqlalchemy import create_engine, text
from airflow.hooks.postgres_hook import PostgresHook


config = configparser.ConfigParser()
config.read('config.ini')
global mongo_engine, postgres_engine
mongo_engine, postgres_engine = None, None

print("------------------------------------------------------------------------------------")
print(f"Batch started at {datetime.now()}", end='\n')
global master_id
master_id = []


def save_master_details(data):
    master_df = pd.DataFrame([])

    data = data['data']
    for i in data:
        new_df = pd.DataFrame([{"id": i['id'], "symbol": i["symbol"], "name": i['name'],
                                "explorer": i["explorer"]}])

        master_df = pd.concat([master_df, new_df], ignore_index=True)
    master_df.to_sql('cryptocurreny_master', con=postgres_engine, if_exists='append', index=False)

    print("saved successfully")


def get_connection(user, password, host, port, database):
    return create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )


def get_engine(db_user, password, db_name, db_host="localhost", port="5432"):
    print("Creating postgresDB engine")
    try:
        # engine = None
        # engine = get_connection(db_user, password, db_host, port, db_name)

        # with engine.connect() as connection:
        #     connection.execute(text("COMMIT;"))

        # print("Postgres engine created successfully")
        # engine.execute("commit")
   
       
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')  
        engine = pg_hook.get_sqlalchemy_engine()  
        return engine
    except Exception as e:
        print(f"Failed while connecting to DB {e}")


def invoke_db_engine():
    return get_engine(db_user="postgres", password="welcome", db_name="crypto_currency")


if postgres_engine is None:
    postgres_engine = invoke_db_engine()


def get_connection(host, port, username, password, database):
    try:
        client = MongoClient(host, port, username=username, password=password)
        db = client[database]
        db.list_collection_names()
        return client
    except Exception as e:
        return None


def invoke_mongodb():
    try:
        print("Invoking  mongoDB engine")
        client = get_connection("172.17.0.1", 27017, "anil", "welcome", "crypto_currency")
        if client:
            return client
        else:
            raise Exception("Connection to MongoDB failed")
    except Exception as e:
        print(f"Failed while connecting to MongoDB '172.17.0.1': {e}")


def fetch_crypto_details():
    try:
        url = "https://api.coincap.io/v2/assets"
        headers = {
            "Authorization": "Bearer 66bc99f3-6892-4b63-a1a8-d7caa946baf5"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            if response.text is not None and len(response.text) > 0:
                print("Data fetched successfully from API")
                return (json.loads(response.text))
            raise Exception("No data available from source")
        else:
            raise Exception("Unable to communicate with api")
    except Exception as e:
        print(f"Exception occurred while fetching the data {e}")


def save_to_mongodb(client, database_name, collection_name, data):
    try:
        db = client[database_name]
        collection = db[collection_name]
        collection.insert_one(data)
        print("Data inserted successfully in mongo DB")
    except Exception as e:
        print(f"Failed to insert data into MongoDB: {e}")


def insert_into_current_rates(data):
    master_df = pd.DataFrame([])
    filtered_data = pd.DataFrame([])
    data = data['data']
    for i in data:
        if i.get("id") not in master_id:
            new_df = pd.DataFrame([{"id": i['id'], "symbol": i["symbol"], "name": i['name'],
                                    'explorer': i['explorer']}])
            master_df = pd.concat([master_df, new_df], ignore_index=True)
        new_df = pd.DataFrame([{"id": i['id'], "symbol": i["symbol"], "rank": i['rank'],
                                "market_cap_usd": i["marketCapUsd"], "volume_usd": i['volumeUsd24Hr'],
                                "price_usd": i['priceUsd'], "change_percent": i['changePercent24Hr'],
                                "last_updated": datetime.now()
                                }])

        filtered_data = pd.concat([filtered_data, new_df], ignore_index=True)
    if not master_df.empty:
        master_df.to_sql('cryptocurreny_master', con=postgres_engine, if_exists='append', index=False)

    insert_query = text("insert into crypto_currency_rates_dump select * from crypto_currency_rates;")
    print("Moving to dump ")
    with postgres_engine.connect() as connection:
        connection.execute(insert_query)
        connection.execute(text("COMMIT;"))

    delete_query = text("delete from crypto_currency_rates; ")
    print("Truncating data")
    with postgres_engine.connect() as connection:
        connection.execute(delete_query)
        connection.execute(text("COMMIT;"))
    print("Inserting data")
    filtered_data.to_sql('crypto_currency_rates', con=postgres_engine, if_exists='append', index=False)
    print("Data saved successfully")


def fetch_master_details():
    query = "select id from cryptocurreny_master cm"
    data = pd.read_sql(query, con=postgres_engine)
    return data['id'].to_list()


def controller():
    try:
        global mongo_engine, postgres_engine, master_id
        #mongo_engine = invoke_mongodb()
        #if mongo_engine is None:
            #pass
            #raise Exception("Mongo DB inactive.. terminating session")

        if postgres_engine:
            print("DB engine is active..Proceeding further")
        else:
            raise Exception("DB inactive, aborting process")

        master_id = fetch_master_details()

        print(master_id)

        batch_details = fetch_crypto_details()

        #save_to_mongodb(mongo_engine, "crypto_currency", "dump", batch_details)

        insert_into_current_rates(batch_details)

        print(f"Batch successfully executed and ended at {datetime.now()}", end='\n')

        postgres_engine.dispose()


    except Exception as e:
        print(e)


print("------------------------------------------------------------------------------------")
