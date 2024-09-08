from elasticsearch import Elasticsearch
import os
import json

import pandas as pd

import motor.motor_asyncio

config_path = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_path) as config_file:
    config = json.load(config_file)

mapping = {
    "mappings": {
        "properties": {
            "created_date": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss"  # Формат даты
            }
        }
    }
}


ELASTIC_URL = config.get("ELASTIC_URL")
es = Elasticsearch(ELASTIC_URL)
index_name = config.get("INDEX_NAME")

MONGODB_HOST = config.get("MONGODB_HOST")
MONGODB_PORT = config.get("MONGODB_PORT")
DB_NAME = config.get("DB_NAME")
COLLECTION_NAME = config.get("COLLECTION_NAME")
client = motor.motor_asyncio.AsyncIOMotorClient(f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}')
collection = client[DB_NAME][COLLECTION_NAME]

     
if not es.indices.exists(index=index_name):
    response = es.indices.create(index=index_name, body=mapping)
    print(response)

db = client.POSTS
collection = db.posts

df = pd.read_csv('posts.csv')
df = df.drop(columns=['rubrics'])

df['uuid'] = df.index + 1
df.to_json('posts.json', orient='records', force_ascii=False, indent=4)

df = pd.read_json('posts.json', encoding='utf-8')
for i, row in df.iterrows():
    es.index(index=index_name, document=row.to_dict())
print('Files in ElasticSearch added')

df = pd.read_csv('posts.csv')
df['uuid'] = df.index + 1
df.to_json('mongo.json', orient='records', force_ascii=False, indent=4)
df = pd.read_json('mongo.json', encoding='utf-8')
for i, row in df.iterrows():
    collection.insert_one(row.to_dict())
print('Files in MongoDB added')

if os.path.exists('mongo.json'):
    os.remove('mongo.json')
if os.path.exists('posts.json'):
    os.remove('posts.json')
if os.path.exists('posts.json'):
    os.remove('posts.csv')