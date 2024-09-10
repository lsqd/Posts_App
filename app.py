import os
import json

from quart import Quart, jsonify, Response
from elasticsearch import AsyncElasticsearch

import motor.motor_asyncio

config_path = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_path) as config_file:
    config = json.load(config_file)

app = Quart(__name__)
ELASTIC_URL = config.get("ELASTIC_URL")
es = AsyncElasticsearch(ELASTIC_URL)
index_name = config.get("INDEX_NAME")

MONGODB_HOST = config.get("MONGODB_HOST")
MONGODB_PORT = config.get("MONGODB_PORT")
DB_NAME = config.get("DB_NAME")
COLLECTION_NAME = config.get("COLLECTION_NAME")
client = motor.motor_asyncio.AsyncIOMotorClient(f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}')
collection = client[DB_NAME][COLLECTION_NAME]

@app.route('/posts/<text>', methods=['GET'])
async def search_text_by_text(text):
    try:
        data_collection = []
        response = await es.search(
            index=index_name,
            body={
                "_source": ["uuid"],
                "query":{
                    "match":{
                        "text": text
                    }
                },
                "sort":[
                    {"created_date": {"order": "desc"}}
                ],
                "size": 20,
                "from": 0,
            }
        )
        
        for hit in response['hits']['hits']:
            uuid = int(hit['_source']['uuid'])
            db_answer = await collection.find_one({"uuid": uuid})
            db_answer["_id"] = str(db_answer["_id"])
            data_collection.append(db_answer)
        return jsonify(data_collection), 200
    except Exception as error:
        return jsonify({"error": str(error)}), 503

@app.route('/posts/<int:uuid>', methods=['DELETE'])
async def remove_post_by_id(uuid):
    try:
        await es.delete_by_query(index=index_name, body={"query": {"term": {"uuid": uuid}}})
        result = await collection.delete_one({"uuid": uuid})
        if result.deleted_count == 0:
            return jsonify({"response": "No elements to delete"}), 204
        return Response(status=204)
    except Exception as error:
        print(error)
        return jsonify({"error": "Service temporarily unavailable"}), 503

if __name__ == '__main__':
    app.run(debug=False, host='localhost', port=8080)