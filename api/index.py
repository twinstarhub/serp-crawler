import os
import time
from flask import Flask,jsonify
from duckduckgo_search import DDGS

from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
from pymongo.errors import BulkWriteError, ConnectionFailure

from mongo import MongoDBConnector

app = Flask(__name__)

def scrap_serp_image(fullname):
    query = f"site:linkedin.com {fullname} profile"
    result = []
    with DDGS(proxies= os.getenv('RESIDENTIAL_PROXY_URL'),timeout=30) as ddgs:
        try:
            keywords = query
            ddgs_images_gen = ddgs.images(
                keywords,
                region="se-sv",
                safesearch="off",
                size=None,
                color=None,
                type_image="photo",
                layout=None,
                license_image=None,
            )
            # count = 0
            for r in ddgs_images_gen:
                if 'linkedin.com/in' in r['url']:
                    result.append(r)

        except Exception as ex:
            print(str(ex))
    return result
st_time = time.monotonic()
print('start')
result = scrap_serp_image("Michael Bage")

with MongoDBConnector() as connector:
    connector.bulk_upsert_updated('serp_result_image',result,'url')
print('end')
print(f"Updated in [{time.monotonic() - st_time:.2f}]s")

@app.route('/')
def home():
    result = scrap_serp_image("Henry James")

    with MongoDBConnector() as connector:
        connector.bulk_upsert_updated('usernames',data,'username')
    return jsonify(result)

@app.route('/about')
def about():
    return 'About'