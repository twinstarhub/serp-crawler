import os
from flask import Flask,jsonify
from duckduckgo_search import DDGS

from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
from pymongo.errors import BulkWriteError, ConnectionFailure

app = Flask(__name__)

def cron_job(fullname):
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
                    # count += 1
                    # if count > 10:
                    #     break

        except Exception as ex:
            print(str(ex))
    return result


def connect_mongo():
    """Connect to MongoDB."""
    uri = f"mongodb+srv://{os.getenv('MONGO_USERNAME', None)}:{os.getenv('MONGO_PASSWORD', None)}@{os.getenv('MONGO_HOST', 'localhost')}/?retryWrites=true&w=majority"
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        print('Connect to MongoDB.')

    except Exception:
        print('Unable to connect to MongoDB.')


@app.route('/')
def home():
    result = cron_job("Michael")
    return jsonify(result)

@app.route('/about')
def about():
    return 'About'