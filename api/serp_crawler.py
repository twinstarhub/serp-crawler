import os
import json
import random
import concurrent.futures
from duckduckgo_search import DDGS
from mongo import MongoDBConnector
from synccacher import Cacher
from filter import specialized_filter

script_dir = os.path.dirname(__file__)  # Get the directory of the current script
json_path = os.path.join(script_dir, 'query.json')

with open(json_path,'r') as fp:
    query_schema = json.load(fp)


class SearchResult:
    def __init__(self):
        self.cacher = Cacher(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=os.getenv('REDIS_PORT', 6379),
            password=os.getenv('REDIS_PASSWORD', None)
        )

    @staticmethod
    def generate_name():
        # Get the directory of the current script
        script_dir = os.path.dirname(__file__)
        first_name_path = os.path.join(script_dir, 'resource/firstname.json')
        with open(first_name_path, 'r', encoding="utf-8") as fp:
            first_names = json.load(fp)

        last_name_path = os.path.join(script_dir, 'resource/lastname.json')
        with open(last_name_path, 'r', encoding="utf-8") as fp:
            last_names = json.load(fp)

        while True:
            # Randomly select a first name and last name
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)

            # Combine the first name and last name to form a full name
            full_name = f"{first_name} {last_name}"

            yield full_name
    
    @staticmethod
    def search_query_platform(fullname:str,platform):
        """
        One query per one platform search 

        params query:string, platform: target platform name
        return result:list<dict> search result list 
        """
        result = []
        querys = query_schema[platform]
        proxies = {
            "http://": os.getenv('ZENROWS_PROXY_URL'),
            "https://": os.getenv('ZENROWS_PROXY_URL'),
        }
        
        try:
            with DDGS(proxies = proxies, timeout=30) as ddgs:
                for query in querys:
                    query = query.replace('$query',fullname)
                    generator_ddg = ddgs.text(query,region="se-sv")
                    for r in generator_ddg:
                        if specialized_filter(r['href'],platform):
                            r['url'] = r['href']
                            r["platform"] = platform
                            del r['href']
                            result.append(r)

        except Exception as ex:
            print(f"**Error in search result # **: {str(ex)}")
        
        return result


    @staticmethod
    def search_image(fullname, platform):
        # query = f"site:{platform}.com {fullname} profile"
        querys = query_schema[platform]
        result = []
        # with DDGS(proxies= os.getenv('RESIDENTIAL_PROXY_URL'),timeout=30) as ddgs:
        proxies = {
            "http://": os.getenv('ZENROWS_PROXY_URL'),
            "https://": os.getenv('ZENROWS_PROXY_URL'),
        }
        with DDGS(proxies=proxies, timeout=30) as ddgs:
            for query in querys:
                query = query.replace('$query',fullname)

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
                    for r in ddgs_images_gen:
                        if specialized_filter(r['url'], platform):
                            result.append(r)
                            r["platform"] = platform

                except Exception as ex:
                    print(str(ex))

        return result
    
    def run(self,platform):
        name_generator = self.generate_name()
        mongoconnector =  MongoDBConnector()
        with self.cacher as cacher:
            for _ in range(100000):
                random_name = next(name_generator)
                combined_key = f"{random_name.lower()}:{platform}:text"
                result = cacher.get([combined_key]) or {}
                if result:
                    print("ERROR : repeated")
                    continue

                else:
                    result = SearchResult().search_query_platform(random_name,platform)
                    with mongoconnector as connector:
                        connector.bulk_upsert_updated('serp_result_image', result, 'url')
                    cacher.insert(combined_key, True)

def run_worker(target):
    search_result = SearchResult()
    result = search_result.run(target)
    return result


def main():
    max_processes = 10  # Adjust this based on your needs
    targets = ["facebook","linkedin", "twitter", "tiktok","instagram","pinterest"]
    with concurrent.futures.ProcessPoolExecutor(max_processes) as executor:
        # Submit each task to the process pool
        results = [executor.submit(run_worker,target) for target in targets]


if __name__ == "__main__":
    main()