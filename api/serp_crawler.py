import os
import json
import csv
import random
import concurrent.futures
from duckduckgo_search import DDGS
from mongo import MongoDBConnector
from synccacher import Cacher
from filter import specialized_filter

# Get the directory of the current script
script_dir = os.path.dirname(__file__)
json_path = os.path.join(script_dir, 'query.json')

with open(json_path, 'r') as fp:
    query_schema = json.load(fp)


class SearchResult:
    def __init__(self):
        self.cacher = Cacher(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=os.getenv('REDIS_PORT', 6379),
            password=os.getenv('REDIS_PASSWORD', None)
        )

    @staticmethod
    def generate_name_special(index_log):
        script_dir = os.path.dirname(__file__)
        # Start from Sweden Nordic names
        name_csv_file = os.path.join(script_dir, 'resource/SE.csv')

        with open(name_csv_file, 'r', encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)

            for index, row in enumerate(list(csv_reader)[index_log:]):
                full_name = f"{row[0]} {row[1]}"
                yield full_name, index+index_log

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
    def search_query_platform(fullname: str, platform):
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
            with DDGS(proxies=proxies, timeout=30) as ddgs:
                for query in querys:
                    query = query.replace('$query', fullname)
                    generator_ddg = ddgs.text(query, region="se-sv")
                    for r in generator_ddg:
                        if specialized_filter(r['href'], platform):
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
                query = query.replace('$query', fullname)

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

    def run(self, platform):
        mongoconnector = MongoDBConnector()
        with self.cacher as cacher:
            indice_log = cacher.get([f'Sweden:{platform}']) or 0
            name_generator = self.generate_name_special(indice_log)

            for _ in range(100000):
                full_name, index = next(name_generator)
                cacher.insert([f'Sweden:{platform}'], index)
                combined_key = f"{full_name.lower()}:{platform}:v2"
                result = cacher.get([combined_key]) or {}
                if result:
                    print("ERROR : repeated")
                    continue

                else:
                    result = SearchResult().search_query_platform(full_name, platform)
                    with mongoconnector as connector:
                        connector.bulk_upsert_updated('scrapped_profiles_v2', result, 'url')
                    # value = [{
                    #     "fullname": full_name,
                    #     "country_code": "SE",
                    # }]
                    # with mongoconnector as connector:
                    #     connector.bulk_upsert_updated('nameset_v2',value, 'fullname')
                    cacher.insert(combined_key, True)


def run_worker(target):
    search_result = SearchResult()
    result = search_result.run(target)
    return result


def main():
    max_processes = 10  # Adjust this based on your needs
    # targets = ["facebook","linkedin", "twitter", "tiktok","instagram","pinterest","reddit","quora","badoo","snapchat"]
    targets = ["facebook","linkedin", "twitter", "tiktok","instagram"]
    # targets = ["linkedin"]
    with concurrent.futures.ProcessPoolExecutor(max_processes) as executor:
        # Submit each task to the process pool
        results = [executor.submit(run_worker, target) for target in targets]


if __name__ == "__main__":
    main()
