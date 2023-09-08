import os
import json
import random
from duckduckgo_search import DDGS
from mongo import MongoDBConnector
from cacher import Cacher
class SearchResult:
    def __init__(self):
        self.cacher = Cacher(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=os.getenv('REDIS_PORT', 6379),
                password=os.getenv('REDIS_PASSWORD', None)
            )
    
    
    @staticmethod
    def generate_name():
        script_dir = os.path.dirname(__file__)  # Get the directory of the current script
        first_name_path = os.path.join(script_dir, 'resource/firstname.json')
        with open(first_name_path,'r',encoding="utf-8") as fp:
            first_names = json.load(fp)

        last_name_path = os.path.join(script_dir, 'resource/lastname.json')
        with open(last_name_path,'r',encoding="utf-8") as fp:
            last_names = json.load(fp)

        while True:
            # Randomly select a first name and last name
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)

            # Combine the first name and last name to form a full name
            full_name = f"{first_name} {last_name}"

            yield full_name


    @staticmethod
    def search_image(fullname):
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
                count = 0
                for r in ddgs_images_gen:
                    if 'linkedin.com/in' in r['url']:
                        r["platform"] = "linkedin"
                        result.append(r)

            except Exception as ex:
                print(str(ex))
        return result


    async def run(self):
        name_generator = SearchResult.generate_name()
        for _ in range(10000):
            random_name = next(name_generator)
            print("_________________________________________")
            combined_key = f"{random_name.lower()}:{'linkedin'}"
            
            async with self.cacher as cacher:
                result = await cacher.get([combined_key]) or {}

                if result:
                    print("ERROR : repeated")
                    continue

                else : 
                    result = SearchResult.search_image(random_name)
                    with MongoDBConnector() as connector:
                        connector.bulk_upsert_updated('serp_result_image',result,'url')
                    await cacher.insert(combined_key, True)
        return True

# Example usage
async def main():
    search_result = SearchResult()
    result = await search_result.run()
    return result


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())