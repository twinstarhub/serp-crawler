import os
import json
import random
import threading
import asyncio
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
            
        self.stop_signal = asyncio.Event()  # Create an event to signal when to stop
    
    
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

        mongo_connector = MongoDBConnector()
    
        async with self.cacher as cacher:
            for _ in range(10000):
                if self.stop_signal.is_set():
                    break  # Exit the loop if the stop signal is set
                random_name = next(name_generator)
                print("_________________________________________")
                combined_key = f"{random_name.lower()}:{'linkedin'}"
                
                result = await cacher.get([combined_key]) or {}

                if result:
                    print("ERROR : repeated")
                    continue

                else : 
                    result = SearchResult.search_image(random_name)
                    with mongo_connector as connector:
                        connector.bulk_upsert_updated('serp_result_image', result, 'url')
                    await cacher.insert(combined_key, True)
        return True
    def stop(self):
        self.stop_signal.set()  # Set the stop signal to indicate that the loop should stop

# Example usage
async def main():
    search_result = SearchResult()
    loop = asyncio.get_event_loop()

    # Start the loop in the background
    loop.create_task(search_result.run())

    # Simulate a signal to stop the loop after a certain delay
    await asyncio.sleep(10000)  # Replace with your actual signal handling logic
    search_result.stop()  # Set the stop signal to stop the loop



if __name__ == "__main__":
    import os

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())