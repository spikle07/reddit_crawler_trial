import logging
from pyfaktory import Client, Job, Producer
import sys
from dotenv import load_dotenv
import os
import json

# Load environment variables
load_dotenv()

# Load configuration
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

CONFIG = load_config()

logger = logging.getLogger("reddit crawler starter")
logger.propagate = False
logger.setLevel(getattr(logging, CONFIG['logging']['level']))
sh = logging.StreamHandler()
formatter = logging.Formatter(CONFIG['logging']['format'])
sh.setFormatter(formatter)
logger.addHandler(sh)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python start_reddit_crawl.py <subreddit>")
        sys.exit(1)

    subreddit = sys.argv[1]
    subreddit_names = [s['name'] for s in CONFIG['subreddits']]
    
    if subreddit not in subreddit_names:
        print(f"Error: {subreddit} is not in the configured subreddits list.")
        print(f"Available subreddits: {', '.join(subreddit_names)}")
        sys.exit(1)

    print(f"Starting crawl for subreddit: {subreddit}")
    
    faktory_server_url = os.getenv('FAKTORY_SERVER_URL', "tcp://:password@localhost:7419")

    with Client(faktory_url=faktory_server_url, role="producer") as client:
        producer = Producer(client=client)
        job = Job(jobtype="crawl-subreddit", args=(subreddit, []), queue="crawl-subreddit")
        producer.push(job)
    
    print(f"Crawl job for {subreddit} has been queued.")