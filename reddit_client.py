import logging
import requests
from dotenv import load_dotenv
import os
import datetime

# Load environment variables
load_dotenv()

# Logger setup
logger = logging.getLogger("reddit client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class RateLimitException(Exception):
    pass

class RedditClient:
    API_BASE = "https://oauth.reddit.com"
    
    def __init__(self):
        self.client_id = os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = os.getenv('REDDIT_CLIENT_SECRET')
        self.user_agent = os.getenv('REDDIT_USER_AGENT')
        self.access_token = None

    def get_access_token(self):
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        data = {
            'grant_type': 'client_credentials',
            'username': os.getenv('REDDIT_USERNAME'),
            'password': os.getenv('REDDIT_PASSWORD')
        }
        headers = {'User-Agent': self.user_agent}
        response = requests.post('https://www.reddit.com/api/v1/access_token',
                                 auth=auth, data=data, headers=headers)
        response.raise_for_status()
        self.access_token = response.json()['access_token']

    def get_subreddit_new(self, subreddit):
        if not self.access_token:
            self.get_access_token()
        
        headers = {'Authorization': f'bearer {self.access_token}',
                   'User-Agent': self.user_agent}
        url = f"{self.API_BASE}/r/{subreddit}/new"
        return self.execute_request(url, headers)

    def get_post_comments(self, post_id):
        if not self.access_token:
            self.get_access_token()
        
        headers = {'Authorization': f'bearer {self.access_token}',
                   'User-Agent': self.user_agent}
        url = f"{self.API_BASE}/comments/{post_id}"
        return self.execute_request(url, headers)

    def execute_request(self, url, headers):
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 404:
                logger.warning(f"404 Not Found: {url}")
                return None
            elif resp.status_code == 429:
                logger.warning("Rate limit reached (429). Request will be rescheduled.")
                raise RateLimitException("Rate limit reached")
            resp.raise_for_status()
            logger.info(f"Status code: {resp.status_code}")
            json_data = resp.json()
            logger.debug(f"JSON response: {json_data}")
            return json_data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request: {e}")
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                raise RateLimitException("Rate limit reached")
            return None

if __name__ == "__main__":
    # This block is for testing the RedditClient
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
    if DEBUG:
        logger.setLevel(logging.DEBUG)

    client = RedditClient()
    
    # Test get_subreddit_new
    new_posts = client.get_subreddit_new("AskReddit")
    if new_posts:
        print("New posts retrieved successfully")
        print(f"Number of posts: {len(new_posts['data']['children'])}")
    else:
        print("Failed to retrieve new posts")

    # Test get_post_comments
    comments_data = client.get_post_comments("12345")  # Replace with a real post ID
    if comments_data:
        print("Comments data retrieved successfully")
        print(f"Number of top-level comments: {len(comments_data[1]['data']['children'])}")
    else:
        print("Failed to retrieve comments data")   