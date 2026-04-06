
'''
Name: api_ntfy.py
Description: 
Object-based API client responsible for interacting with the Ntfy Notification API.
The NTFY Service is used to send real-time notifications to my mobile (when dealing with long-running processes)
Another "Logging" module is also used for more detailed logs that I can also review later.
'''

import requests
import urllib3
import config as cfg # Configuration Details for Ntfy API

class api_ntfy:
    def __init__(self): 
        # Disable IPv6 to avoid issues with long API responses due to proxies
        urllib3.util.connection.HAS_IPV6 = False

        self.base_url = cfg.NTFY_TOKENS['ntfy_url']
        self.topic = cfg.NTFY_TOKENS['ntfy_topic']
    
    def send_notification(self, title: str, message: str):
        url = f"{self.base_url}/{self.topic}"  # Endpoint for sending notifications to a specific topic in Ntfy
        payload = {
            "title": title,
            "message": message
        }
        r = requests.post(url, json=payload)
        r.raise_for_status()
        return r.status_code    