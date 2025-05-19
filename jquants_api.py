

import os
import requests
import json
import logging
import time
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

class JQuantsAPI:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.id_token = None
        self.refresh_token = None

    def get_refresh_token(self, retries=3):
        logging.info("Getting refresh token...")
        data = {"mailaddress": self.email, "password": self.password}
        for attempt in range(retries):
            try:
                response = requests.post("https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))
                logging.info(f"API Response Status Code: {response.status_code}")
                response.raise_for_status()
                response_data = response.json()
                
                self.refresh_token = response_data.get("refreshToken")
                return self.refresh_token
            except (HTTPError, ConnectionError, Timeout, RequestException) as e:
                logging.error(f"Failed to get refresh token (attempt {attempt + 1}/{retries}): {e}")
                if response.content:
                    logging.error(f"API Error Message: {response.content.decode('utf-8')}")
                if attempt < retries - 1:
                    time.sleep(5)  # wait before retrying
                else:
                    raise
        return self.refresh_token

    def get_id_token(self, retries=3):
        logging.info("Getting ID token...")
        for attempt in range(retries):
            try:
                response = requests.post(f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={self.refresh_token}")
                logging.info(f"API Response Status Code: {response.status_code}")
                response.raise_for_status()
                response_data = response.json()
                
                self.id_token = response_data.get("idToken")
                return self.id_token
            except (HTTPError, ConnectionError, Timeout, RequestException) as e:
                logging.error(f"Failed to get ID token (attempt {attempt + 1}/{retries}): {e}")
                if response.content:
                    logging.error(f"API Error Message: {response.content.decode('utf-8')}")
                if attempt < retries - 1:
                    time.sleep(5)  # wait before retrying
                else:
                    raise
        return self.id_token

    def fetch_company_info(self):
        logging.info("Fetching company info...")
        headers = {'Authorization': f'Bearer {self.id_token}'}
        url = "https://api.jquants.com/v1/listed/info"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json().get("info", [])
        except (HTTPError, ConnectionError, Timeout, RequestException) as e:
            logging.error(f"Failed to fetch company info: {e}")
            raise

    def fetch_data(self, code=None, date=None):
        if not code and not date:
            raise ValueError("Either code or date must be provided.")

        headers = {'Authorization': f'Bearer {self.id_token}'}
        base_url = "https://api.jquants.com/v1/fins/statements?"
        url = base_url + (f"code={code}" if code else "") + (f"&date={date}" if date else "")

        logging.info(f"Fetching data from {url}...")
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json().get("statements", [])

            # Handle pagination
            while "pagination_key" in response.json():
                pagination_key = response.json()["pagination_key"]
                paginated_url = f"{base_url}pagination_key={pagination_key}" + (f"&code={code}" if code else "") + (f"&date={date}" if date else "")
                logging.info(f"Fetching more data with pagination_key: {pagination_key}...")
                response = requests.get(paginated_url, headers=headers)
                response.raise_for_status()
                data += response.json().get("statements", [])
        except (HTTPError, ConnectionError, Timeout, RequestException) as e:
            logging.error(f"Failed to fetch data: {e}")
            raise

        return {"statements": data}
