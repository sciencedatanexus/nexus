# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.ror_api.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus
# Science Data Nexus, 2022
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 28/11/2024
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
import requests
import time
from collections import deque


# =============================================================================
# Functions and classes
# =============================================================================

def request_query_post(method, url, query, f_headers):
    try:
        resp = requests.request(
            method,
            url,
            data=query,
            headers=f_headers)  # This is the initial API request
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)
    finally:
        return resp


def request_query(query, f_headers=None):
    try:
        if f_headers:
            resp = requests.get(
            query,
            headers=f_headers)  # This is the initial API request
        else:
            resp = requests.get(query)
    except requests.exceptions.RequestException as err:
        raise SystemExit(err)
    finally:
        return resp

def retry(query, f_headers=None, max_tries=10, n=5):
    """
    Retry a query function with a rate limit of up to `n` calls per minute.
    
    Args:
        query: The query to be executed.
        f_headers: Optional headers for the query.
        max_tries: Maximum number of retry attempts.
        n: Maximum number of calls allowed per minute (default is 5).
    
    Returns:
        The response of the query.
    """
    resp = None  # Initialize response variable
    delay = 60 / n  # Calculate delay between requests in seconds
    delay = 60  # Calculate delay between requests in seconds

    for i in range(max_tries):
        try:
            if i > n:
                time.sleep(delay)  # Wait for the calculated delay after the first attempt
            resp = request_query(query, f_headers)
            break  # Break out of the loop if the request is successful
        except requests.exceptions.RequestException as e:
            if i == max_tries - 1:  # If max attempts reached, re-raise the exception
                raise e
            continue  # Retry on exception

    return resp

def request_retry(f_query, f_headers=None, f_method='GET', f_url=None, max_tries=10, sleep_sec=0.3):
    """
    Retry a query function with a rate limit of up to `n` calls per minute.

    Args:
        f_query: The query to be executed.
        f_headers: Optional headers for the query.
        f_method: HTTP method ('GET' or 'POST').
        f_url: URL for the request (used for POST requests).
        max_tries: Maximum number of retry attempts.
        sleep_sec: the sleep in ms.      

    Returns:
        The response of the query.
    """    
    def api_request():
        if f_method == 'POST':
            api_resp = request_query_post(f_method, f_url, f_query, f_headers)
        else:
            api_resp = request_query(f_query, f_headers)
        return api_resp
    for i in range(max_tries):
        try:
            time.sleep(sleep_sec)
            resp = api_request()
        except requests.exceptions.RequestException:
            continue
        finally:
            return resp

class APICallTracker:
    def __init__(self):
        self.last_call = None  # Stores the timestamp of the last API call
        self.call_timestamps = deque()  # Stores the timestamps of recent API calls

    def track_api_call(self):
        """
        Tracks the timestamp of the current API call.
        """
        current_time = time.time()
        self.last_call = current_time
        self.call_timestamps.append(current_time)

    def get_last_call(self):
        """
        Returns the details of the last API call.

        :return: dict, details of the last API call or None if no calls were made
        """
        if self.last_call:
            current_time = time.time()
            # Remove timestamps older than 1 minute
            while self.call_timestamps and current_time - self.call_timestamps[0] > 60:
                self.call_timestamps.popleft()

            return {
                "last_call_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_call)),
                "recent_calls": list(self.call_timestamps)
            }
        return None

    def loop_call(self, f_query, f_headers=None, f_method='GET', f_url=None, max_tries=10, n=5):
        """
        Ensures rate-limited API calls and tracks the responses.

        :param f_query: The API query to be executed.
        :param n: Maximum allowed calls per minute.
        :return: The response of the API call.
        """
        current_time = time.time()
        if len(self.call_timestamps) >= n:
            sleep_time = 60 - (current_time - self.call_timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
            # while self.call_timestamps and current_time - self.call_timestamps[0] > 60:
            #     self.call_timestamps.popleft()
                self.call_timestamps = deque()

        # Make the request
        resp = request_retry(f_query, f_headers=f_headers, f_method=f_method, f_url=f_url, max_tries=max_tries)

        # Log the timestamp of the successful call
        self.track_api_call()
        return resp


# =============================================================================
# End of script
# =============================================================================
