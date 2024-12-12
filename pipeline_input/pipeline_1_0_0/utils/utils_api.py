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
from time import sleep


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

def retry(query, f_headers=None, max_tries=10):
    for i in range(max_tries):
        try:
            sleep(0.3)
            resp = request_query(query, f_headers)
            break
        except requests.exceptions.RequestException:
            continue
        finally:
            return resp


def request_retry(f_query, f_headers=None, f_method='GET', f_url=None, max_tries=10, sleep_sec=0.3):
    def api_request():
        if f_method == 'POST':
            api_resp = request_query_post(f_method, f_url, f_query, f_headers)
        else:
            api_resp = request_query(f_query, f_headers)
        return api_resp
    for i in range(max_tries):
        try:
            sleep(sleep_sec)
            resp = api_request()
            # if f_method == 'POST':
            #     resp = request_query_post(f_method, f_url, f_query, f_headers)
            # else:
            #     resp = request_query(f_query, f_headers)
            # break
        except requests.exceptions.RequestException:
            continue
        finally:
            return resp


# =============================================================================
# End of script
# =============================================================================
