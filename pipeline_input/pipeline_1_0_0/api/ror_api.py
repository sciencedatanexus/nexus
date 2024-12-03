# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.ror_api.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 11/11/2024
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
from ..utils.utils_api import request_retry
import requests
import zipfile
import io
import json

# =============================================================================
# Functions and classes
# =============================================================================


class RORapi:
    _api_config_zenodo = None  # Class-level variable for api_config_zenodo

    def __init__(self, api_config_zenodo):
        self.api_config_zenodo = api_config_zenodo  # Set the initial configuration

    @property
    def api_config_zenodo(self):
        """Getter for api_config_zenodo."""
        return self._api_config_zenodo

    @api_config_zenodo.setter
    def api_config_zenodo(self, value):
        """Setter for api_config_zenodo."""
        self._api_config_zenodo = value

    def get_ror_dump(self):
        """Retrieve the latest ROR data dump as a JSON object."""
        try:
            print('\t start ROR data dump retrieval')
            url = self._api_config_zenodo['endpoint']
            query_string = "communities/ror-data/records?q=&sort=newest"
            query = "{}{}".format(url, query_string)
            headers = ''
            query_response = request_retry(query, headers)  # request_retry is defined in utils_api/request_retry
            
            if query_response.status_code == 200:
                resp = query_response.json()
                dump_file = resp['hits']['hits'][0]['files'][0]['links']['self']
                dump_string = resp['hits']['hits'][0]['files'][0]['key']
                dump_json = dump_string.replace(".zip", "_schema_v2.json")
                response = requests.get(dump_file, timeout=30)
                
                if response.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                        with zip_ref.open(dump_json) as file:
                            json_obj = json.load(file)
                            
            print('\t Last ROR data dump retrieved')
            return json_obj

        except Exception as e:
            print(f"Error retrieving ROR data dump: {e}")
            return None

    
# =============================================================================
# End of script
# =============================================================================
