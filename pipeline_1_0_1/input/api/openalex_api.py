# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.openalex_api.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus
# Science Data Nexus, 2022
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 23/03/2022
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
import pandas as pd
import json
from pyalex import config, Topics, Concepts, Subfield, Field, Domain
import requests
import gzip
import io
import json


# =============================================================================
# Functions and classes
# =============================================================================


class GetOpenAlexData:
    # =============================================================================
    # Constructor
    # =============================================================================
    def __init__(self,
                 api_configuration,
                 ):
        self.__name = 'GetOpenAlexData'
        # =============================================================================
        # Global attributes
        # =============================================================================
        self.api_configuration= api_configuration
        '''
        gets API credentials from a config.ini file that has the following content:
        [APILENSS]
        endpoint = https://api.openalex.org/
        apikey = [contact@myemail.org]
        '''
        # =============================================================================
        # Methods attributes (variable with project)
        # =============================================================================

    # =============================================================================
    # Properties
    # =============================================================================
    @property
    def api_configuration(self):
        return self._api_configuration

    @api_configuration.setter
    def api_configuration(self, new_api_configuration):
        if isinstance(new_api_configuration, dict):
            self._api_configuration = new_api_configuration
        else:
            raise ValueError("Please enter a valid configuration")
    # =============================================================================
    # Methods
    # =============================================================================
    def set_openalex_api(self):
        # Setup Open Alex API.
        try:
            print('\t start OA setup')
            config.email = self._api_configuration['apikey']
            config.max_retries = 10
            config.retry_backoff_factor = 0.1
            config.retry_http_codes = [429, 500, 503]
        finally:
            print('\t OA setup completed')
    
    def get_openalex_topics(self):
        # Retrieve OpenAlex topics data.
        # Response is paged by increments of 100 per page

        def get_tree_data(drow, column):
            idx = drow[column]
            data = {'wikidata': None, 'wikipedia': None}
            url = idx.replace('https://openalex.org/', 'https://api.openalex.org/')
            print(url)
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    data = data['ids']
            finally:
                # print(data['wikidata'], data['wikipedia'])
                return data['wikidata'], data['wikipedia']

        try:
            print('\t start OA topics query')
            per_page = 100
            topics = []
            nb_total = Topics().count()  ## retrieve topics
            tot_page = int(nb_total/per_page)
            # tot_page = 2
            for i in range(0, tot_page):
                x = Topics().get(return_meta=False, per_page=per_page, page= i+1)
                topics = topics + x
            topics = pd.json_normalize(topics, errors='ignore')
            print('\t ', nb_total, "topics extracted")
            for i in ['domain.id', 'field.id', 'subfield.id']:
                x = topics.groupby(by=[i]).size().reset_index()
                x[i[:-2]+'wikidata'] = None
                x[i[:-2]+'wikipedia'] = None
                x[[i[:-2]+'wikidata',i[:-2]+'wikipedia']]= x.apply(get_tree_data, axis=1, result_type='expand', args=(i,))
                topics = topics.merge(x[[i, i[:-2]+'wikidata', i[:-2]+'wikipedia']], on=[i], how='left')
        finally:
            print('\t OA topics retrieved')
            return topics


    def get_openalex_concepts(self):
        # Retrieve OpenAlex concepts data.
        # Response is the download of saved concepts (65k concepts is too large for paged response from API)
        try:
            print('\t start OA concepts and topics query')
            concepts = []
            nb_total = Concepts().count()  ## retrieve concepts (deprecated 2024)
            url = 'https://openalex.s3.amazonaws.com/data/concepts/manifest'
            response = requests.get(url)
            data = response.json()
            data = [x['url'] for x in data['entries']]
            for i in data:
                url = i.replace('s3://openalex/', 'https://openalex.s3.amazonaws.com/')
                print('\t ', url)
                response = requests.get(url)
                # Use io.BytesIO to handle the in-memory bytes buffer
                with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                    # Read the decompressed content as text
                    file_content = f.read().decode('utf-8')
                    file_content = file_content.strip()
                    json_objects = file_content.splitlines()
                    json_obj = [json.loads(x) for x in json_objects]
                concepts = concepts + json_obj
            concepts = pd.json_normalize(concepts, errors='ignore')
            print('\t ', nb_total, "concepts extracted")
        finally:
            print('\t OA concepts retrieved')
            return concepts

    # =============================================================================
    # Pipeline steps
    # ============================================================================
    
  


def get_openalex_object(search, entity='author'):
    # Retrieve
    # Returns
    # https://docs.openalex.org/api/get-single-entities
    # url: https://api.openalex.org/works/W2741809807
    try:
        url = 'https://api.openalex.org/'
        query = "{}{}/{}".format(url, entity, search)
        headers = ''
        query_response = retry(query, headers)
        data = None
        if query_response.status_code == 200:
            resp = query_response.json()
            data = resp
    finally:
        return data


def search_openalex(search, entity='author', page=1, perpage=100, cursor=None):
    # Retrieve
    # Returns
    # https://docs.openalex.org/api/get-single-entities
    # url: https://api.openalex.org/works/W2741809807
    try:
        url = 'https://api.openalex.org/'
        if cursor:
            query = "{}{}?filter={}&page={}&per-page={}&cursor={}".format(url, entity, search, page, perpage, cursor)
        else:
            query = "{}{}?filter={}&page={}&per-page={}".format(url, entity, search, page, perpage)
        headers = None
        # print(query)
        query_response = retry(query, headers)
        data = None
        if query_response.status_code == 200:
            resp = query_response.json()
            data = resp
    finally:
        return data

# =============================================================================
# End of script
# =============================================================================
