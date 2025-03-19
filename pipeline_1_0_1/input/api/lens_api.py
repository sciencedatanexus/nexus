# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.api.lens_api.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.1
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 12/12/2024
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
import pandas as pd
import json
from ..utils.utils_api import request_retry, APICallTracker

# =============================================================================
# Functions and classes
# =============================================================================

class GetLensData:
    # =============================================================================
    # Constructor
    # =============================================================================
    def __init__(self,
                 api_configuration,
                 query_string,
                 query_parameters=None, ## API query string
                 page_start=0,
                 page_size=100,  ## maximum value is 100 for patents
                 aggregation_string=None,
                 api_type='scholarly',
                 api_sort=[{"relevance":"desc"}, {"year_published": "desc"}],
                 api_include= ["lens_id"],
                 api_exclude=None,
                 api_scroll=None,
                 api_stemming=True,
                 api_regex=False,
                 api_min_score=0,
                 patent_group_by=False
                 ):
        self.__name = 'GetLensData'
        # =============================================================================
        # Global attributes
        # =============================================================================
        self.query_string= query_string  ## ex: {"bool": {"must":[{'match': {'author.last_name': 'Kondratyev'}}]}}
        self.query_parameters= query_parameters ## ex: [{"bool": {"must":[{"match": {"publication_type": "journal article"}}]}}]
        self.page_start= page_start
        self.page_size= page_size
        self.aggregation_string= aggregation_string
        self.api_type= api_type
        self.api_sort= api_sort
        self.api_exclude= api_exclude
        self.api_include= api_include
        self.api_scroll= api_scroll
        self.api_configuration= api_configuration
        self.api_stemming= api_stemming
        self.api_regex= api_regex
        self.api_min_score= api_min_score
        self.patent_group_by = patent_group_by
        '''
        gets API credentials from a config.yaml file that has the following content:
        APILENSS
            endpoint: https://api.lens.org/scholarly/
            apikey: [API token KEY HERE]
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

    @property
    def query_string(self):
        return self._query_string

    @query_string.setter
    def query_string(self, new_query_string):
        if isinstance(new_query_string, dict):
            self._query_string = new_query_string
        else:
            raise ValueError("Please enter a valid query parameters")

    @property
    def query_parameters(self):
        return self._query_parameters

    @query_parameters.setter
    def query_parameters(self, new_query_parameters):
        self._query_parameters = new_query_parameters

    @property
    def page_start(self):
        return self._page_start

    @page_start.setter
    def page_start(self, new_page_start):
        if isinstance(new_page_start, int) and new_page_start >= 0:
            self._page_start = new_page_start
        else:
            raise ValueError("Please enter a valid page start of 0 or above")
    
    @property
    def page_size(self):
        return self._page_size

    @page_size.setter
    def page_size(self, new_page_size):
        if 0 <= new_page_size <= 10000:
            self._page_size = new_page_size
        else:
            raise ValueError("Please enter a valid size between 0 and 100")
    
    @property
    def aggregation_string(self):
        return self._aggregation_string

    @aggregation_string.setter
    def aggregation_string(self, value):
        self._aggregation_string = value
    
    @property
    def api_type(self):
        return self._api_type

    @api_type.setter
    def api_type(self, value):
        self._api_type = value

    @property
    def api_sort(self):
        return self._api_sort

    @api_sort.setter
    def api_sort(self, value):
        self._api_sort = value

    @property
    def api_exclude(self):
        return self._api_exclude

    @api_exclude.setter
    def api_exclude(self, value):
        self._api_exclude = value

    @property
    def api_include(self):
        return self._api_include

    @api_include.setter
    def api_include(self, value):
        self._api_include = value

    @property
    def api_scroll(self):
        return self._api_scroll

    @api_scroll.setter
    def api_scroll(self, value):
        self._api_scroll = value

    @property
    def api_stemming(self):
        return self._api_stemming

    @api_stemming.setter
    def api_stemming(self, value):
        self._api_stemming = value

    @property
    def api_regex(self):
        return self._api_regex

    @api_regex.setter
    def api_regex(self, value):
        self._api_regex = value

    @property
    def api_min_score(self):
        return self._api_min_score

    @api_min_score.setter
    def api_min_score(self, value):
        self._api_min_score = value

    @property
    def patent_group_by(self):
        return self._patent_group_by

    @patent_group_by.setter
    def patent_group_by(self, value):
        self._patent_group_by = value


    # =============================================================================
    # Methods
    # =============================================================================
    def build_query_strategy(self, query_json, query_parameters):
        try:
            new_query_strategy = {
                                "bool": 
                                    {
                                    "must": 
                                        [
                                            query_json # include the topic search strategies
                                        ]
                                    }
                            } 
            if query_parameters:
                new_query_strategy['bool']['must'] = new_query_strategy['bool']['must'] +  query_parameters # add any common query parameters (eg document types)`
        finally:
            return new_query_strategy


    def get_lens_data(self, start_year, end_year, call_tracker=None):
        # Retrieve Lens data from a query.
        # Returns: 1 panda dataframe objects with data fields:
        #   df = list of lens_id
        # api_configuration: extract from the configfile details about the API (endpoint, APIkey)
        # query string: json parameters from query
        # aggregation_string: json parameters from aggragation details
        # api_type: the complete endpoint (scholarly, patent), by default search
        # Response is paged by increments of 10 per page
        """
        Sample query strings
        query = {
            "bool": {
                "must": [{"range": {"year_published": {"gte": "2019", "lte": "2023"}}},
                    {"bool": {"must":[
                        {"match": {"publication_type": "journal article"}},
                        {"term": {"author.affiliation.address.country_code": "AU"}},
                        {"term": {"author.affiliation.address.state_code": "AU-VIC"}},
                        {"match": {"field_of_study": "Internal medicine"}}
                    ]}}
                ]
            }
        }
        query_patents = {
            "bool": {
                "must": [{"range": {"year_published": {"gte": "2019", "lte": "2023"}}},
                    {"bool": {"must":[
                        {"term": {"priority_claim.jurisdiction": "AU"}},
                        {"bool": {"should":[
                            {"match": {"applicant.address": "victoria"}},
                            {"match": {"inventor.address": "victoria"}}
                        ]}}
                    ]}}
                        
                ]
            }
        }
        aggregation = {
            "scholarly_citations": [{"cardinality": {"field": "year_published"}},{"sum": {"field": "referenced_by_count"}} ] 
        }
        aggregation = {
            "level 1": {
                "date_histogram": {"field": "date_published", "interval": "YEAR",
                    "aggregations": {
                        "level 2": {
                            "terms": {"field": "author.affiliation.address.city", "size": 50, "order": {"field_value": "asc"}
                                # ,"aggregations": {
                                #     "level 3": {
                                #         "date_histogram": {"field": "date_published", "interval": "YEAR"}
                                #     }
                                # }
                            }
                        }, "scholarly_citations": {"avg": {"field": "referenced_by_count"}},
                        "max_scholarly_citations": {"max": {"field": "referenced_by_count"}},
                        "author_count": {"avg": {"field": "author_count"}}
                    }
                }
            }
        }
        """
        try:
            print('\t start Lens query')
            if call_tracker is None:
                call_tracker = APICallTracker()
            method = 'POST'
            token = 'Bearer {}'.format(self._api_configuration['apikey'])
            headers = {'Authorization': token, 'Content-Type': 'application/json'}
            df = pd.DataFrame()
            df_aggregation = pd.DataFrame()  
            nb_total = 0
            max_score = 0
            query_strategy = self.build_query_strategy(self._query_string, self._query_parameters)
            if self._aggregation_string:
                query_strategy['bool']['must'] = query_strategy['bool']['must'] + [{"range": {"year_published": {"gte": start_year, "lte": end_year}}}] # restrict by year
                url = '{}{}'.format(self._api_configuration['endpoint'], 'aggregate')
                json_params = {
                    "query": query_strategy,
                    "aggregations": self._aggregation_string,
                    "size": 0,
                    "stemming": self._api_stemming,
                    "regex": self._api_regex
                    # "min_score": self._api_min_score
                }
                json_query = json.dumps(json_params)  ## format the python dictionary into json (notably for parameters with null values)
                query_response = call_tracker.loop_call(json_query, headers, method, url, max_tries=10, n=5)
                # print(query_response.content)
                if query_response.status_code == 200:
                        r = query_response.json()
                        nb_total = r['total']
                        if nb_total > 0:
                            data = r['aggregations']
                            agg_key = list(data.keys())[0]
                            df_aggregation = pd.DataFrame.from_dict(data[agg_key], orient='index').reset_index()
                            df_aggregation.rename(columns={'index': agg_key}, inplace=True)
            else:
                for py in range(end_year, start_year -1,  -1):
                    query_strategy['bool']['must'] = query_strategy['bool']['must'] + [{"match": {"year_published": py}}] # restrict by year
                    url = '{}{}'.format(self._api_configuration['endpoint'], 'search')
                    # self._page_size = self._page_size
                    json_params = {
                        "query": query_strategy,
                        "size": self._page_size,
                        "sort": self._api_sort,
                        "exclude": self._api_exclude,
                        "scroll": "1m",
                        "stemming": self._api_stemming,
                        "regex": self._api_regex,
                        "min_score": self._api_min_score
                    }
                    if self._api_include:
                        json_params["include"] = self._api_include,
                    json_query = json.dumps(json_params)  ## format the python dictionary into json (notably for parameters with null values)
                    # method = 'POST'
                    # token = 'Bearer {}'.format(self._api_configuration['apikey'])
                    # headers = {'Authorization': token, 'Content-Type': 'application/json'}
                    query_response = request_retry(json_query, headers, method, url, max_tries=10, n=10)
                    if query_response.status_code == 200:
                        r = query_response.json()
                        nb_total = r['total']
                        if nb_total > 0:
                            nb_results = r['results']
                            max_score = r['max_score']
                            data = r['data']
                            df_py = pd.json_normalize(data, errors='ignore')
                            df = pd.concat([df, df_py])
                            print("\t\t", nb_total, "records in", py)
                            if nb_total > nb_results:
                                if r["scroll_id"]:
                                    json_params = {"scroll": "1m", "scroll_id": r['scroll_id'], "include": self._api_include }
                                    for i in range(nb_results, nb_total, self._page_size):
                                        json_query = json.dumps(json_params)
                                        query_response = call_tracker.loop_call(json_query, headers, method, url, max_tries=10, n=10)
                                        # query_response = request_retry(json_query, headers, method, url, max_tries=10)
                                        if query_response.status_code == 200:
                                            r_id = query_response.json()
                                            print("\t\t\t", i, "records retrieved for ", py)
                                            if r_id["scroll_id"]:
                                                json_params["scroll_id"] = r_id['scroll_id']
                                                data_id = r_id['data']
                                                max_score = r['max_score']
                                                d_id = pd.json_normalize(data_id, errors='ignore')
                                                d_id['score'] = max_score
                                                df = pd.concat([df, d_id])
                                                i += self._page_size
        finally:
            nb_total = df.shape[0]
            print('\t Last Lens data retrieved')
            # print(df.shape[0])
            return df, df_aggregation, nb_total, max_score, call_tracker
    # =============================================================================
    # Pipeline steps
    # ============================================================================
    
    



# =============================================================================
# End of script
# =============================================================================

