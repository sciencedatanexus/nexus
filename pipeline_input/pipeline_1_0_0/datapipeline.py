# coding=utf-8

# =============================================================================
# """
# .. module:: pipeline_input.inputpipeline.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0.0
# .. description:: data pipeline for Consulting project.
# :Copyright: Jean-Francois Desvignes for Science Data Nexus
# :Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 26/10/2024
# """
# =============================================================================
"""
Refer to the README.txt for details about HOW TO run the input pipeline.
"""
# =============================================================================
# modules to import
# =============================================================================

# Python modules
import os
import sys

import pandas as pd
import datetime
import yaml ## pyyaml module
# Science Data Nexus Pipeline modules - list of modules
from .api.lens_api import *
from .api.openalex_api import *
from .api.ror_api import *
from .api.search_strategy import load_search_strategy
from .core.ddb_data import *
from .core.ddb_baselines import *
"""
Add modules when needed when custom pipelines are run such as:
import matplotlib.pyplot as plt
from matplotlib_venn import venn3, venn3_circles
import zipfile
import boto3  ## AWS SDK
from bs4 import BeautifulSoup
import numpy as np
"""


# =============================================================================
# Functions and classes
# =============================================================================


class DataPipeLine:
    # =============================================================================
    # Constructor
    # =============================================================================
    def __init__(self,
                 project_name,
                 ror_version,
                 project_start_year=0,
                 project_end_year=0,
                 root_dir=None,
                 data_dir=None,
                 project_dir_name=None,
                 configfile=None,
                 baseline_version='baselines',
                 project_variant=None):
        self.__name = 'DataPipeLine'
        # =============================================================================
        # Global attributes
        # =============================================================================
        self.project_name = project_name
        self.project_dir_name = project_dir_name
        ## the directories and paths
        self.root_dir = root_dir
        self.data_dir = data_dir
        self.wdir = root_dir  # for instance "~/code/nexus/project_dir_name"
        self.outdir = data_dir  # for instance "~/data/project_name"
        self.lib_dir = sys.modules["nexus.pipeline_input.pipeline"].__path__[0]  ## version of the module"
        self.tempdir = data_dir
        self.configfile = configfile  # yaml configuration file, for instance: "~/config.yaml"
        '''
        gets API credentials from a config.yaml file that has the following content:
        APISDN:
            endpoint: https://api.ScienceDataNexus.com/
            apikey: "[API KEY HERE]"
        '''
        self.project_variant = project_variant  # used to create names of variants in the ame project
        ## Database
        self.db_schema = project_name
        self.baseline_version = baseline_version  # table in the DB, by deafult 'metrics.baselines'
        self.ror_version = ror_version  # tables in the DB (eg. data.ror_2024-11_X)
        ## Other variables
        self.project_start_year = project_start_year  # eg. py=2007, includes this year
        self.project_end_year = project_end_year  # eg. py = 2024, includes this year
        self.uid = "lens_id" # unique identyifier for the records, for instance 'lens_id"
        # =============================================================================
        # Methods attributes (variable with project)
        # =============================================================================
        ## LENS API
        self.lens_query_boundaries = " DT=(Article OR Review OR Proceedings Paper) "  ## by default " DT=(Article OR Review OR Proceedings Paper) "
        ## variables for network graph creation
        self.network_sample_size = None # size of the sampling to create a network map
        self.network_metrics = ['cnci', 'percentile', 'is_top10', 'is_top01']  ## default paper lavel metrics to include
        self.network_metadata = ["category", "country", 'country_label', "state",
                                 "organisation"]  ## collaboration metadata to include
        self.network_cluster = 'country'  ## network level to create: 'country', 'precinct', 'organisation'
        self.network_categories = None  ## set value to None for 1 grpah per category otherwise set value to ['combined'] for a single graph across all categories.
        self.network_version = ""  ## text string to define a specific version of the graph, by default empty
        self.network_type = 'organisation'
        """
            ## 'country' graph of countries
            ## 'organisation' graph of organisation
            ## 'precinct' graph of precincts as clusters
        """
        self.network_interval = [2013, 2018]  ## interval to use, None if whole dataset
        self.network_nodes = (100,)  ## top n countries/organisations/clusters to include
        self.network_mode = 1  ## graph mode (1 or 2)
        ## S3 export
        self._s3_bucket = "projects"
        # =============================================================================
        # Methods attributes (calculated)
        # =============================================================================
        ## Directories
        self.searchdir = os.path.join(self._wdir, "search_strategy")
        ## APIs
        with open(self._configfile, 'r') as f:
            config_content = yaml.safe_load(f)
        self.api_config_lenss = config_content['APILENSS']
        self.api_config_lensp = config_content['APILENSP']
        self.api_config_oa = config_content['APIOA']
        self.api_config_zenodo = config_content['APIZENODO']
        ## LENS API
        """
        Read boolean search string in each file
        """
        self.years = [i for i in range(self._project_start_year, self._project_end_year + 1)]
        self.last_year = self._project_end_year  ## in the case of specific date, to be changed
        self.searches = []
        self.names = []
    # =============================================================================
    # Properties
    # =============================================================================
    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, new_project_name):
        if isinstance(new_project_name, str):
            self._project_name = new_project_name
        else:
            raise ValueError("Please enter a New project name")

    @property
    def project_variant(self):
        return self._project_variant

    @project_variant.setter
    def project_variant(self, new_project_variant):
        self._project_variant = new_project_variant

    @property
    def project_dir_name(self):
        return self._project_dir

    @project_dir_name.setter
    def project_dir_name(self, new_project_dir_name):
        if isinstance(new_project_dir_name, str):
            self._project_dir_name = new_project_dir_name
        else:
            self._project_dir_name = self.project_name

    @property
    def root_dir(self):
        return self._root_dir

    @root_dir.setter
    def root_dir(self, new_root_dir):
        if isinstance(new_root_dir, str):
            self._root_dir = new_root_dir
            # for instance: "D:\MyProjects\\project_dir_name\\", can be different to allow more than one pipeline in a single root_dir
        else:
            raise ValueError("Please enter a New project directory")

    @property
    def data_dir(self):
        return self._data_dir

    @data_dir.setter
    def data_dir(self, new_data_dir):
        if isinstance(new_data_dir, str):
            self._data_dir = new_data_dir
            # for instance: "D:\MyData", can be different to allow more than one pipeline in a single data_dir
        else:
            raise ValueError("Please enter a New data directory")

    @property
    def wdir(self):
        return self._wdir

    @wdir.setter
    def wdir(self, new_wdir):
        if isinstance(self._root_dir, str):
            self._wdir = os.path.join(self._root_dir, self._project_dir_name, "input")
            # for instance: "D:\MyProjects\\project_dir_name\\", can be different to allow more than one pipeline in a single WDIR
        else:
            raise ValueError("Please enter a New project directory")

    @property
    def outdir(self):
        return self._outdir

    @outdir.setter
    def outdir(self, new_data_dir):
        if isinstance(new_data_dir, str):
            self._outdir = os.path.join(new_data_dir, self._project_name)
            # for instance: "D:\MyData\\project_dir_name\\", can be different to allow more than one pipeline in a single WDIR
        else:
            raise ValueError("Please enter a New data directory")

    @property
    def tempdir(self):
        return self._tempdir

    @tempdir.setter
    def tempdir(self, new_data_dir):
        if isinstance(new_data_dir, str):
            self._tempdir = os.path.join(new_data_dir, self._project_name, "temp_files")
            # for instance: "D:\MyData\\project_dir_name\\temp_files\\"
        else:
            raise ValueError("Please enter a New data directory")

    @property
    def lib_dir(self):
        return self._lib_dir

    @lib_dir.setter
    def lib_dir(self, new_lib_dir):
        self._lib_dir = new_lib_dir

    @property
    def db_schema(self):
        return self._db_schema

    @db_schema.setter
    def db_schema(self, new_db_schema):
        if isinstance(new_db_schema, str):
            self._db_schema = os.path.join(new_db_schema)
        else:
            raise ValueError("Please enter a New project name")

    @property
    def baseline_version(self):
        return self._baseline_version

    @baseline_version.setter
    def baseline_version(self, new_baseline_version):
        if isinstance(new_baseline_version, str):
            self._baseline_version = new_baseline_version
        else:
            raise ValueError("Please enter the reference to the baseline table")

    @property
    def ror_version(self):
        return self._ror_version

    @ror_version.setter
    def ror_version(self, new_ror_version):
        if isinstance(new_ror_version, str):
            self._ror_version = new_ror_version
        else:
            raise ValueError("Please enter the reference to the ror version")

    @property
    def configfile(self):
        return self._configfile

    @configfile.setter
    def configfile(self, new_configfile):
        if os.path.exists(new_configfile):
            self._configfile = new_configfile
        else:
            raise ValueError("Please enter a valid path with the config file")

    @property
    def project_start_year(self):
        return self._project_start_year

    @project_start_year.setter
    def project_start_year(self, new_project_start_year):
        if 1800 < new_project_start_year < 2100:
            self._project_start_year = int(new_project_start_year)
        else:
            self._project_start_year = datetime.date.today().year - 4

    @property
    def project_end_year(self):
        return self._project_end_year

    @project_end_year.setter
    def project_end_year(self, new_project_end_year):
        if 1800 < new_project_end_year < 2100:
            self._project_end_year = int(new_project_end_year)
        else:
            self._project_end_year = datetime.date.today().year

    @property
    def s3_bucket(self):
        return self._s3_bucket

    @s3_bucket.setter
    def s3_bucket(self, new_s3_bucket):
        if isinstance(new_s3_bucket, str):  # when S3 setup change value
        # if os.path.exists(new_s3_bucket):
            self._s3_bucket = new_s3_bucket
        else:
            raise ValueError("Please enter a correct S3 bucket name (eg projects)")

    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self, new_uid):
        self._uid = new_uid


    # =============================================================================
    # Methods
    # =============================================================================
    """
    Steps to run (order is very important as dependencies exist between steps)
    Usage: 
    1) add as many pipeline step as needed, for instance p1.pipeline_end()
    2) run the script
    3) log of results is saved in the app_project_name.log file
    steps:
    len = generate SQL table(s): papers_dataset from LENS api call (needs Lens search strings in text files in the [PROJECT]/search strategy folder
    """
    # =============================================================================
    # Pipeline steps
    # ============================================================================
    def pipeline_bas(self):
        """
        Details in ./pipeline_VERSION/README.txt
        """
        print("\t >>> BAS, generate SQL table(s): baselines and taxonomies")
        " Baselines DB setup "
        # create_baseline_table(self._data_dir, self._baseline_version)
        # print('\t\t Table baselines created')
        " Taxonomies creation "
        # i_file = os.path.join(self._lib_dir, "sql/create_db_taxonomies.sql")
        # with open(i_file, 'r') as sql_file:
        #     sql_code = sql_file.read()
        # conn.execute(sql_code)
        """ Classifications """
        file_topics = os.path.join(self._data_dir, self._baseline_version, 'openalex_topics.pkl')
        file_concepts = os.path.join(self._data_dir, self._baseline_version, 'openalex_concepts.pkl')
        oa = GetOpenAlexData(api_configuration= self.api_config_oa)
        oa.set_openalex_api()
        if os.path.exists(file_topics):
            x = pd.read_pickle(file_topics)
        else:
            x = oa.get_openalex_topics()
            x.to_pickle(file_topics)
        if os.path.exists(file_concepts):
            y = pd.read_pickle(file_concepts)
        else:
            y = oa.get_openalex_concepts()
            l = ["display_name","level","description","works_count","cited_by_count","ancestors","related_concepts","updated_date","created_date","ids.openalex","ids.wikidata","ids.wikipedia","ids.mag", "international.display_name.fr","international.description.fr"]  ## selected columns to reduce file size (800M raw)
            y = y[l]
            y.to_pickle(file_concepts)
        get_classification_openalex(self._data_dir, self._baseline_version, file_concepts, file_topics)
        print('\t\t Categories baselines completed')
        """ ROR data dump"""
        # ror = RORapi(self.api_config_zenodo)
        # json_obj = ror.get_ror_dump()
        # get_ror_organisations(self._data_dir, self._baseline_version, json_obj)
        print('\t\t ROR baselines completed')
        """ Final cleanup """
   
    def pipeline_len(self):
        """
        Details in ./pipeline_VERSION/README.txt
        """
        print("\t >>> API, extract uids from thelens.org API and export to SQL")
        if self._project_variant:
            project_variant_string = self._project_variant + "_"
        else:
            project_variant_string = ""
        if not os.path.exists(self._tempdir):
            os.mkdir(self._tempdir)
        if not os.path.exists(os.path.join(self._tempdir, '{}records_dataset.parquet'.format(project_variant_string))):
            # infile = os.path.join(self._wdir, "config", "search_strategy.yaml")
            infile = os.path.join(self._root_dir, "config", "search_strategy.yaml")
            with open(infile, 'r') as f:
                search_strategy = yaml.safe_load(f)
            # print(len(search_strategy))
            search_strategy = load_search_strategy(infile)
            # ss = search_strategy.loc[search_strategy.source.isin(['lens_scholarly', 'lens_patents']) & search_strategy.category.isin(['main', 'secondary'])]
            """ Run the Lens API for the main searches"""
            ss = search_strategy.loc[(search_strategy.source == 'lens_scholarly') & (search_strategy.category == 'main')]
            nb_searches =ss.shape[0]
            if nb_searches > 0:
                uids = pd.DataFrame()
                sectors = pd.DataFrame()
                lens = GetLensData(api_configuration= self._api_config_lensp,
                                    query_string= None,
                                    query_parameters=None, 
                                    page_size=1000,
                                    aggregation_string=None,
                                    api_type='scholarly',
                                    api_sort=[{"relevance":"desc"}, {"year_published": "desc"}],
                                    api_include= None,
                                    api_exclude=None,
                                    api_stemming=True,
                                    api_regex=False,
                                    api_min_score=0 
                                )
                for i in ss.index:
                    topic = ss.loc[ss.index == i,].iloc[0]['id']
                    lens.query_string = ss.loc[ss.index == i,].iloc[0]['value']
                    df, df_aggregation, nb_total, max_score = lens.get_lens_data(self._project_start_year, self._project_end_year) 
                    outfile = os.path.join(self._tempdir, '{}{}_{}_raw.pkl'.format(
                        project_variant_string,
                        lens.api_type,
                        topic
                        )
                    )
                    df.to_pickle(outfile)
                    uids = uids.merge(df[[self._uid, topic, "score"]], on=self._uid, how='outer')
                    sectors = pd.concat([sectors, df])
                # uids['n'] = nb_searches
                # uids['ranking'] = uids[search_strategy[i][j]['topics']].max(axis=1, numeric_only=True)
                # if self.min_relevance > 0:
                #     uids = uids[uids['score'] >= self.min_relevance]  ## only returns results above relevance threshold
                uids.drop_duplicates(inplace=True)
                sectors.sort_values(by=[self._uid, 'score'], ascending=False, inplace=True)
                sectors.drop_duplicates(subset=[self._uid], keep='first', inplace=True)
                uids = uids.merge(sectors, on=self._uid, how='left')
                uids.to_pickle(os.path.join(self._tempdir, '{}{}records_topics.pkl'.format(project_variant_string, 'lens_scholarly')))
                sectors.to_pickle(os.path.join(self._tempdir, '{}{}_raw.pkl'.format(project_variant_string, 'lens_scholarly')))
                    
            """ Run the Lens API for the secondary searches with aggregates"""
            ss = search_strategy.loc[(search_strategy.source == 'lens_scholarly') & (search_strategy.category == 'secondary')]
            ss_agg = search_strategy.loc[(search_strategy.source == 'lens_scholarly') & (search_strategy.category == 'aggegation')]
            if ss.shape[0] > 0:
                lens = GetLensData(api_configuration= self._api_config_lensp,
                                    query_string= None,
                                    query_parameters=None, 
                                    page_size=100,
                                    aggregation_string=None,
                                    api_type='scholarly',
                                    api_sort=[{"relevance":"desc"}, {"year_published": "desc"}],
                                    api_include= None,
                                    api_exclude=None,
                                    api_stemming=True,
                                    api_regex=False,
                                    api_min_score=0 
                                )
                for i in ss.index:
                    if ss_agg.shape[0] > 0:
                        for agg in ss_agg:
                            lens.query_string = ss.loc[ss.index == i,].iloc[0]['value']
                            lens.aggregation_string = ss_agg.loc[ss_agg.index == agg,].iloc[0]['value']
                            topic = ss.loc[ss.index == i,].iloc[0]['id']
                            table = ss_agg.loc[ss_agg.index == agg,].iloc[0]['id']
                            df, df_aggregation, nb_total, max_score = lens.get_lens_data(self._project_start_year, self._project_end_year) 
                            outfile = os.path.join(self._tempdir, '{}{}_{}_{}_aggregate.pkl'.format(
                                project_variant_string,
                                lens.api_type,
                                topic,
                                table
                                )
                            )
                            df_aggregation.to_pickle(outfile)
                    # lens.aggregation_string = None
                    # df, df_aggregation, nb_total, max_score = lens.get_lens_data(self._project_start_year, self._project_end_year) 
                    # outfile = os.path.join(self._tempdir, '{}{}_{}_raw.pkl'.format(
                    #     project_variant_string,
                    #     lens.api_type,
                    #     topic
                    #     )
                    # )
                    # df.to_pickle(outfile)            
            # for i in search_strategy.keys():  ## iterate on the Lens searches
            #     if i.isin(['lens_scholarly', 'lens_patents']):
            #         for j in ['main', 'secondary']:  ## iterate on main or secondary
            #             for k in search_strategy[i][j]['topics']: # iterates on the topics
            #                 if i == 'lens_patents':
            #                     lens = GetLensData(api_configuration= self._api_config_lensp,
            #                         query_string= None,
            #                         query_parameters=None, 
            #                         page_size=100,
            #                         aggregation_string=None,
            #                         api_type='patents',
            #                         api_sort=[{"relevance":"desc"}, {"year_published": "desc"}],
            #                         api_include= None,
            #                         api_exclude=None,
            #                         api_stemming=True,
            #                         api_regex=False,
            #                         api_min_score=0         
            #                     )
            #                 else:
            #                     lens = GetLensData(api_configuration= self._api_config_lensp,
            #                         query_string= None,
            #                         query_parameters=None, 
            #                         page_size=1000,
            #                         aggregation_string=None,
            #                         api_type='scholarly',
            #                         api_sort=[{"relevance":"desc"}, {"year_published": "desc"}],
            #                         api_include= None,
            #                         api_exclude=None,
            #                         api_stemming=True,
            #                         api_regex=False,
            #                         api_min_score=0 
            #                     )
            #                 topic = list(k.keys())[0]
            #                 lens.query_string = list(k.values())[0]
            #                 aggregations = search_strategy[i]['aggregations']
            #                 for agg in aggregations:
            #                     table = list(agg.keys())[0]
            #                     lens.aggregation_string = list(agg.values())[0]
            #                     df, df_aggregation, nb_total, max_score = lens.get_lens_data(self._project_start_year, self._project_end_year) 
            #                     outfile = os.path.join(self._tempdir, '{}{}_{}_{}.pkl'.format(
            #                         project_variant_string,
            #                         lens.api_type,
            #                         topic,
            #                         table
            #                         )
            #                     )
            #                     df_aggregation.to_pickle(outfile)
            #                 if j == 'main':
            #                     lens.aggregation_string = None
            #                     df, df_aggregation, nb_total, max_score = lens.get_lens_data(self._project_start_year, self._project_end_year) 
            #                     outfile = os.path.join(self._tempdir, '{}{}_{}_raw.pkl'.format(
            #                         project_variant_string,
            #                         lens.api_type,
            #                         topic
            #                         )
            #                     )
            #                     df.to_pickle(outfile)
            # for i in search_strategy.keys():  ## iterate on the Lens searches
            #     if i.isin(['lens_scholarly', 'lens_patents']):
            #         for j in ['main', 'secondary']:  ## iterate on main or secondary
            #             uids = pd.DataFrame()
            #             sectors = pd.DataFrame()
            #             for k in search_strategy[i][j]['topics']: # iterates on the topics
            #                 if i == 'lens_patents':
            #                     lens.api_type = 'patents'
            #                 else:
            #                     lens.api_type = 'scholarly'
            #                 topic = list(k.keys())[0]
            #                 for agg in search_strategy[i]['aggregations']:
            #                     table = list(agg.keys())[0]
            #                     print(table)
            #                 if j == 'main':
            #                     outfile = os.path.join(self._tempdir, '{}{}_{}_raw.pkl'.format(
            #                         project_variant_string,
            #                         lens.api_type,
            #                         topic
            #                         )
            #                     )
            #                     df = pd.read_pickle(outfile)
            #                     # df.sort_values(by=['uid', 'py', topic], ascending=[False, True, False], inplace=True)  ## remove duplicates when publications are in 2 different years
            #                     # df.drop_duplicates(subset=['uid'], keep='first', inplace=True)
            #                     # df['topic'] = topic
            #                     # # df['ranking'] = df[topic]
            #                     # if topic == list(search_strategy[i][j]['topics'].keys())[0]:
            #                     #     uids = df[[self._uid, topic]].copy(deep=True)
            #                     #     sectors = df[[self._uid, 'ranking', 'topic']].copy(deep=True)
            #                     # else:
            #                     uids = uids.merge(df[[self._uid, topic, "score"]], on=self._uid, how='outer')
            #                     sectors = pd.concat([sectors, df])
            #             uids['n'] = uids[search_strategy[i][j]['topics']].count(axis=1, numeric_only=True)
            #             uids['ranking'] = uids[search_strategy[i][j]['topics']].max(axis=1, numeric_only=True)
            #             # if self.min_relevance > 0:
            #             #     uids = uids[uids['score'] >= self.min_relevance]  ## only returns results above relevance threshold
            #             uids.drop_duplicates(inplace=True)
            #             sectors.sort_values(by=[self._uid, 'score'], ascending=False, inplace=True)
            #             sectors.drop_duplicates(subset=[self._uid], keep='first', inplace=True)
            #             uids = uids.merge(sectors, on=self._uid, how='left')
            #             uids.to_pickle(os.path.join(self._tempdir, '{}{}records_topics.pkl'.format(project_variant_string, i)))
            #             sectors.to_pickle(os.path.join(self._tempdir, '{}{}_raw.pkl'.format(project_variant_string, i)))

    def pipeline_ddb(self, main_source='lens_scholarly', network_max_team_size=20):
        """
        Details in ./pipeline_VERSION/README.txt
        """
        print("\t >>> DDB, generate SQL table(s): save project data into a DB")
        if self._project_variant:
            project_variant_string = self._project_variant + "_"
        else:
            project_variant_string = ""
        """ Export code """
        source_baseline = os.path.join(self._data_dir, self._baseline_version, "baseline_data.duckdb")
        """database setup"""
        infile = os.path.join(self._root_dir, self._project_name, "config", "search_strategy.yaml")
        infile = os.path.join(self._root_dir, "config", "search_strategy.yaml")
        search_strategy = load_search_strategy(infile)
        """ Lens Scholarly data export """
        ss = search_strategy.loc[(search_strategy.source == main_source) & (search_strategy.category == 'main')]
        if ss.shape[0] > 0:
            version_name = "{}{}".format(project_variant_string, main_source)
            infile = os.path.join(self._tempdir, '{}_raw.pkl'.format(version_name))
            outfile = os.path.join(self._data_dir, self._project_name, 'project_data.duckdb')
            create_ddb(infile, outfile, project_variant_string, source_baseline, source_data=main_source, network_max_team_size=20, network_sample_size= self.network_sample_size) # use the core/ddb_data module
            print("\t\t - Data for {} {} saved into the duckd".format(self._uid, main_source))
            # with open(infile, 'r') as f:
            #     search_strategy = yaml.safe_load(f)
            # for i in search_strategy.keys():  ## iterate on the Lens searches
            #     version_name = "{}{}".format(project_variant_string, i)
            #     infile = os.path.join(self._tempdir, '{}_raw.pkl'.format(version_name))
            #     if i == main_source:
            #         outfile = os.path.join(self._data_dir, self._project_name, 'project_data.duckdb')
            #         create_ddb(infile, outfile, project_variant_string, source_baseline, source_data=i, network_sample_size= self.network_sample_size) # use the core/ddb_data module
                # else:
                #     outfile = os.path.join(self._data_dir, self._project_name, '{}_data.duckdb'.format(version_name))
                # create_ddb(infile, outfile, project_variant_string, source_data=i) # use the core/ddb_data module
            

        