# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.modules.baselines.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus
# Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 15/10/2024
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
import numpy as np
import pandas as pd
from recordlinkage.preprocessing import clean
import recordlinkage
import os
import duckdb
# =============================================================================
# Functions and classes
# =============================================================================


def print_hi(name):
    print(f'>>> {name}')


if __name__ == '__main__':
    print_hi('Python start')


def create_baseline_table(db_directory, baseline_version):
    # db_infile = a DuckDB (.duckdb) DB
    try:
        print('\t start baseline table creation')
        """database setup"""
        baseline_dir = os.path.join(db_directory, baseline_version)
        if not os.path.exists(baseline_dir):  # creates search directory
            os.mkdir(baseline_dir)
            print("\t", baseline_dir, "created")
        db_infile = os.path.join(db_directory, baseline_version, 'baseline_data.duckdb')
        conn = duckdb.connect(db_infile)  
        sql_code = "CREATE SCHEMA IF NOT EXISTS baselines;"
        conn.execute(sql_code)
        """ Final code """
        conn.close()
    except Exception as e:
        print(e)
    finally:
        print('\t baseline DB created in {}'.format(db_infile))

def get_classification_openalex(db_directory, baseline_version, file_concepts, file_topics):
    # db_directory = data directory where the basleines should be stored (eg. \user\MyData\)
    # baseline_version = baselines version name (eg baslines, baselines_2024_10)
    # file_concepts = a pickle file from an OpenAlex API call using Pyalex.Concepts() and repository e.g. openalex_concepts.pkl
    # file_topics = a pickle file from an OpenAlex API call using Pyalex.Topics() e.g. openalex_topics.pkl

    def rename_id (my_id, id_type='https://openalex.org/'):
        new_id = my_id
        if isinstance(my_id, str):
            new_id = my_id.replace(id_type, "")
        return new_id
    
    try:
        print('\t start classification from OA')
        """ DB connection """
        db_infile = os.path.join(db_directory, baseline_version, 'baseline_data.duckdb')
        conn = duckdb.connect(db_infile)  
        """ 
        Get data for CONCEPTS
        """
        my_file = file_concepts  ## list of concepts
        df = pd.read_pickle(my_file)
        df['parent'] = 0
        df['parent'] = df.apply(lambda x: [i['id'] for i in x['ancestors'] if i['level'] ==  x['level']-1], axis=1)
        df['nb_parents'] = df['parent'].apply(lambda x: len(x))
        df['related'] = 0
        df['related'] = df.apply(lambda x: [i['id'] for i in x['related_concepts'] if i['level'] ==  x['level']], axis=1)
        df['nb_related'] = df['related'].apply(lambda x: len(x))
        df.rename(inplace=True, columns={'related':'sibling', 'nb_related': 'nb_siblings'})
        df.drop(columns=['ancestors', 'related_concepts'], inplace=True)
        df['ids.openalex'] = df['ids.openalex'].apply(rename_id)
        e = df[['ids.openalex', 'parent', 'nb_parents']].copy()  ## create the edgelist for parents (directed)
        e = e.explode('parent')
        e['ids.openalex'] = e['ids.openalex'].apply(rename_id)
        e['parent'] = e['parent'].apply(rename_id)
        e = e[~e.parent.isna()]
        e.reset_index(inplace=True, drop=True)
        r = df[['ids.openalex', 'sibling', 'nb_siblings']].copy()  ## create the edgelist for related records (directed to simplify visualisation)
        r = r.explode('sibling')
        r['ids.openalex'] = r['ids.openalex'].apply(rename_id)
        r['sibling'] = r['sibling'].apply(rename_id)
        r = r[~r.sibling.isna()]
        r.reset_index(inplace=True, drop=True)
        df.drop(columns=['sibling', 'nb_siblings', 'parent', 'nb_parents'], inplace=True)
        my_file_concepts = os.path.join(db_directory, baseline_version, '{}_concepts_nodes.pkl'.format('_baselines'))
        df.to_pickle(my_file_concepts)
        sql_code = "CREATE TABLE baselines.concepts_nodes AS SELECT * FROM df;"
        conn.execute(sql_code)
        # my_file = os.path.join(db_directory, baseline_version, '{}_concepts_edgelist_parents.pkl'.format('baselines'))
        # e.to_pickle(my_file)
        sql_code = "CREATE TABLE baselines.concepts_edgelist_parents AS SELECT * FROM e;"
        conn.execute(sql_code)
        # my_file = os.path.join(db_directory, baseline_version, '{}_concepts_edgelist_siblings.pkl'.format('baselines'))
        # r.to_pickle(my_file)  ## only used for comparison purpose
        sql_code = "CREATE TABLE baselines.concepts_edgelist_siblings AS SELECT * FROM r;"
        conn.execute(sql_code)
        """ 
        Get data for TOPICS
        """
        my_file = file_topics  ## list of topics
        df = pd.read_pickle(my_file)
        l = ['display_name', 'description', 'keywords', 'siblings', 'works_count', 'cited_by_count', 'updated_date', 'created_date', 'ids.openalex', 'ids.wikipedia', 'subfield.id', 'subfield.display_name', 'field.id', 'field.display_name', 'domain.id', 'domain.display_name', 'domain.wikidata', 'domain.wikipedia', 'field.wikidata', 'field.wikipedia', 'subfield.wikidata', 'subfield.wikipedia']
        df = df[l]
        df['domain.id'] = df['domain.id'].apply(rename_id)
        df['field.id'] = df['field.id'].apply(rename_id)
        df['subfield.id'] = df['subfield.id'].apply(rename_id)
        df['ids.openalex'] = df['ids.openalex'].apply(rename_id)
        df['nb_siblings'] = df['siblings'].apply(lambda x: len(x))  ## create the edgelist for related records (directed to simplify visualisation)
        r = df[['ids.openalex', 'siblings', 'nb_siblings']].copy()  ## create ?
        r = r.explode('siblings')
        r.rename(inplace=True, columns={'siblings': 'sibling'})
        r = r[~r.sibling.isna()]
        r['sibling'] = r['sibling'].apply(lambda x: x['id'] )
        r['ids.openalex'] = r['ids.openalex'].apply(rename_id)
        r['sibling'] = r['sibling'].apply(rename_id)
        r.reset_index(inplace=True, drop=True)
        df['ids.wikidata'] = None
        df['level'] = 3
        for i in ['domain', 'field', 'subfield']:  ## append the node list with tree data
                        my_fields = {'ids.openalex': i+'.id', 'display_name': i+'.display_name', 'ids.wikidata': i+'.wikidata', 'ids.wikipedia': i+'.wikipedia'}
                        x = df.groupby(by=list(my_fields.values())).size().reset_index()
                        if i == 'domain':
                            x['level']  = 0
                        elif i == 'field':
                            x['level']  = 1
                        else:
                            x['level']  = 2
                        for f in my_fields:
                            x.rename(columns={my_fields[f]:f }, inplace=True)
                            x[i+'.id'] = x['ids.openalex']
                        df = pd.concat([df, x[list(my_fields)+['level', i+'.id']]])
        d = df.groupby(by=['field.id', 'domain.id']).size().reset_index()  ## create the edgelist for parents (directed)
        x = d.groupby('domain.id').size()
        x.name = 'nb_parents'
        d = d.merge(x, on='domain.id', how='left')
        d.rename(columns={'domain.id': 'parent', 'field.id': 'ids.openalex'}, inplace=True)
        f = df.groupby(by=['subfield.id', 'field.id']).size().reset_index()
        x = f.groupby('field.id').size()
        x.name = 'nb_parents'
        f = f.merge(x, on='field.id', how='left')
        f.rename(columns={'field.id': 'parent', 'subfield.id': 'ids.openalex'}, inplace=True)
        s = df[df.level==0].groupby(by=['subfield.id', 'ids.openalex']).size().reset_index()
        x = s.groupby('subfield.id').size()
        x.name = 'nb_parents'
        s = s.merge(x, on='subfield.id', how='left')
        s.rename(columns={'subfield.id': 'parent'}, inplace=True)
        e = pd.concat([d, f, s])
        e = e[['ids.openalex', 'parent', 'nb_parents']].sort_values(by=['parent']).reset_index()
        df.drop(columns=['siblings', 'nb_siblings'], inplace=True)
        # my_file = os.path.join(db_directory, baseline_version, '{}_topics_nodes.pkl'.format('baselines'))
        # df.to_pickle(my_file)
        sql_code = "CREATE TABLE baselines.topics_nodes AS SELECT * FROM df;"
        conn.execute(sql_code)
        # my_file = os.path.join(db_directory, baseline_version, '{}_topics_edgelist_parents.pkl'.format('baselines'))
        # e.to_pickle(my_file)
        sql_code = "CREATE TABLE baselines.topics_edgelist_parents AS SELECT * FROM e;"
        conn.execute(sql_code)
        # my_file = os.path.join(db_directory, baseline_version, '{}_topics_edgelist_siblings.pkl'.format('baselines'))
        # r.to_pickle(my_file)
        sql_code = "CREATE TABLE baselines.topics_edgelist_siblings AS SELECT * FROM r;"
        conn.execute(sql_code)
        """
        Comparison concepts and topics
        """
        d = pd.read_pickle(my_file_concepts)  ## compare concepts and topics
        x = df.reset_index(drop=False)
        y = d.reset_index(drop=False)
        l = ['ids.openalex', 'ids.wikipedia', 'ids.wikidata']
        mp = x.merge(y[l], on='ids.wikipedia', how='inner', suffixes=("", "_m"))
        md = x.merge(y[l], on='ids.wikidata', how='inner', suffixes=("", "_m"))
        m =md[['ids.openalex']].merge(md[['ids.openalex']], on='ids.openalex', how='inner')
        my_file = os.path.join(db_directory, baseline_version, 'result_concepts_topics_map.txt') 
        x = d[d.level==1].groupby(by=['ids.openalex', 'display_name']).size()
        x.to_csv(my_file, sep='\t')
        """ Final code """
        conn.close()
    except Exception as e:
        print(e)
    finally:
        files = [my_file_concepts, file_concepts, file_topics]
        for file_path in files:
            if os.path.exists(file_path):
                os.remove(file_path)
                print("{} has been deleted successfully".format(file_path))   
        print('\t AO classification imported into '.format(db_infile))

def concepts_duplication_correction(df):
    # remove duplicates of display names used to match Lens categories (fields of study)
    # list_ids = {'C21036866':'C2776756274', 'C8880873':'C2776838516', 'C2776095024':'C2780027415', 'C205147927':'C119047807'}
    list_ids = ['C21036866', 'C8880873', 'C2776095024', 'C205147927']
    df['category_id'] = df['ids.openalex']
    df['raw_display_name'] = df['display_name']
    df = df[~df['ids.openalex'].isin(list_ids)]
    # for i in list_ids:
    #     df.loc[df['ids.openalex'] == i, 'category_id'] = list_ids[i]  
    # It changes "Metre" (C151011524, C151011524) which points to different concepts and wikipedia pages.
    df.loc[df['ids.openalex'] == 'C151011524', 'display_name'] = "Metre (SI)"
    df.loc[df['ids.openalex'] == 'C182181037', 'display_name'] = "Metre (poetry)"
    # df.drop_duplicates(subset=[['category_id', 'level', 'display_name']], inplace=True)
    return df

def openalex_concepts_hierarchy(db_directory, baseline_version):
    # db_directory = data directory where the basleines should be stored (eg. \user\MyData\)
    # baseline_version = baselines version name (eg baslines, baselines_2024_10)
    try:
        print('\t start classification hierarchy from OA\'s concepts')
        """ DB connection """
        db_infile = os.path.join(db_directory, baseline_version, 'baseline_data.duckdb')
        conn = duckdb.connect(db_infile, read_only=True)  
        """ 
        Get data for CONCEPTS
        """
        sql_code = "SELECT * from baselines.concepts_nodes;"
        concepts = conn.execute(sql_code).fetchdf()
        sql_code = "SELECT * from baselines.concepts_edgelist_parents;"
        e = conn.execute(sql_code).fetchdf()
        conn.close()
        # correction for duplicated display_names
        concepts = concepts_duplication_correction(concepts)
        c = concepts[['category_id', 'level', 'display_name', 'raw_display_name']].copy()
        e = e[['ids.openalex', 'parent']].rename(columns={'ids.openalex': 'category_id'})
        e = e.merge(c[['category_id', 'level']], left_on='parent', right_on='category_id', suffixes=("", "_p")).drop(columns=['category_id_p'])
        # levels 0 and 1
        c_parents = c.merge(e, on='category_id', how='left', suffixes=("", "_p")).drop(columns=['level_p']).fillna(0)
        c_parents['parent_1'] = "N/A"
        c_parents['parent_0'] = "N/A"
        c_parents['parent_0'] = c_parents['parent_0'].mask(c_parents.level ==0, c_parents['category_id'])
        c_parents['parent_1'] = c_parents['parent_1'].mask(c_parents.level ==1, c_parents['category_id'])
        c_parents['parent_0'] = c_parents['parent_0'].mask(c_parents.level ==1, c_parents['parent'])
        # Level 2
        c_parents = c_parents.merge(e, left_on='parent', right_on='category_id', how='left', suffixes=("", "_2")).drop(columns=['category_id_2', 'level_2']).fillna(0)
        c_parents['parent_1'] = c_parents['parent_1'].mask(c_parents.level ==2, c_parents['parent'])
        c_parents['parent_0'] = c_parents['parent_0'].mask(c_parents.level ==2, c_parents['parent_2'])
        c_parents = c_parents.drop(columns=['parent'])
        # Level 3
        c_parents = c_parents.merge(e, left_on='parent_2', right_on='category_id', how='left', suffixes=("", "_3")).drop(columns=['category_id_3', 'level_3']).fillna(0)
        c_parents['parent_1'] = c_parents['parent_1'].mask(c_parents.level ==3, c_parents['parent_2'])
        c_parents['parent_0'] = c_parents['parent_0'].mask(c_parents.level ==3, c_parents['parent'])
        c_parents = c_parents.drop(columns=['parent_2'])
        # Level 4
        c_parents = c_parents.merge(e, left_on='parent', right_on='category_id', how='left', suffixes=("", "_4")).drop(columns=['category_id_4', 'level_4']).fillna(0)
        c_parents['parent_1'] = c_parents['parent_1'].mask(c_parents.level ==4, c_parents['parent'])
        c_parents['parent_0'] = c_parents['parent_0'].mask(c_parents.level ==4, c_parents['parent_4'])
        c_parents = c_parents.drop(columns=['parent'])
        # Level 4
        c_parents = c_parents.merge(e, left_on='parent_4', right_on='category_id', how='left', suffixes=("", "_5")).drop(columns=['category_id_5', 'level_5']).fillna(0)
        c_parents['parent_1'] = c_parents['parent_1'].mask(c_parents.level ==5, c_parents['parent_4'])
        c_parents['parent_0'] = c_parents['parent_0'].mask(c_parents.level ==5, c_parents['parent'])
        c_parents = c_parents.drop(columns=['parent', 'parent_4'])
        c_parents = c_parents.merge(c, left_on='parent_1', right_on='category_id', how='left', suffixes=("", "_1")).drop(columns=['category_id_1', 'level_1'])
        c_parents = c_parents.merge(c, left_on='parent_0', right_on='category_id', how='left', suffixes=("", "_0")).drop(columns=['category_id_0', 'level_0'])
        c_parents = c_parents.drop_duplicates()
        c_parents['nb_parent_1'] = c_parents.groupby('category_id')['parent_1'].transform('nunique')
        c_parents['nb_parent_0'] = c_parents.groupby('category_id')['parent_0'].transform('nunique')
        """ Final code """
        conn = duckdb.connect(db_infile)  
        sql_code = "CREATE TABLE baselines.concepts_hierarchy AS SELECT * FROM c_parents;"
        conn.execute(sql_code)
        conn.close()
    except Exception as e:
        print(e)
    finally: 
        print('\t classification hierarchy from OA\'s concepts')

def get_ror_organisations(db_directory, baseline_version, json_object):
    # db_infile = a DuckDB (.duckdb) DB
    # creates tables from ROR data dump file (JSON):
    # - ror (pk = id (ror_id))
    # - ror_location (ror_id, geoname_id)
    # - ror_location_id (pk = geoname_id)
    # - ror_external_id (ror_id, external_ids)
    # - ror_names (ror_id, names)
    # - ror_relationsships (from (ror_id from), to (ror_id to))
    try:
        print('\t start baseline table creation')
        """database setup"""
        db_infile = os.path.join(db_directory, baseline_version, 'baseline_data.duckdb')
        conn = duckdb.connect(db_infile)  
        df = pd.json_normalize(json_object, errors='ignore')
        """ Locations """
        d = pd.json_normalize(json_object, record_path='locations', meta='id', errors='ignore')
        g = d[['geonames_id', 'geonames_details.country_code', 'geonames_details.country_name', 'geonames_details.lat', 'geonames_details.lng', 'geonames_details.name']].drop_duplicates(subset='geonames_id').rename(columns={'geonames_details.country_code':'country_code', 'geonames_details.country_name':'country_name', 'geonames_details.lat':'lat', 'geonames_details.lng':'lng', 'geonames_details.name':'name'})
        d = d[['id', 'geonames_id']]
        sql_code = "CREATE TABLE baselines.ror_location AS SELECT * FROM d;"
        conn.execute(sql_code)
        sql_code = "CREATE TABLE baselines.ror_location_id AS SELECT * FROM g;"
        conn.execute(sql_code)
        """ External IDs """
        d = pd.json_normalize(json_object, record_path='external_ids', meta='id', errors='ignore')
        d = d.explode('all').rename(columns={'all': 'value'})
        d['preferred'] = d['value'].mask(d.preferred.isnull(), d['value'])
        sql_code = "CREATE TABLE baselines.ror_external_id AS SELECT * FROM d;"
        conn.execute(sql_code)
        """ Names """
        d = pd.json_normalize(json_object, record_path='names', meta='id', errors='ignore')
        d = d.explode('types').rename(columns={'types': 'type'}).reset_index(drop=True)
        g = d.loc[d.type=='ror_display', ['id', 'value']].drop_duplicates(subset=['id']).rename(columns={'value': 'ror_display'})
        df = df.merge(g, on='id', how='left')
        g = d.loc[d.type=='acronym', ['id', 'value']].drop_duplicates(subset=['id']).rename(columns={'value': 'acronym'})
        df = df.merge(g, on='id', how='left')
        sql_code = "CREATE TABLE baselines.ror_names AS SELECT * FROM d;"
        conn.execute(sql_code)
        """ Links """
        d = pd.json_normalize(json_object, record_path='links', meta='id', errors='ignore')
        g = d.loc[d.type=='website', ['id', 'value']].drop_duplicates(subset=['id']).rename(columns={'value': 'website'})
        df = df.merge(g, on='id', how='left')
        g = d.loc[d.type=='wikipedia', ['id', 'value']].drop_duplicates(subset=['id']).rename(columns={'value': 'wikipedia'})
        df = df.merge(g, on='id', how='left')
        """ Types """
        df['nb_types'] = df['types'].apply(lambda x: len(x))
        df['main_type'] = df['types'].apply(lambda x: x[0])
        df['second_type'] = df['types'].apply(lambda x: x[1] if len(x)>1 else None)
        df['third_type'] = df['types'].apply(lambda x: x[1] if len(x)>2 else None)
        """ Domains """
        d = pd.json_normalize(json_object, record_path='domains', meta='id', errors='ignore').rename(columns={'0': 'domain'})
        sql_code = "CREATE TABLE baselines.ror_domains AS SELECT * FROM d;"
        conn.execute(sql_code)
        """ Relationships """
        d = pd.json_normalize(json_object, record_path='relationships', meta='id', record_prefix="to_", errors='ignore').rename(columns={'to_type': 'type', 'to_label': 'to_ror_display', 'to_id': 'to', 'id': 'from'})
        sql_code = "CREATE TABLE baselines.ror_relationships AS SELECT * FROM d;"
        conn.execute(sql_code)
        df['nb_relationships'] = df['relationships'].apply(lambda x: len(x))
        """ Data """
        l = ['id', 'ror_display', 'acronym', 'website', 'wikipedia', 'established', 'status', 'nb_types', 'nb_relationships', 'main_type', 'second_type', 'third_type', 'admin.created.date', 'admin.created.schema_version', 'admin.last_modified.date', 'admin.last_modified.schema_version']
        df = df[l]
        sql_code = "CREATE TABLE baselines.ror AS SELECT * FROM df;"
        conn.execute(sql_code)
        """ Final code """
        conn.close()
    except Exception as e:
        print(e)
    finally:
        print('\t baseline DB updated in {}'.format(db_infile))
# =============================================================================
# Global variables
# =============================================================================
# connection = create_connection_to_postgresql("PSQL")
# =============================================================================
# Start of script
# =============================================================================


# =============================================================================
# End of script
# =============================================================================

if __name__ == '__main__':
    print_hi('Python completed')
