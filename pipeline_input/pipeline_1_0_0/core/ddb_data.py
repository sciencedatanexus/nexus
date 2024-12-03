# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.modules.ddb_data.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus
# Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 09/10/2024
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
import numpy as np
import pandas as pd
import duckdb
from itertools import combinations
from collections import Counter
from recordlinkage.preprocessing import clean
import math
import recordlinkage
import os
import ibis
import ast


# =============================================================================
# Functions and classes
# =============================================================================


def print_hi(name):
    print(f'>>> {name}')


if __name__ == '__main__':
    print_hi('Python start')

def create_table_records_id(df, rec_id, conn, label):
    """External ids table"""
    d = df[[rec_id, 'external_ids']].copy()
    d = d.explode('external_ids')
    dict_df = d['external_ids'].apply(pd.Series)
    d = pd.concat([d.drop('external_ids', axis=1), dict_df], axis=1)
    d['type'] = d['type'].astype('category')
    sql_code = "CREATE TABLE project.{}records_id AS SELECT * FROM d;".format(label)
    conn.execute(sql_code)
    print("\t\t table_records_id")
    # conn.sql("select count(*) from project.records_id;")  # check DuckDB table
def create_table_source(df, definition_source, conn, label):
    """Source table"""
    sql_code = "drop TABLE if exists project.{}source;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}source_issn;".format(label)
    conn.execute(sql_code)
    d = df[definition_source].copy()
    l = ['source.title', 'source.publisher', 'source.type', 'source.country']
    d.drop_duplicates(subset=l, inplace=True)
    d.reset_index(drop=True, inplace=True)
    issn = d.loc[d['source.issn'] != 'other', ['source.issn']]
    issn = issn.explode('source.issn')
    dict_df = issn['source.issn'].apply(pd.Series)
    issn = pd.concat([issn.drop('source.issn', axis=1), dict_df], axis=1)
    issn = issn.reset_index().rename(columns={'index': 'source_id'})
    d = d.reset_index().rename(columns={'index': 'source_id'})
    d.drop(columns=['source.issn'], inplace=True)
    sql_code = "CREATE TABLE project.{}source_issn AS SELECT * FROM issn;".format(label)
    conn.execute(sql_code)
    # conn.sql("select count(*) from project.source_issn;")  # check DuckDB table
    d.rename(columns={'source.title': 'title', 'source.publisher': 'publisher_name', 'source.type': 'type', 'source.country': 'country'}, inplace=True)
    d['type'] = d['type'].astype('category')
    d['country'] = d['country'].astype('category')
    d['publisher_name'] = d['publisher_name'].astype('category')
    sql_code = "CREATE TABLE project.{}source AS SELECT * FROM d;".format(label)
    conn.execute(sql_code)
    # conn.sql("select count(*) from project.source;")  # check DuckDB table
    print("\t\t table_source")
    return d
def create_table_records(df, d_source, rec_id, conn, label):
    """Records table"""
    sql_code = "drop TABLE if exists project.{}records;".format(label)
    conn.execute(sql_code)
    l = ['source.title', 'source.publisher', 'source.country', 'source.type']
    rec = df[[rec_id, 
    'year_published', 
    'is_open_access', 
    'publication_type', 
    'author_count',
    'scholarly_citations_count',
    'references_resolved_count',
    'references_count',
    'patent_citations_count'
    ]
    + l].copy()
    rec.rename(columns={'source.title': 'title', 'source.publisher': 'publisher_name', 'source.type': 'type', 'source.country': 'country', 'author_count': 'nb_authors'}, inplace=True)
    l = ['title', 'publisher_name', 'type', 'country']
    rec = rec.merge(d_source, on=l, how='left')
    rec.drop(columns=l, inplace=True)
    rec['publication_type'] = rec['publication_type'].astype('category')
    rec['year_published'] = rec['year_published'].astype('category')
    sql_code = "CREATE TABLE project.{}records AS SELECT * FROM rec;".format(label)
    conn.execute(sql_code)
    print("\t\t table_records")
    # conn.sql("select publication_type, count(*) as n from project.records group by publication_type limit 10;")  # check DuckDB table
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
def generate_categories_oaconcepts(df, rec_id, source_baseline_version):
    # outfile = os.path.join(project_variables['data_directory'], project_variables['baseline_version'], 'baseline_data.duckdb')    
    df = df.loc[df.type=='fields_of_study', ['lens_id', 'value']]
    conn_bas = duckdb.connect(source_baseline_version, read_only=True)
    sql_query = "SELECT * from baselines.concepts_nodes;"
    concepts = conn_bas.execute(sql_query).fetchdf()
    sql_query = "SELECT * from baselines.concepts_edgelist_parents;"
    e = conn_bas.execute(sql_query).fetchdf()
    conn_bas.close()
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
    df.rename(columns={'value': 'raw_display_name'}, inplace=True)
    df = df.merge(c_parents, on='raw_display_name', how='left').drop(columns="raw_display_name")
    df['is_not_linked'] = df.groupby(rec_id)['category_id'].transform(lambda x: all([pd.isna(element) for element in x]))
    return df
def generate_categories_oatopics(df, rec_id, source_baseline_version):
    # outfile = os.path.join(project_variables['data_directory'], project_variables['baseline_version'], 'baseline_data.duckdb')    
    df = df.loc[df.type=='fields_of_study', ['lens_id', 'value']]
    conn_bas = duckdb.connect(source_baseline_version, read_only=True)
    sql_query = "SELECT * from baselines.topics_nodes;"
    topics = conn_bas.execute(sql_query).fetchdf()
    sql_query = "SELECT * from baselines.topics_edgelist_parents;"
    e = conn_bas.execute(sql_query).fetchdf()
    conn_bas.close()
    t = topics.loc[topics.level != 3, ['ids.openalex', 'level', 'display_name']].copy().rename(columns={'ids.openalex': 'category_id'})
    e = e[['ids.openalex', 'parent']].rename(columns={'ids.openalex': 'category_id'})
    e = t[['category_id', 'level']].merge(e, on='category_id', how='left', suffixes=("", "_p"))
    et = topics.copy().loc[topics.level==3, ['ids.openalex', 'level','display_name', 'subfield.id', 'field.id', 'domain.id']].rename(columns={'ids.openalex': 'category_id', 'subfield.id': 'parent_2', 'field.id': 'parent_1', 'domain.id': 'parent_0'})
    # levels 0 and 1 and 2
    t_parents = t.merge(e, on='category_id', how='left', suffixes=("", "_p")).drop(columns=['level_p']).fillna(0)
    t_parents['parent_2'] = "N/A"
    t_parents['parent_1'] = "N/A"
    t_parents['parent_0'] = "N/A"
    t_parents['parent_0'] = t_parents['parent_0'].mask(t_parents.level ==0, t_parents['category_id'])
    t_parents['parent_1'] = t_parents['parent_1'].mask(t_parents.level ==1, t_parents['category_id'])
    t_parents['parent_0'] = t_parents['parent_0'].mask(t_parents.level ==1, t_parents['parent'])
    t_parents['parent_1'] = t_parents['parent_1'].mask(t_parents.level ==2, t_parents['parent'])
    t_parents['parent_2'] = t_parents['parent_2'].mask(t_parents.level ==2, t_parents['category_id'])
    # Level 2
    t_parents = t_parents.merge(e, left_on='parent', right_on='category_id', how='left', suffixes=("", "_p")).drop(columns=['category_id_p', 'level_p']).fillna(0)
    t_parents['parent_0'] = t_parents['parent_0'].mask(t_parents.level ==2, t_parents['parent_p'])
    t_parents.drop(columns=['parent', 'parent_p'], inplace=True)
    # Level 3
    t_parents = pd.concat([t_parents, et], ignore_index=True)
    # Final step
    t_parents = t_parents.merge(t, left_on='parent_2', right_on='category_id', how='left', suffixes=("", "_2")).drop(columns=['category_id_2', 'level_2'])
    t_parents = t_parents.merge(t, left_on='parent_1', right_on='category_id', how='left', suffixes=("", "_1")).drop(columns=['category_id_1', 'level_1'])
    t_parents = t_parents.merge(t, left_on='parent_0', right_on='category_id', how='left', suffixes=("", "_0")).drop(columns=['category_id_0', 'level_0'])
    t_parents = t_parents.drop_duplicates()
    t_parents['nb_parent_2'] = t_parents.groupby('category_id')['parent_2'].transform('nunique')
    t_parents['nb_parent_1'] = t_parents.groupby('category_id')['parent_1'].transform('nunique')
    t_parents['nb_parent_0'] = t_parents.groupby('category_id')['parent_0'].transform('nunique')
    df.rename(columns={'value': 'display_name'}, inplace=True)
    df = df.merge(t_parents, on='display_name', how='left')
    df['is_not_linked'] = df.groupby(rec_id)['category_id'].transform(lambda x: all([pd.isna(element) for element in x]))
    return df
def create_table_categories(df, rec_id, conn, label, source_baseline_version):
    # sql_code = "drop TABLE if exists project.{}categories;".format(label)
    # conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}categories_openalex_concepts;".format(label)
    conn.execute(sql_code)
    # sql_code = "drop TABLE if exists project.{}categories_openalex_topics;".format(label)
    # conn.execute(sql_code)
    # """Records table - FOS"""
    # d = df[[rec_id, 'fields_of_study']].copy()
    # d.dropna(how='any', inplace=True)
    # d = d.explode('fields_of_study')
    # d['category_id'] = pd.NA
    # d['category_id'] = d['category_id'].astype('category')
    # d.rename(columns={'fields_of_study': 'value'}, inplace=True)
    # d['qualifier'] = pd.NA
    # d['qualifier_id'] = pd.NA
    # d['type'] = 'fields_of_study'
    # d['type'] = d['type'].astype('category')
    # """Records table - MESH"""
    # m = df[[rec_id, 'mesh_terms']].copy()
    # m.dropna(how='any', inplace=True)
    # m = m.explode('mesh_terms')
    # dict_df = m['mesh_terms'].apply(pd.Series)
    # m = pd.concat([m.drop('mesh_terms', axis=1), dict_df], axis=1)
    # m.rename(columns={'mesh_heading':'value', 'mesh_id': 'category_id', 'qualifier_name':'qualifier'}, inplace=True)
    # m['type'] = 'mesh_terms'
    # dm = pd.concat([d, m], ignore_index=True)
    # dm['type'] = dm['type'].astype('category')
    # sql_code = "CREATE TABLE project.{}categories AS SELECT * FROM dm;".format(label)
    # conn.execute(sql_code)
    # # conn.sql("select type, count(*) from project.categories group by type;")  # check DuckDB table
    sql_query = "SELECT * from project.{}categories;".format(label)
    dm = conn.execute(sql_query).fetchdf()
    categories_openalex = generate_categories_oaconcepts(dm, rec_id, source_baseline_version)
    sql_code = "CREATE TABLE project.{}categories_openalex_concepts AS SELECT * FROM categories_openalex;".format(label)
    conn.execute(sql_code)
    # categories_openalex = generate_categories_oatopics(dm, rec_id, source_baseline_version)
    # sql_code = "CREATE TABLE project.{}categories_openalex_topics AS SELECT * FROM categories_openalex;".format(label)
    # conn.execute(sql_code)
    # print("\t\t table_categories, table_categories_openalex_concepts, table_categories_openalex_topics ")
    # conn.sql("select level, count(*) from project.categories_openalex_topics group by level;")  # check DuckDB table
    # t = pd.read_pickle('/Users/jfd/Documents/sciencedatanexus/data/project_sandbox/project_sandbox_topics_nodes.pkl')
def create_table_contributors_id(aut_ids, conn, label):
    """Contributor ids table"""
    sql_code = "drop TABLE if exists project.{}contributors_id;".format(label)
    conn.execute(sql_code)
    aut_ids = aut_ids.explode('ids')
    dict_df = aut_ids['ids'].apply(pd.Series)
    aut_ids = pd.concat([aut_ids.drop('ids', axis=1), dict_df], axis=1)
    aut_ids['type'] = aut_ids['type'].astype('category')
    aut_ids = aut_ids[['contribution_id', 'type', 'value']]
    aut_ids.dropna(how='any', inplace=True) # remove authorship with no aut_ids (about 0.5%)
    sql_code = "CREATE TABLE project.{}contributors_id AS SELECT * FROM aut_ids;".format(label)
    conn.execute(sql_code)
    print("\t\t table_contributors_id")
    # conn.sql("select type, count(*) from project.contributors_id group by type;")  # check DuckDB table
def create_table_organisations_id(org_ids, conn, label):
    """Organisations ids table"""
    sql_code = "drop TABLE if exists project.{}organisations_id;".format(label)
    conn.execute(sql_code)
    org_ids = org_ids.explode('ids')
    dict_df = org_ids['ids'].apply(pd.Series)
    org_ids = pd.concat([org_ids.drop('ids', axis=1), dict_df], axis=1)
    org_ids['type'] = org_ids['type'].astype('category')
    org_ids = org_ids[['org_id', 'type', 'value']]
    org_ids.dropna(how='any', inplace=True)
    sql_code = "CREATE TABLE project.{}organisations_id AS SELECT * FROM org_ids;".format(label)
    conn.execute(sql_code)
    print("\t\t table_organisations_id")
def generate_collaboration_network(rec_id, publications_df, network_sample_size=None):
    # rec_id: the unique record ID (e.g. lens_id)
    # publications_df: the list of records with affiliation data
    # Preparation step (subset the list of publications)
    # publications_df = publications_df[publications_df.lens_id == '000-024-593-676-416'].copy()
    if network_sample_size:
        # Get a sample of 1,000 unique values in the 'lens_id' column
        sampled_ids = publications_df[rec_id].drop_duplicates().sample(n=network_sample_size, random_state=42)
        # If you need the rows corresponding to these unique 'lens_id' values, use `isin()`
        publications_df = publications_df[publications_df[rec_id].isin(sampled_ids)]
    # Step 1: Group and reduce org_id to unique sets within each contribution
    d = (publications_df
         .groupby([rec_id, "contribution_id"])["org_id"]
         .agg(lambda orgs: tuple(set(orgs)))
         .reset_index())
    nb_authors = d.groupby(by=[rec_id])['contribution_id'].agg(nb_authors=("nunique"))
    # Step 2: Generate all pairs of contributions per rec_id
    contribution_pairs = (d.groupby(rec_id)["contribution_id"]
                          .apply(lambda orgs: list(combinations(orgs, 2)))
                          .explode()
                          .dropna()
                          .reset_index())
    # Step 3: Split pairs into 'from' and 'to' columns
    contribution_pairs[["from", "to"]] = pd.DataFrame(contribution_pairs["contribution_id"].tolist(), index=contribution_pairs.index)
    contribution_pairs.rename(columns={"contribution_id": "contribution_pairs"}, inplace=True)

    # Step 4: Merge with original grouped DataFrame 'd' for 'from' and 'to' orgs
    contribution_pairs = contribution_pairs.merge(d, left_on=[rec_id, "from"], right_on=[rec_id, "contribution_id"], suffixes=("", "_from"))
    contribution_pairs = contribution_pairs.merge(d, left_on=[rec_id, "to"], right_on=[rec_id, "contribution_id"], suffixes=("", "_to"))

    # Step 5: Rename columns and check for external organization connections
    contribution_pairs.rename(columns={"org_id": "from_org", "org_id_to": "to_org"}, inplace=True)
    contribution_pairs['is_ext'] = contribution_pairs.apply(lambda x: set(x["from_org"]).isdisjoint(x["to_org"]), axis=1)

    # Step 6: Generate unique pairs and weights
    contribution_pairs = contribution_pairs.explode("from_org").explode("to_org")
    contribution_pairs['pair'] = contribution_pairs.apply(lambda x: tuple(sorted([x["from_org"], x["to_org"]])), axis=1)
    pair_counts = contribution_pairs.groupby([rec_id, "contribution_pairs"])['pair'].transform('nunique')
    contribution_pairs['nb_pairs'] = pair_counts
    contribution_pairs = contribution_pairs.join(nb_authors, on=rec_id, how='left')
    contribution_pairs['weight'] = contribution_pairs.apply(lambda x: math.comb(x['nb_authors'], 2) * x['nb_pairs'] if x['nb_pairs'] > 1 else math.comb(x['nb_authors'], 2),
    axis=1)

    # Step 7: Normalize weights and aggregate
    o = (contribution_pairs
         .groupby([rec_id, "pair", 'is_ext'])
         .agg(weight=("weight", "sum"))
         .reset_index())
    # Step 8: Expand pair column and aggregate final weights
    dict_df = o['pair'].apply(pd.Series)
    o = (pd.concat([o, dict_df], axis=1)
         .rename(columns={0: "from", 1: "to"})
        #  .groupby(['from', 'to', 'is_ext']).agg(weight=('weight', 'sum'))
        #  .reset_index()
        #  .sort_values(by="weight", ascending=False)
        )
    # Collaborations through joint-appointments are removed
    o = o[(o.is_ext) | (o['to'] == o['from'])]
    # Normalize weights by rec_id
    o['weight'] /= o.groupby(rec_id)['weight'].transform("sum")

    o = (o[(o.is_ext) | (o['to'] == o['from'])]
         .groupby(['from', 'to', 'is_ext']).agg(weight=('weight', 'sum'))
         .reset_index()
         .sort_values(by="weight", ascending=False)
        )

    # Step 9: Create nodes DataFrame
    list_nodes = sorted(set(o['from']).union(o['to']))
    nodes = pd.DataFrame(list_nodes, columns=['org_id'])

    return o, nodes
def create_table_contribution_information(df, rec_id, conn, label, source_baseline_version, net_sample):
    """Contributor table"""
    sql_code = "drop TABLE if exists project.{}net_org_edges;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}net_org_nodes;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}organisations;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}contribution;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}affiliation;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}locations;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}net_org_edges;".format(label)
    conn.execute(sql_code)
    sql_code = "drop TABLE if exists project.{}net_org_nodes;".format(label)
    conn.execute(sql_code)    
    d = df[[rec_id, 'authors']].copy()
    d = d.explode('authors').reset_index(drop=True)
    d['author_position'] = d.groupby(rec_id).cumcount() + 1
    dict_df = d['authors'].apply(pd.Series)
    d = pd.concat([d.drop('authors', axis=1), dict_df], axis=1)
    d = d.reset_index().rename(columns={'index': 'contribution_id'})
    ids = d.copy()[['contribution_id', 'ids']]
    aff = d.copy()[['contribution_id','affiliations']]
    d['nb_ids'] = d['ids'].apply(len)
    d.drop(columns=['ids', 'affiliations'], inplace=True)
    # create_table_contributors_id(ids, conn, label)
    """Affiliation table"""
    aff.dropna(how='any', inplace=True) # remove contribution with no affiliations (usually doesn't happen)
    aff = aff.explode('affiliations')
    aff['affiliations'] = aff['affiliations'].mask(aff.affiliations.isna(), [{'name': 'unknown', 'ids': np.nan, 'grid_id': np.nan, 'country_code': '??', 'name_original': 'unknown'}])
    dict_df = aff['affiliations'].apply(pd.Series)
    aff = pd.concat([aff.drop('affiliations', axis=1), dict_df], axis=1)
    aff = aff.reset_index(drop=True).reset_index().rename(columns={'index': 'affiliation_id'})
    aff['country_code'] = aff['country_code'].mask(aff.country_code.isna(), '??')
    aff['grid_id'] = aff['grid_id'].mask(aff.grid_id.isna(), '??')
    """Organisations table"""
    org = aff[['name', 'country_code', 'grid_id', 'ids']].drop_duplicates(subset=['name', 'country_code','grid_id'])
    org['country_code'] = org['country_code'].astype('category')
    org = org.reset_index(drop=True).reset_index().rename(columns={'index': 'org_id'})
    org['nb_ids'] = org['ids'].apply(lambda x: len(x) if isinstance(x, list) else 0)
    ids = org[['org_id', 'ids']]
    org.drop(columns=['ids'], inplace=True)
    aff = aff.merge(org, on=['name', 'country_code', 'grid_id'], how='left') # add org_id to the affiliations table
    aff = aff[['affiliation_id', 'contribution_id', 'org_id']]
    g = aff.groupby('contribution_id').agg(nb_affiliations=("affiliation_id", 'nunique')) # add the nb of affiliations to the authorship table
    d = d.merge(g, on='contribution_id', how='left')
    # g = d.groupby(rec_id).agg(nb_authors=("contribution_id", 'nunique')) # add the nb of authorships to the records table
    org.drop(columns=['grid_id'], inplace=True)  # export the orgnaisation table
    org['country_code'] = org['country_code'].astype('category')
    """ Creation of the Organisations_id table"""
    create_table_organisations_id(ids, conn, label)
    # conn.sql("select type, count(*) as n from project.organisations_id group by type;")  # check DuckDB table
    sql_code = "CREATE TABLE project.{}organisations AS SELECT * FROM org;".format(label)
    conn.execute(sql_code)
    # conn.sql("select country_code, count(*) as n from project.organisations group by country_code order by n Desc limit 10;")  # check DuckDB table    
    """Import ROR information to add to the Organisations table """
    ids = conn.sql("SELECT * FROM project.organisations_id;").fetchdf()
    conn_bas = duckdb.connect(source_baseline_version, read_only=True) 
    query = "SELECT B.id, A.type, A.value  FROM baselines.ror_external_id A inner join baselines.ror B on A.id = B.id where B.status = 'active';"
    ror_id = conn_bas.execute(query).fetchdf()
    loc = ids.merge(ror_id, on=['type', 'value'],how="left")
    query = "SELECT * FROM baselines.ror_location A inner join baselines.ror_location_id B on A.geonames_id=B.geonames_id;"
    geo = conn_bas.execute(query).fetchdf()
    loc = loc.merge(geo, on='id').drop_duplicates(subset=['org_id', 'geonames_id'])
    loc = loc[['org_id', 'id', 'geonames_id', 'name', 'country_code', 'country_name', 'lat', 'lng']]
    sql_code = "CREATE TABLE project.{}locations AS SELECT * FROM loc;".format(label)
    conn.execute(sql_code)
    # conn.sql("select country_code, count(distinct geonames_id) as n from project.locations group by country_code order by n Desc limit 10;")  # check DuckDB table    
    """Save all the contribution related tables"""
    sql_code = "CREATE TABLE project.{}contribution AS SELECT * FROM d;".format(label)
    conn.execute(sql_code)
    # conn.sql("select count(*) as n from project.contribution;")  # check DuckDB table
    sql_code = "CREATE TABLE project.{}affiliation AS SELECT * FROM aff;".format(label)
    conn.execute(sql_code)
    # conn.sql("select count(*) as n from project.affiliation;")  # check DuckDB table
    """ Add network data (edges, nodes) """
    aff = conn.sql("SELECT * FROM project.affiliation;").fetchdf()
    org = conn.sql("SELECT * FROM project.organisations;").fetchdf()
    df = conn.sql("SELECT {}, contribution_id FROM project.contribution".format(rec_id)).fetchdf()
    publications_df = df.merge(aff, on="contribution_id", how='left')
    df_e, df_n = generate_collaboration_network(rec_id, publications_df, network_sample_size=net_sample)
    df_n = df_n.merge(org, on='org_id', how='inner')
    sql_code = "CREATE TABLE project.{}net_org_nodes AS SELECT * FROM df_n;".format(label)
    conn.execute(sql_code)
    sql_code = "CREATE TABLE project.{}net_org_edges AS SELECT * FROM df_e;".format(label)
    conn.execute(sql_code)
    conn_bas.close()
    print("\t\t table_contribution_information with network data")
    # conn.sql("select country_code, count(*) as n from project.organisations group by country_code order by n Desc limit 10;")  # check 
def create_table_funding(df, rec_id, conn, label):
    """External ids table"""
    d = df[[rec_id, 'funding']].copy()
    d = d.explode('funding')
    dict_df = d['funding'].apply(pd.Series)
    d = pd.concat([d.drop('funding', axis=1), dict_df], axis=1)
    d['org'] = d['org'].astype('category')
    d['country'] = d['country'].astype('category')
    sql_code = "CREATE TABLE project.{}funding AS SELECT * FROM d;".format(label)
    conn.execute(sql_code)
    # conn.sql("select count(*) from project.records_id;")  # check DuckDB table
    print("\t\t table_funding")
def create_ddb(infile, outfile, project_variant_string, source_baseline_version, source_data="lens_scholarly", network_sample_size=None):
    # infile = a pandas DF with raw data from xml or API for records (eg Lens, OpenAlex)
    # outfile = a DuckDB (.duckdb) DB
    # uid = the label of the header which contains the records unique identifiers (eg: lens_id, openalex)
    try:
        print('\t start data export to DDB')
        """database setup"""
        conn = duckdb.connect(outfile)
        sql_code = "CREATE SCHEMA IF NOT EXISTS project;"
        conn.execute(sql_code)
        if source_data=="lens_scholarly":
            uid = "lens_id"
            df = pd.read_pickle(infile)
            """ Data cleaning"""
            def_source = [
                'source.title',
                'source.publisher',
                'source.issn',
                'source.type',
                'source.country'
                ]
            for i in def_source:
                df[i] = df[i].fillna('other')
            """ CSV source adaptation """
            # df = pd.read_csv(infile, sep="|")
            # for i in ['funding', 'authors', 'mesh_terms', 'fields_of_study','source.issn', 'external_ids']:
            #     df[i] = df[i].fillna("[]")
            # df['funding'] = df['funding'].apply(ast.literal_eval)  # CSV source adaptation
            # df['authors'] = df['authors'].apply(ast.literal_eval)  # CSV source adaptation
            # df['mesh_terms'] = df['mesh_terms'].apply(ast.literal_eval)  # CSV source adaptation
            # df['fields_of_study'] = df['fields_of_study'].apply(ast.literal_eval)  # CSV source adaptation
            # df['source.issn'] = df['source.issn'].apply(ast.literal_eval)  # CSV source adaptation
            # df['external_ids'] = df['external_ids'].apply(ast.literal_eval)  # CSV source adaptation
            # infile = infile.replace(".txt", ".pkl")
            # df.to_pickle(infile)
            """ CSV source adaptation """
            # create_table_records_id(df, uid, conn, project_variant_string)
            # list_source = create_table_source(df, def_source, conn, project_variant_string)
            # create_table_records(df, list_source, uid, conn, project_variant_string)
            create_table_categories(df, uid, conn, project_variant_string, source_baseline_version)
            # create_table_contribution_information(df, uid, conn, project_variant_string, source_baseline_version,network_sample_size)
            # create_table_funding(df, uid, conn, project_variant_string)
        elif source_data == "lens_patents":
            uid = "lens_id"
            print("\t Lens patents data not implemented yet")
        else:
            print("\t No source selected (eg Lens, OpenAlex)")
        """ Final code """
        conn.close()
    except Exception as e:
        print(e)
    finally:
        print('\t data exported to the DB in {}'.format(outfile))


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
