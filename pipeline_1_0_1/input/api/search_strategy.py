# coding=utf-8

# =============================================================================
# """
# .. module:: input_pipeline.api.search_strategy.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus, 2024
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 09/12/2024
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
import yaml
import pandas as pd

# =============================================================================
# Functions and classes
# =============================================================================

def load_search_strategy(search_strategy_file):
    """
    Load search_strategy_file as a YAML file:
    lens_scholarly:
            main:
                topics:
                - topic_1:
                        name: FR and AU collaborations
                        value: {
                                "bool": {
                                    "must": [{"range": {"year_published": {"gte": "2015", "lte": "2024"}}},
                                        {"bool": {"must":[
                                            {"term": {"author.affiliation.address.country_code": "AU"}},
                                            {"term": {"author.affiliation.address.country_code": "FR"}}
                                        ]}}
                                    ]
                                }
                            }
            secondary:
                topics:
                - topic_2: 
                        name: Australian publications (AU)
                        value: {
                        "bool": {
                            "must": [{"range": {"year_published": {"gte": "2015", "lte": "2024"}}},
                                {"bool": {"must":[
                                    {"term": {"author.affiliation.address.country_code": "AU"}}
                                ]}}
                            ]
                        }
                    }
            aggegation: 
                aggegations: 
                    - agg_1: 
                            name: Agg year_published
                            value: {"year_published_histogram": 
                                        {"date_histogram": {"field": "date_published", "interval": "YEAR"}}
                                    }
    Output: a pandas dataframe 
                    source    category       type       id                          name                                              value
        0   lens_scholarly        main      topic  topic_1      FR and AU collaborations  {'bool': {'must': [{'range': {'year_published'...
        1   lens_scholarly   secondary      topic  topic_2  Australian publications (AU)  {'bool': {'must': [{'range': {'year_published'...
        2   lens_scholarly   secondary      topic  topic_3      French publications (FR)  {'bool': {'must': [{'range': {'year_published'...
        3   lens_scholarly  aggegation  aggregate    agg_1            Agg year_published  {'year_published_histogram': {'date_histogram'...

    """
    try:
        with open(search_strategy_file, 'r') as f:
            data = yaml.safe_load(f)

        # Extract topics and aggregations from all data
        flattened_data = []

        # Helper function to flatten topics and aggregations
        def extract_topics_aggregations(source, source_name):
            for category, category_data in source.items():
                if "topics" in category_data:
                    for topic in category_data["topics"]:
                        for topic_id, topic_details in topic.items():
                            flattened_data.append({
                                "source": source_name,
                                "category": category,
                                "type": "topic",
                                "id": topic_id,
                                "name": topic_details.get("name"),
                                "value": topic_details.get("value"),
                            })
                if "aggegations" in category_data:
                    for agg in category_data["aggegations"]:
                        for agg_id, agg_details in agg.items():
                            flattened_data.append({
                                "source": source_name,
                                "category": category,
                                "type": "aggregate",
                                "id": agg_id,
                                "name": agg_details.get("name"),
                                "value": agg_details.get("value"),
                            })
                if "thesaurus" in category_data:
                    for agg in category_data["thesaurus"]:
                        for agg_id, agg_details in agg.items():
                            flattened_data.append({
                                "source": source_name,
                                "category": category,
                                "type": "thesaurus",
                                "id": agg_id,
                                "name": agg_details.get("name"),
                                "value": agg_details.get("value"),
                            })

        # Process each source
        for source_name, source_data in data.items():
            extract_topics_aggregations(source_data, source_name)

        # Convert the flattened data to a DataFrame
        df = pd.DataFrame(flattened_data)
    except Exception as e:
        print(e)
    finally:
        print('\t data exported to a Dataframe')
        return df


# =============================================================================
# End of script
# =============================================================================

