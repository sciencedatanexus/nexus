# coding=utf-8

# =============================================================================
# """
# .. module:: pipeline.input.utils.utils_core.py
# .. moduleauthor:: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# .. version:: 1.0
#
# :Copyright: Jean-Francois Desvignes for Science Data Nexus
# Science Data Nexus, 2025
# :Contact: Jean-Francois Desvignes <contact@sciencedatanexus.com>
# :Updated: 14/02/2025
# """
# =============================================================================

# =============================================================================
# modules to import
# =============================================================================
import yaml
import os
import pandas as pd
# =============================================================================
# Functions and classes
# =============================================================================

def load_pipeline_config_file(file_path):
    """
    Load the project variables from the config/pipeline_config file
    """
    project_config =os.path.join(file_path, "config", "pipeline_config.yaml")
    with open(project_config, 'r') as f:
        data = yaml.safe_load(f)
    # Extract topics and aggregations from all data
    flattened_data = []

    # Helper function to flatten topics and aggregations
    def extract_type_aggregations(source, source_name):
        for category, category_data in source.items():
            if "thesaurus" in category_data:
                for agg in category_data["thesaurus"]:
                    for agg_id, agg_details in agg.items():
                        flattened_data.append({
                            "source": source_name,
                            "category": category,
                            "type": "unification",
                            "id": agg_id,
                            "name": agg_details.get("name"),
                            "value": agg_details.get("value"),
                        })

    # Process each source
    for source_name, source_data in data.items():
        extract_type_aggregations(source_data, source_name)

    # Convert the flattened data to a DataFrame
    df = pd.DataFrame(flattened_data)
    return df


# =============================================================================
# End of script
# =============================================================================
