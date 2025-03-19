# ==============================================================================
# Title:        Network visualisation
# Description:  Creates a visualisation for a network of collaborations
#
# Author:       Jean-Francois Desvignes (contact@sciencedatanexus.com)
# Date:         2024-10-15
# Version:      1.0.0
# License:      MIT
#
# Input:        - "[PROJECT].duckdb": The raw data.
#               - "config/cleaning_params.yaml": Configuration file with parameters
#                 for data cleaning.
#
# Output:       - "network_plot": The ggplot object
#
# Dependencies: - R version 4.4.1
#               - tidyverse (1.3.1)
#               - ggplot2 (3.3.5)
#               - igraph
#               - visNetwork
#
# Usage:        Run the script with RStudio or Positron, or in the terminal as:
#               Rscript source(script.R) and run each function
#
# Notes:        This script is part of a Science Data Nexus project aimed at
#               improving data-driven insights from Open Science information.
#               Please cite the associated work if using this code.
# ==============================================================================

# Load required libraries
library(igraph)
library(visNetwork)
library(dplyr)

network_visualization <- function(nodes_list, edges_list, top_collaboration_limit=30) {
  # ==============================================================================
  # 1. Preprocess the data
  # ==============================================================================
  # Preprocess the data
  # publications: dataframe with columns 'lens_id', 'affiliation_id', 'org_id'
  # organisations: dataframe with columns 'org_id', 'name'
  # ==============================================================================
  # 2. Create edges based on publications
  # ==============================================================================
  # Aggregate the collaboration count between organizations
  # collaborations <- publications %>%
  #   select(lens_id, org_id) %>%
  #   distinct() %>%
  #   group_by(lens_id) %>%
  #   filter(n() > 1) %>% # Filter to publications with more than one organization (collaborations)
  #   summarize(collab_pairs = combn(org_id, 2, simplify = FALSE)) %>% # Generate all unique pairs of orgs per publication
  #   # unnest(collab_pairs) %>% # Expand pairs into rows
  #   # ungroup() %>%
  #   group_by(collab_pairs) %>%
  #   mutate(from = map_chr(collab_pairs, 1), to = map_chr(collab_pairs, 2)) %>% # Separate pairs into 'from' and 'to'
  #   count(from, to) %>% # Count each pair's occurrences
  #   rename(collaboration_count = n) %>%
  #   arrange(desc(collaboration_count)) %>%
  #   ungroup() %>%
  #   slice(1:top_collaboration_limit) # Select top collaborations
  # Filter for top 10 organizations
  # top_org_ids <- collaborations$org_id
  # top_org_ids <- unique(c(collaborations$from, collaborations$to))
  # Create edges based on publications
  # edges <- publications %>%
  #   filter(org_id %in% top_org_ids) %>%
  #   select(lens_id, org_id) %>%
  #   distinct() %>%
  #   group_by(lens_id) %>%
  #   filter(n() > 1) %>% # Filter to publications with more than one organization (collaborations)
  #   summarize(edges = combn(org_id, 2, simplify = FALSE)) %>%
    # unnest(cols = c(edges)) %>%
    # ungroup() %>%
    # mutate(from = map_chr(edges, 1), to = map_chr(edges, 2)) %>%
    # select(from, to)
  # ==============================================================================
  # 3. Prepare nodes data
  # ==============================================================================

  # nodes <- organisations %>%
  #   distinct(org_id, name) %>%
  #   rename(id = org_id, label = name)
  # ==============================================================================
  # 4. Create an igraph object
  # ==============================================================================
  # graph <- graph_from_data_frame(d = collaborations, vertices = nodes, directed = FALSE)
  graph <- graph_from_data_frame(edges_list, directed = FALSE)
  # ==============================================================================
  # 5. Prepare the network for visNetwork
  # ==============================================================================
  # vis_nodes <- data.frame(id = V(graph)$name, label = V(graph)$label)
  # vis_edges <- as_data_frame(graph, what = "edges")
  head(nodes)
  nodes = nodes_list %>% filter(org_id %in% V(graph)$name)
  # vis_nodes <- data.frame(id = V(graph)$name, label = V(graph)$name)
  vis_nodes <- data.frame(id= nodes$org_id, label = nodes$name, group = nodes$country_code)
  vis_edges <- data.frame(from = edges$from, to = edges$to, width = edges$weight)
  # ==============================================================================
  # 6. Render the visNetwork plot
  # ==============================================================================
    # Step 5: Render the visNetwork plot
    # visNetwork(vis_nodes, vis_edges) %>%
    #   visEdges(arrows = "to") %>%
    #   visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
    #   visLayout(randomSeed = 42)
  # Plot network with visNetwork
  visNetwork(vis_nodes, vis_edges, height = "100%", width = "100%") %>%
    visOptions(highlightNearest = TRUE, nodesIdSelection = FALSE) %>%
    # visOptions(highlightNearest = list(enabled =TRUE, degree = 2, hover = T), nodesIdSelection = FALSE) %>%
    # visIgraphLayout(layout = "layout_in_circle")
    # visIgraphLayout(layout = "layout_nicely")
    visNodes(shape = "dot", opacity=0.7) %>%
    visEdges(scaling = list(min = min(edges$weight), max = max(edges$weight)), color=list(opacity=0.4)) %>%
    visLayout(randomSeed = 123) %>%
    visInteraction(navigationButtons = TRUE, dragNodes = FALSE) %>%
    visLegend(width = 0.1, position = "right", main = "Country") %>%
    addFontAwesome()
  # If there are additional output steps, they would go here. For example, saving
  # more visualizations, or generating a report.

  # Final message
}

#     # Dummy data - replace with actual org_nodes and publications data
#     org_nodes <- data.frame(org_id = paste0("Org", 1:100))
#     publications <- data.frame(
#       lens_id = sample(1:50, 500, replace = TRUE),
#       org_id = sample(org_nodes$org_id, 500, replace = TRUE)
#     )
# network_visualization(org_nodes, publications, 30)
