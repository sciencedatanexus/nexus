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

  # ==============================================================================
  # 2. Create edges based on publications
  # ==============================================================================

  # ==============================================================================
  # 3. Prepare nodes data
  # ==============================================================================

  # ==============================================================================
  # 4. Create an igraph object
  # ==============================================================================
  graph <- graph_from_data_frame(edges_list, directed = FALSE)
  # ==============================================================================
  # 5. Prepare the network for visNetwork
  # ==============================================================================
  nodes = nodes_list %>% filter(org_id %in% V(graph)$name)
  vis_nodes <- data.frame(id= nodes$org_id, label = nodes$name, group = nodes$country_code)
  vis_edges <- data.frame(from = edges$from, to = edges$to, width = edges$weight)
  # ==============================================================================
  # 6. Render the visNetwork plot
  # ==============================================================================
  # Plot network with visNetwork
  # height = "100%",
  viz_net <- visNetwork(vis_nodes, vis_edges, width = "100%", height = "100%") %>%
    visOptions(highlightNearest = TRUE, nodesIdSelection = FALSE) %>%
    # visOptions(highlightNearest = list(enabled =TRUE, degree = 2, hover = T), nodesIdSelection = FALSE) %>%
    # visIgraphLayout(layout = "layout_in_circle")
    # visIgraphLayout(layout = "layout_nicely")
    visNodes(shape = "dot", opacity=0.7) %>%
    visEdges(scaling = list(min = min(edges$weight), max = max(edges$weight)), color=list(opacity=0.4)) %>%
    visLayout(randomSeed = 123) %>%
    visInteraction(navigationButtons = TRUE, dragNodes = FALSE) %>%
    # visLegend(width = 0.1, position = "right", main = "Country") %>%
    addFontAwesome()
  # If there are additional output steps, they would go here. For example, saving
  # more visualizations, or generating a report.
  # Final message
  return(viz_net)
}

network_visualization_map <- function(nodes_list, edges_list, top_collaboration_limit=30) {
  # ==============================================================================
  # 1. Preprocess the data
  # ==============================================================================
  # Preprocess the data
  # ==============================================================================
  # 2. Create edges based on publications
  # ==============================================================================

  # ==============================================================================
  # 3. Prepare nodes data
  # ==============================================================================

  # ==============================================================================
  # 4. Create an igraph object
  # ==============================================================================
  graph <- graph_from_data_frame(edges_list, directed = FALSE)
  # ==============================================================================
  # 5. Prepare the network for visNetwork
  # ==============================================================================
  nodes = nodes_list %>% filter(org_id %in% V(graph)$name)
  vis_nodes <- data.frame(id= nodes$org_id, label = nodes$name, group = nodes$country_code)
  vis_edges <- data.frame(from = edges$from, to = edges$to, width = edges$weight)
  # ==============================================================================
  # 6. Render the visNetwork plot
  # ==============================================================================
  # https://kateto.net/sunbelt2023.html#overlaying-networks-on-geographic-maps
  # https://plot.ly/r/lines-on-maps/
  # https://github.com/danwild/leaflet-network
  # https://luukvdmeer.github.io/sfnetworks/
  # https://tidygraph.data-imaginist.com/index.html
  # https://ggraph.data-imaginist.com/
  map <- get_map(location = c(lon = -100, lat = 40), zoom = 4)
  ggmap(map) +
  geom_point(aes(x = -100, y = 40), color = "red", size = 3)
  # png("my_plot.png", width = 3200, height = 2400)
  # maps::map("world2", col="grey20", fill=TRUE, bg="black", lwd=0.1)
  # points(x=nodes_map$lng, y=nodes_map$lat, pch=19,
  #   cex=nodes_map$nb_records/80, col="orange")
  # col.1 <- adjustcolor("orange red", alpha=0.4)
  # col.2 <- adjustcolor("orange", alpha=0.4)
  # edge.pal <- colorRampPalette(c(col.1, col.2), alpha = TRUE)
  # edge.col <- edge.pal(100)
  # for(i in 1:nrow(nodes_map))  {
  #   node1 <- nodes_map[nodes_map$org_id == edges_map[i,]$from,]
  #   node2 <- nodes_map[nodes_map$org_id == edges_map[i,]$to,]
  #   arc <- gcIntermediate( c(node1[1,]$lng, node1[1,]$lat),
  #                         c(node2[1,]$lng, node2[1,]$lat),
  #                         n=1000, addStartEnd=TRUE, breakAtDateLine = FALSE )
  #   edge.ind <- round(100*edges_map[i,]$weight / max(edges_map$weight))
  #     # Plot the arc segments
  #     if (is.list(arc)) {  # If the arc is split at the date line, plot each segment
  #       for (j in seq_along(arc)) {
  #         lines(arc[[j]], col = edge.col[edge.ind], lwd = edge.ind / 1)
  #       }
  #     } else {  # If no split, plot as a single line
  #       lines(arc, col = edge.col[edge.ind], lwd = edge.ind / 11)
  #     }
  #   # lines(arc, col=edge.col[edge.ind], lwd=edge.ind/30)
  # }
  # # Close the PNG device to save the file
  # dev.off()
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
