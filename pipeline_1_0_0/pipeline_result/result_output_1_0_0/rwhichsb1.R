# ==============================================================================
# Title:        Plot collaborations between organisations
# Description:  This script reads research paper data from an input_pipeline DB, cleans the dataset, performs
#               exploratory data analysis (EDA), and outputs summary statistics
#               and visualizations.
#
# Author:       Jean-Francois Desvignes
# Date:         2024-12-02
# Version:      1.0.0
# License:      MIT
#
# Input:        - "[PROJECT].duckdb": The raw data.
#               - "config/params.yaml": Configuration file with parameters
#                 for data operations.
#
# Output:       - plot: a plot for results
#               - table: the data underpinning the plot in a table format
#               - hero: an important value in a formatted format
#
# Dependencies: - R version 4.4.1
#               - tidyverse
#               - ggplot2 
#               - duckdb 
#               - ggplot2 
#               - duckplyr
#               - ghrbrthemes 
#
# Usage:        Run the script with RStudio or Positron, or in the terminal as:
#               Rscript [FILENAME].R
#
# Notes:        This script is part of a Science Data Nexus project aimed at
#               improving data-driven insights from Open Science information.
#               Please cite the associated work if using this code.
# ==============================================================================

# Load required libraries
# library(tidyverse)
# library(duckdb)

# ==============================================================================
# 0. function definition 
# ==============================================================================
rplot_rwhichsb1 <- function(
  data_file, 
  input_years_evolution=c(project_start_year, project_end_year),
  top_collaboration_limit=1,
  ui_font_size=14
) {
# ==============================================================================
# 1. Global variables
# ==============================================================================


# ==============================================================================
# 2. Data Loading
# ==============================================================================

# Load the data into a dataframe
# to use a database file (shared between processes)
conn <- dbConnect(duckdb(), dbdir = data_file, read_only = TRUE)

# List of publications with collaboration data
edges <- tbl(conn, 'project.net_org_edges') %>%
  # filter_scholarly_records(input_years_evolution[1], input_years_evolution[2]) %>%
  filter(is_ext == TRUE) %>%
  filter(if (!is.null(top_collaboration_limit)) weight >= top_collaboration_limit else TRUE) %>%
  collect()
# summary(edges)
nodes <- tbl(conn, 'project.net_org_nodes') %>% 
  collect()
# summary(nodes)
nodes <- nodes %>% 
  filter(org_id %in% unique(c(edges$from, edges$to)))
# summary(nodes)
nodes_map <- tbl(conn, 'project.locations') %>% 
  rename('city' = 'name') %>%
  collect()
nodes_map <- nodes_map %>%
  filter(org_id %in% unique(c(edges$from, edges$to))) %>%
  inner_join(select(nodes, org_id, name, nb_records, nb_contributions), by = 'org_id') %>%
  group_by(id) %>%
  ungroup()
edges_map <- edges %>% 
  filter((to %in% unique(nodes_map$org_id)) & (from %in% unique(nodes_map$org_id)))

# Diconnect to DB (at the end to prevent Connection to be garbage-collected)
dbDisconnect(conn, shutdown = TRUE)

# Preview the first few rows of the data
# head(data)

# ==============================================================================
# 3. Data Cleaning
# ==============================================================================
# Remove any rows with missing values

# ==============================================================================
# 4. Data Analysis
# ==============================================================================

# Generate summary statistics for the cleaned data
country_metrics <- nodes_map %>% group_by(country_code)%>%
  summarise(
    nb_org = n()
  )
countries <- sf::read_sf(file.path(baseline_directory, "countries.geojson"))
country_data <- countries %>%
  filter(ISO_A2 %in% unique(nodes_map$country_code)) %>% 
  rename('country_code'='ISO_A2') %>%
  inner_join(country_metrics, by='country_code')
# edges_loc <- edges_map %>%
#   inner_join(select(nodes_map, org_id, lat), by=c("from"="org_id")) %>%
#   inner_join(select(nodes_map, org_id, lat), by=c("to"="org_id"), suffix=c("_1", "_to")) %>%
#   inner_join(select(nodes_map, org_id, lng), by=c("from"="org_id")) %>%
#   inner_join(select(nodes_map, org_id, lng), by=c("to"="org_id"), suffix=c("_1", "_to")) %>%
#   mutate(lat = purrr::map2(lat_1, lat_to, c), lng = purrr::map2(lng_1, lng_to, c))

# ==============================================================================
# 5. Exploratory Data Analysis (EDA)
# ==============================================================================
# Total number of observations
total_nb <- format(nrow(edges_map), big.mark = ",", scientific = FALSE)
# total_nb = 100
# eda_plot <- network_visualization(nodes, edges)
# eda_plot <- network_visualization_map(nodes_map, edges_map)
# eda_plot <- network_visualization(organisations, cleaned_data, 10)
max_rec <- max(country_data$nb_org)
  min_value <- 1
bins <- c(
  min_value , 
  ceiling(max_rec/10)+min_value , 
  ceiling(max_rec/5)+min_value , 
  ceiling(max_rec/2)+min_value , 
  max_rec
)
pal <- colorBin("Oranges", domain = country_data$nb_org, bins = bins)
# print(bins)
labels <- sprintf(
  "<strong>%s</strong><br/>%s organisations",
  country_data$ADMIN, format(country_data$nb_org, big.mark = ",", scientific = FALSE)
) %>% lapply(htmltools::HTML)
labels_markers <- sprintf(
  "<strong>%s</strong><br/>%s records",
  nodes_map$name, format(nodes_map$nb_records, big.mark = ",", scientific = FALSE)
) %>% lapply(htmltools::HTML)
eda_plot <- leaflet(country_data) %>%
  addTiles() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%  # list at https://rstudio.github.io/leaflet/reference/providers.html
  setView(lng = 135, lat = -15, zoom = 2) %>%
  addMarkers(
    clusterOptions = markerClusterOptions(),
    data = nodes_map,
    label = labels_markers
  ) %>%
  # addPolylines(lng = nodes_map$lng, lat = nodes_map$lat)
  addPolygons(
      fillColor = ~pal(nb_org),
      weight = 0.5,
      opacity = 1,
      color = "grey",
      dashArray = "",
      fillOpacity = 0.6
      ,
      highlightOptions = highlightOptions(
        weight = 1,
        color = "#666",
        dashArray = "",
        fillOpacity = 0.7,
        bringToFront = TRUE),
      label = labels,
      labelOptions = labelOptions(
          style = list("font-weight" = "normal", padding = "3px 8px"),
          textsize = "15px",
          direction = "auto")
      ) %>%
      leaflet::addLegend(pal = pal, values = ~nb_org, opacity = 0.7, title = NULL,
      position = "bottomright") ## needs the latest version of xts to work (Error in get: object '.xts_chob' not found)
    
# Display the plot
# print(eda_plot)

# Save the plot to a file
# ggsave("output/eda_plot.png", plot = eda_plot)

# ==============================================================================
# 6. Output Generation
# ==============================================================================
data_table <- edges_map %>%
  select(from, to, weight) %>%
  arrange(desc(weight)) %>%
  mutate(
      weight = round(weight, 1)
      ) %>%
  inner_join(select(nodes_map, org_id, name, nb_records), by=c("from" = "org_id"), relationship = "many-to-many") %>%
  mutate(from =name) %>%
  select(-name) %>%
  rename("Total count of records by organisation 1" = "nb_records") %>%
  inner_join(select(nodes_map, org_id, name, nb_records), by=c("to" = "org_id"), relationship = "many-to-many") %>%
  mutate(to =name) %>%
  select(-name) %>%
  rename("Total count of records by organisation 2" = "nb_records") %>%
  rename(
      "Organisation 1"="from",
      "Organisation 2"="to",
      # "External collaboration"="is_ext",
      "Number of collaborations"="weight"
      )
# If there are additional output steps, they would go here. For example, saving
# more visualizations, or generating a report.
return(list(net = eda_plot, table = data_table, hero=total_nb))
# Diconnect to DB (at the end to prevent Connection to be garbage-collected)
dbDisconnect(conn, shutdown = TRUE)
# Final message (for debug)
# cat("Analysis complete. Check the 'output' folder for results.\n")
}