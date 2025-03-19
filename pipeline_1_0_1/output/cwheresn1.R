# ==============================================================================
# Title:        Plot research capacity by location (country)
# Description:  This script reads research paper over time (years) data from an input_pipeline DB, cleans the dataset, performs
#               exploratory data analysis (EDA), and outputs summary statistics
#               and visualizations.
#
# Author:       Jean-Francois Desvignes
# Date:         2024-10-15
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
rplot_cwheresn1 <- function(
  data_file,
  input_years_evolution=c(project_start_year, project_end_year),
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

# retrieve the items again
# sql_code <- "SELECT * FROM project.records limit 5;"
# dbGetQuery(conn, sql_code)

data1 <- tbl(conn, 'project.records') %>%
  filter_scholarly_records(input_years_evolution[1], input_years_evolution[2]) %>%
  select(c('lens_id', 'nb_authors', 'year_published'))
data2 <- tbl(conn, 'project.contribution') %>%
  select(c('lens_id', 'contribution_id', 'nb_affiliations'))
data3 <- tbl(conn, 'project.affiliation') %>%
  select(c('contribution_id', 'affiliation_id', "org_id"))
data4 <- tbl(conn, 'project.organisations') %>%
  select(c('org_id', "country_code"))
data <- data1 %>%
  left_join(data2, by = 'lens_id') %>%
  left_join(data3, by = 'contribution_id') %>%
  left_join(data4, by = "org_id") %>%
  group_by(country_code) %>%
  summarise(
    nb_records = n_distinct(lens_id),
    avg_nb_authors = round(mean(nb_authors, na.rm=TRUE), digits = 1),
    median_nb_authors = round(median(nb_authors, na.rm = TRUE), digits = 1),
    avg_year = round(mean(year_published, na.rm = TRUE), digits = 1),
    .groups = 'drop_last' # `summarise()` has grouped output by "year_published". You can override using the `.groups` argument.
  ) %>%
  mutate(share_records = (nb_records / sum(nb_records)) * 100) %>%
  arrange(desc(nb_records)) %>%
  collect()
  # mutate(year_published = as.factor(year_published))

# Preview the first few rows of the data
# head(data)


# ==============================================================================
# 3. Data Cleaning
# ==============================================================================

# Remove any rows with missing values

cleaned_data <- data %>%
  drop_na() %>%
  mutate(country = countrycode(country_code, origin = "iso2c", destination = "country.name", custom_match=c('??' = 'unknown')))

# Standardize column names (lowercase, replace spaces with underscores)
# cleaned_data <- cleaned_data %>%
#   rename_all(~ tolower(gsub(" ", "_", .)))

# Write the cleaned data to a file
# write_csv(cleaned_data, "output/cleaned_data.csv")

# ==============================================================================
# 4. Data Analysis
# ==============================================================================

# Generate summary statistics for the cleaned data
# summary_stats <- cleaned_data %>%
#   summarise(
#     avg_age = mean(age, na.rm = TRUE),
#     median_income = median(income, na.rm = TRUE),
#     survey_count = n()
#   )

# Print the summary statistics to the console
# print(summary_stats)

# Save summary statistics to a text file
# write.table(summary_stats, file = "summary_statistics.txt", sep = "\t")

# ==============================================================================
# 5. Exploratory Data Analysis (EDA)
# ==============================================================================
# Total number of observations
total_nb <- format(sum(cleaned_data$nb_records), big.mark = ",", scientific = FALSE)
# Create a simple bar plot of customer satisfaction levels
# eda_plot <- ggplot()
top10_data <- cleaned_data %>%
    arrange(desc(nb_records)) %>%
    slice_head(n = 10)

  eda_plot <- ggplot(top10_data, aes(x= reorder(country, nb_records), y= nb_records, fill=country)) +
    geom_bar(stat = "identity") +
    coord_flip() +
    scale_y_comma() +
    labs(
      x = "Countries or regions",
      y = "Count of records",
      title = "Largest countries by number of scientific publications from affiliated researchers.",
      subtitle= paste0("Estimates the countries that support a scientific capability (N: ", total_nb, ")."),
      caption="Brought to you by The Lens.org data."
    )+
    theme_ipsum()

  countries <- sf::read_sf(file.path(baseline_directory, "countries.geojson"))
  country_data <- countries %>%
    inner_join(cleaned_data, by = c("ISO_A2" = "country_code"))
  max_rec <- max(country_data$nb_records)
  bins <- c(
    1,
    ceiling(max_rec/10),
    ceiling(max_rec/5),
    ceiling(max_rec/2),
    max_rec
  )
  pal <- colorBin("Oranges", domain = country_data$nb_records, bins = bins)
  labels <- sprintf(
    "<strong>%s</strong><br/>%s records",
    country_data$ADMIN, format(country_data$nb_records, big.mark = ",", scientific = FALSE)
  ) %>% lapply(htmltools::HTML)
  eda_map <- leaflet(country_data) %>%
    addTiles() %>%
    addProviderTiles(providers$CartoDB.Positron) %>%  # list at https://rstudio.github.io/leaflet/reference/providers.html
    setView(lng = 135, lat = -20, zoom = 2) %>%
    addPolygons(
        fillColor = ~pal(nb_records),
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
        )  %>%
        leaflet::addLegend(pal = pal, values = ~nb_records, opacity = 0.7, title = NULL,
        position = "bottomright") ## needs the latest version of xts to work (Error in get: object '.xts_chob' not found)

  # # %>%
    # addMarkers(
    #   rand_lng(), rand_lat(), popup = paste("A random letter", sample(LETTERS, 10))
    # ) %>%
    # addCircleMarkers(~lng, ~lat, radius = ~size,
    #   color = ~RdYlBu(category), fillOpacity = 0.5)

# Display the plot
# print(eda_plot)

# Save the plot to a file
# ggsave("output/eda_plot.png", plot = eda_plot)

# ==============================================================================
# 6. Output Generation
# ==============================================================================
data_table <- as.data.frame(country_data) %>%
  select(ADMIN, nb_records) %>%
  arrange(desc(nb_records)) %>%
  rename(
      "Country"="ADMIN",
      "Count of records"="nb_records"
      )
# If there are additional output steps, they would go here. For example, saving
# more visualizations, or generating a report.
return(list(plot = eda_map, table = data_table , hero=total_nb))
# Diconnect to DB (at the end to prevent Connection to be garbage-collected)
dbDisconnect(conn, shutdown = TRUE)
# Final message (for debug)
# cat("Analysis complete. Check the 'output' folder for results.\n")
}
# result <- rplot_cwheresn1(data_file, c(2010,2024), ui_font_size)
# result$plot
