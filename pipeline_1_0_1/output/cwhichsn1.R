# ==============================================================================
# Title:        Plot research capability by funding organisation
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
rplot_cwhichsn1 <- function(
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
# collect the primary data
records <- tbl(conn, 'project.records') %>%
  select(c('lens_id', 'nb_authors', 'year_published'))
data2 <- tbl(conn, 'project.funding') %>%
  select(c('lens_id', 'org', 'funding_id', "country"))  %>%
  mutate(funding_id = ifelse(is.na(funding_id) & !is.na(org), "Unknown", funding_id)) %>%
  distinct(lens_id, org, funding_id, country)
data1 <- records %>%
  left_join(data2, by = 'lens_id') %>%
  group_by(lens_id) %>%
    mutate(nb_projects_recid = n()) %>%
  ungroup() %>%
  mutate(tagging = ifelse(nb_projects_recid > 1 & is.na(org), 1, 0)) %>% # remove NA funding in papers with multiple fundings
  filter(tagging == 0)
# calculations
data <- data1 %>%
  group_by(lens_id) %>%
    mutate(nb_projects_recid = n()) %>%
  ungroup() %>%
  group_by(org, country) %>%
  summarise(
    nb_records = sum(1/nb_projects_recid),
    nb_projects = n_distinct(funding_id),
    avg_nb_authors = round(mean(nb_authors, na.rm=TRUE), digits = 1),
    median_nb_authors = round(median(nb_authors, na.rm = TRUE), digits = 1),
    avg_year = round(mean(year_published, na.rm = TRUE), digits = 1),
    .groups = 'drop_last' # `summarise()` has grouped output by "year_published". You can override using the `.groups` argument.
  ) %>%
  ungroup() %>%
  mutate(share_records = (nb_records / sum(nb_records)) * 100) %>%
  mutate(share_projects = (nb_projects  / sum(nb_projects)) * 100) %>%
  arrange(desc(nb_projects)) %>%
  collect()
  # mutate(year_published = as.factor(year_published))

# Preview the first few rows of the data
# head(data)

# ==============================================================================
# 3. Data Cleaning
# ==============================================================================

# Remove any rows with missing values

cleaned_data <- data %>%
  drop_na(org)

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
total_nb  <- data1 %>% summarise(total = n_distinct(funding_id, org)) %>% collect()
total_nb <- format(total_nb$total, big.mark = ",", scientific = FALSE)
# Subtotal nb of records with funding
total <- sum(data$nb_records)
d <- data %>% filter(is.na(org))
empty <- sum(d$nb_records)
subtotal <- total - empty
# Create a simple bar plot
# eda_plot <- ggplot()
top10_data <- cleaned_data %>%
    arrange(desc(nb_projects)) %>%
    filter(nb_projects >= 10) %>%
    slice_head(n = 20)  %>%
    # mutate(country = ifelse(is.na(country) , "Other", country)) # replace NA in country
    mutate(country = factor(ifelse(is.na(country) , "Other", as.character(country)))) # replace NA in country


  eda_plot <- ggplot(top10_data, aes(x= reorder(org, nb_projects), y= nb_projects, fill=country)) +
    geom_bar(stat = "identity") +
    coord_flip() +
    scale_y_comma() +
    # scale_fill_ipsum() +
    scale_fill_viridis_d() +
    labs(
      x = "Funding organisations",
      y = "Count of projects",
      title = "Spread of the research projects by funding organsations",
      subtitle= paste0("Estimates the organisations that support a scientific capability (N: ", total_nb, " projects, from n:",subtotal," records)."),
      caption="Brought to you by The Lens.org data."
    )+
    theme(legend.position = "right")
  eda_plot <- ggplot_theme_style(eda_plot, ui_font_size)

# Display the plot
# print(eda_plot)

# Save the plot to a file
# ggsave("output/eda_plot.png", plot = eda_plot)

# ==============================================================================
# 6. Output Generation
# ==============================================================================
data_table <- cleaned_data %>%
  select(org, nb_projects, nb_records, share_projects, share_records, avg_nb_authors, median_nb_authors, avg_year) %>%
  arrange(desc(nb_projects)) %>%
  mutate(
      avg_nb_authors = round(avg_nb_authors, 1),
      nb_records = round(nb_records, 0),
      share_records = round(share_records , 1),
      share_projects = round(share_projects, 1)
        ) %>%
  rename(
      "Funding organisation/institute"="org",
      "Count of projects"="nb_projects",
      "Count of records"="nb_records",
      "Share of projects (%)"="share_projects",
      "Share of records (%)"="share_records",
      "Average number of contributors"="avg_nb_authors",
      "Median number of contributors by document"="median_nb_authors",
      "Average year of publication"="avg_year"
      )
# If there are additional output steps, they would go here. For example, saving
# more visualizations, or generating a report.
return(list(plot = eda_plot, table = data_table, hero=total_nb))
# Diconnect to DB (at the end to prevent Connection to be garbage-collected)
dbDisconnect(conn, shutdown = TRUE)
# Final message (for debug)
# cat("Analysis complete. Check the 'output' folder for results.\n")
}
