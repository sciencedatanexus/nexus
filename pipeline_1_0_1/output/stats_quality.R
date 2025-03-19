# ==============================================================================
# Title:        Record quality calculation
# Description:  calculates the quality of a record based on all dimensions of quality
#
# Author:       Jean-Francois Desvignes (contact@sciencedatanexus.com)
# Date:         2025-26-02
# Version:      1.1.0
# License:      MIT
#
# Input:        - "[PROJECT].duckdb": The raw data.
#               - "config/cleaning_params.yaml": Configuration file with parameters
#                 for data cleaning.
#
# Output:       - "stats_quality": a data table
#
# Dependencies: - R version 4.4.1
#               - tidyverse (1.3.1)
#               - ggplot2 (3.3.5)
#
# Usage:        Run the script with RStudio or Positron, or in the terminal as:
#               Rscript source(script.R) and run each function
#
# Notes:        This script is part of a Science Data Nexus project aimed at
#               improving data-driven insights from Open Science information.
#               Please cite the associated work if using this code.
# ==============================================================================

# Load required libraries
library(dplyr)

stats_quality <- function(
    data_file,
    input_years_evolution=c(project_start_year, project_end_year)
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
  # sql_code <- "SELECT type, count(*) FROM project.categories group by type;"
  # dbGetQuery(conn, sql_code)


  # Preview the first few rows of the data
  # head(data)

  # Diconnect to DB
  dbDisconnect(conn, shutdown = TRUE)

  # ==============================================================================
  # 3. Data Cleaning
  # ==============================================================================

  # Remove any rows with missing values


  # Standardize column names (lowercase, replace spaces with underscores)
  # cleaned_data <- cleaned_data %>%
  #   rename_all(~ tolower(gsub(" ", "_", .)))

  # Write the cleaned data to a file
  # write_csv(cleaned_data, "output/cleaned_data.csv")

  # ==============================================================================
  # 4. Data Analysis
  # ==============================================================================



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

  # Display the plot
  # print(eda_plot)

  # Save the plot to a file
  # ggsave("output/eda_plot.png", plot = eda_plot)

  # ==============================================================================
  # 6. Output Generation
  # ==============================================================================
  eda_plot <- NULL
  data_table <- NULL
  disciplines_nb <- NULL
  data_stats <- tibble(
    distance_type = c("High", "Average", "Unknown"),
    nb_orgs = c(10, 50, 40)
  )
  # If there are additional output steps, they would go here. For example, saving
  # more visualizations, or generating a report.
  return(list(plot = eda_plot, table = data_table, hero=disciplines_nb, stats = data_stats))
  # Diconnect to DB (at the end to prevent Connection to be garbage-collected)
  # dbDisconnect(conn, shutdown = TRUE)
  # Final message (for debug)
  # cat("Analysis complete. Check the 'output' folder for results.\n")
}
