# ==============================================================================
# Title:        Dashboard helper functions
# Description:  These scripts standardise regular operations, notably fo data selection
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
# Output:       - "output/cleaned_data.csv": The cleaned dataset.
#               - "output/summary_stats.txt": Summary statistics.
#               - "output/plots/eda_plot.png": EDA visualizations.
#
# Dependencies: - R version 4.4.1
#               - tidyverse (1.3.1)
#               - ggplot2 (3.3.5)
#               - yaml (2.2.1)
#
# Usage:        Run the script with RStudio or Positron, or in the terminal as:
#               Rscript utils.R
#
# Notes:        This script is part of a Science Data Nexus project aimed at
#               improving data-driven insights from Open Science information.
#               Please cite the associated work if using this code.
# ==============================================================================

# Load required libraries
# library(tidyverse)
# library(duckdb)

# ==============================================================================
# 1. Select data from records
# ==============================================================================

filter_scholarly_records <- function(
  data, 
  min_year, 
  max_year
) {
  result <- data %>%
    filter(between(year_published, min_year, max_year))
  return(result)
}

ggplot_theme_style <- function(
  gg_plot, # ggplot object
  global_font_size # ui_font_size
) {
  gg_plot + 
    theme_ipsum(
      # base_family = "Arial Narrow",
      base_size = global_font_size, #11.5
      # plot_title_family = base_family,
      plot_title_size = global_font_size + 7, #18
      # plot_title_face = "bold",
      plot_title_margin = global_font_size -2, #10
      # subtitle_family = base_family,
      subtitle_size = global_font_size, #12
      # subtitle_face = "plain",
      subtitle_margin = global_font_size +3, #15
      # strip_text_family = base_family,
      strip_text_size = global_font_size, #12
      # strip_text_face = "plain",
      # caption_family = base_family,
      caption_size = global_font_size -3, #9
      # caption_face = "italic",
      caption_margin = global_font_size -2, #10
      # axis_text_size = base_size,
      # axis_title_family = subtitle_family,
      axis_title_size = global_font_size -3 #9
      # axis_title_face = "plain",
      # axis_title_just = "rt",
      # plot_margin = margin(30, 30, 30, 30),
      # grid_col = "#cccccc",
      # grid = TRUE,
      # axis_col = "#cccccc",
      # axis = FALSE,
      # ticks = FALSE
    )
  return(gg_plot)
}

# ==============================================================================
# 2. Capability
# ==============================================================================
 
# infile <- file.path(main_directory, "c2sn1.R")


# ==============================================================================
# 3. Data Cleaning
# ==============================================================================

# Remove any rows with missing values


# cleaned_data <- survey_data %>%
#   drop_na()

# Standardize column names (lowercase, replace spaces with underscores)
# cleaned_data <- cleaned_data %>%
#   rename_all(~ tolower(gsub(" ", "_", .)))

# Write the cleaned data to a CSV file
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
# write.table(summary_stats, file = "output/summary_statistics.txt", sep = "\t")

# ==============================================================================
# 5. Exploratory Data Analysis (EDA)
# ==============================================================================

# Create a simple bar plot of customer satisfaction levels
# eda_plot <- ggplot(cleaned_data, aes(x = satisfaction_level)) +
#   geom_bar(fill = "steelblue") +
#   labs(title = "Customer Satisfaction Levels", x = "Satisfaction Level", y = "Count")

# Display the plot
# print(eda_plot)

# Save the plot to a file
# ggsave("output/eda_plot.png", plot = eda_plot)

# ==============================================================================
# 6. Output Generation
# ==============================================================================

# If there are additional output steps, they would go here. For example, saving
# more visualizations, or generating a report.

# Final message
# cat("Analysis complete. Check the 'output' folder for results.\n")