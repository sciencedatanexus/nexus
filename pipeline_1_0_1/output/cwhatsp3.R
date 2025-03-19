# ==============================================================================
# Title:        Plot Share of research by scientific journal 
# Description:  This script reads research paper data from an input_pipeline DB, cleans the dataset, performs
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
rplot_cwhatsp3 <- function(
  data_file, 
  input_years_evolution=c(project_start_year, project_end_year),
  ui_font_size = 14
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
  # sql_code <- "SELECT * FROM project.source limit 5;"
  # dbGetQuery(conn, sql_code)
  data1 <- tbl(conn, 'project.records') %>%
    # mutate(source_id = as.integer64(source_id)) %>%
    filter_scholarly_records(input_years_evolution[1], input_years_evolution[2]) %>%
    select(c('source_id', 'nb_authors', 'year_published'))
  data2 <- tbl(conn, 'project.source') %>%
    select(c('source_id', 'title', 'country')) 
  data <- data1 %>%
    left_join(data2, by = 'source_id') %>%
    group_by(title) %>%
    summarise(
      nb_records = n(),
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
  
  # Diconnect to DB
  # dbDisconnect(conn, shutdown = TRUE)
  
  # ==============================================================================
  # 3. Data Cleaning
  # ==============================================================================
  
  # Remove any rows with missing values
  
  cleaned_data <- data %>%
    drop_na()
  
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
  total_nb <- format(n_distinct(cleaned_data$title), big.mark = ",", scientific = FALSE)
  # Create a simple bar plot of customer satisfaction levels
  top10_data <- cleaned_data %>%
    arrange(desc(nb_records)) %>%
    slice_head(n = 10)  
  
  eda_plot <- ggplot(top10_data, aes(x= reorder(title, share_records), y= share_records, fill=title)) +
    geom_bar(stat = "identity") +
    geom_text(aes(label = nb_records), hjust = 1.1) + 
    coord_flip() +
    scale_y_comma() +
    # scale_fill_ipsum() + 
    scale_fill_viridis_d() +
    labs( 
      x = "Journal", 
      y = "Share of records (%)",
      title = "Spread of scientific publications by their source (journals)",
      subtitle= paste0("Estimates communities of practice that underpins a scientific capability (N: ", total_nb, ")."),
      caption="Brought to you by The Lens.org data."
    )+
    theme(legend.position = "none")
  eda_plot <- ggplot_theme_style(eda_plot, ui_font_size)
  # Display the plot
  # print(eda_plot)
  
  # Save the plot to a file
  # ggsave("output/eda_plot.png", plot = eda_plot)
  
  # ==============================================================================
  # 6. Output Generation
  # ==============================================================================
  data_table <- cleaned_data %>%
    select(title, share_records, nb_records, avg_year, avg_nb_authors, median_nb_authors) %>%
    arrange(desc(nb_records)) %>%
    mutate(
        avg_nb_authors = round(avg_nb_authors, 1),
        avg_year = round(avg_year, 1),
        share_records = round(share_records, 1)
          ) %>%
    rename(
        "Journal"="title",
        "Share of records (%)"="share_records",
        "Count of records"="nb_records",
        "Average year of publication"="avg_year",
        "Average number of contributors by document"="avg_nb_authors",
        "Median number of contributors by document"="median_nb_authors"
        )
  # If there are additional output steps, they would go here. For example, saving
  # more visualizations, or generating a report.
  return(list(plot = eda_plot, table = data_table, hero=total_nb))
  # Diconnect to DB (at the end to prevent Connection to be garbage-collected)
  dbDisconnect(conn, shutdown = TRUE)
  # Final message (for debug)
  # cat("Analysis complete. Check the 'output' folder for results.\n")
}