# ==============================================================================
# Title:        Plot Share of research by domain and discipline (flow diagram)
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
rplot_cwhatsp1 <- function(
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
  # sql_code <- "SELECT type, count(*) FROM project.categories group by type;"
  # dbGetQuery(conn, sql_code)
  records <- tbl(conn, 'project.records') %>%
    filter_scholarly_records(input_years_evolution[1], input_years_evolution[2]) %>%
    select(c('lens_id'))

  data <- tbl(conn, 'project.categories_openalex_concepts') %>%
    inner_join(records, by='lens_id')

  data1 <- data %>%
    distinct(lens_id, is_not_linked, category_id, parent_1, parent_0, .keep_all = TRUE) %>%
    filter((!is.na(category_id)) & (is_not_linked == FALSE)) %>%
    filter("C" %in% parent_1 ) %>%
    distinct(lens_id, parent_0, display_name_0, parent_1, display_name_1) %>%
    group_by(lens_id) %>%
          mutate(
              nb_level_1 = n(),
              nb_level_0 = n_distinct(parent_0)
          )
  data1 <- data1 %>%
    group_by(parent_0, display_name_0, parent_1, display_name_1) %>%
      summarise(
        nb_1 = sum(1/nb_level_1, na.rm = TRUE),
        .groups = 'drop_last' # `summarise()` has grouped output. You can override using the `.groups` argument.
      ) %>%
    group_by(parent_0, display_name_0) %>%
      mutate(
        nb_0 = sum(nb_1, na.rm = TRUE)
      ) %>%
    collect() %>%
    ungroup() %>%  # Ungroup before calculating percentiles
    arrange(desc(nb_1))
  data_0 <- data1 %>%
    distinct(parent_0, display_name_0, nb_0) %>%
    mutate(percentile_0 = percent_rank(nb_0)) %>%
    arrange(desc(nb_0))
  data_1 <- data1 %>%
    group_by(parent_1, display_name_1) %>%
      summarise(nb_1_total = sum(nb_1), .groups = 'drop_last') %>%
    ungroup() %>%
    mutate(percentile_1 = percent_rank(nb_1_total)) %>%
    arrange(desc(nb_1_total))
  data1 <- data1 %>% inner_join(select(data_0, parent_0, percentile_0), by = "parent_0")
  data1 <- data1 %>% inner_join(select(data_1, parent_1, percentile_1), by = "parent_1")

  # Preview the first few rows of the data
  # head(data)

  # Diconnect to DB
  # dbDisconnect(conn, shutdown = TRUE)

  # ==============================================================================
  # 3. Data Cleaning
  # ==============================================================================

  # Remove any rows with missing values

  cleaned_data <- data1 %>%
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
  d <- data %>% filter(is_not_linked == TRUE) %>% collect()
  nb_missing_category <- n_distinct(select(d, lens_id))
  total_nb_dec <- n_distinct(select(data, lens_id) %>% collect())
  total_nb_dec <- total_nb_dec - nb_missing_category
  total_nb <- format(total_nb_dec, big.mark = ",", scientific = FALSE)

  # Diconnect to DB
  dbDisconnect(conn, shutdown = TRUE)

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
  cleaned_data <- cleaned_data %>%
    filter(percentile_1 > 0.95 & percentile_0 > 0.1)
  # Selected number of papers in these disciplines
  subtotal_nb <- format(ceiling(sum(cleaned_data$nb_1)), big.mark = ",", scientific = FALSE)
  # Selected Number of disciplines (top 95%)
  disciplines_nb <- format(n_distinct(cleaned_data$parent_1), big.mark = ",", scientific = FALSE)

  eda_plot <- ggplot(cleaned_data,
          aes(y = nb_1, axis1=display_name_0, axis2=display_name_1)) +
      geom_flow(decreasing = FALSE) +
      geom_alluvium(aes(fill = parent_0), width = 1/2, decreasing = FALSE) +
      geom_stratum(alpha = .1, width = 1/2, decreasing = FALSE, size=0.5, colour='black') +
      scale_x_discrete(expand = c(.1, .1), labels = c("Domains", "Disciplines")) +
      # geom_text(stat = "stratum", aes(label = after_stat(stratum)), size = 3, decreasing = FALSE) +
      geom_label(stat = "stratum", aes(label = after_stat(stratum)), decreasing = FALSE) +
      # scale_fill_manual(values = viridis(10, option = "D")) +
      scale_y_comma() +
        # scale_fill_ipsum() +
        scale_fill_viridis_d() +
      theme(legend.position = "none") +
      labs(
      y = "Count of records",
      # x = "Level",
      title = "Classification of scolarly publications in the largest scientific domains and disciplines",
      subtitle= paste0("Estimates the overall capability (N: ", total_nb,", only showing largest groups with n: ",subtotal_nb,")."),
      caption="Brought to you by The Lens.org data."
      )
  eda_plot <- ggplot_theme_style(eda_plot, ui_font_size)
  # Display the plot
  # print(eda_plot)

  # Save the plot to a file
  # ggsave("output/eda_plot.png", plot = eda_plot)

  # ==============================================================================
  # 6. Output Generation
  # ==============================================================================
  data_table <- data1 %>%
    select(display_name_0, display_name_1, nb_1, nb_0, percentile_1, percentile_0) %>%
    arrange(desc(percentile_1), desc(percentile_0)) %>%
    mutate(
        nb_1 = round(nb_1, 0),
        nb_0 = round(nb_0, 0),
        percentile_1 = round(percentile_1*100, 1),
        percentile_0 = round(percentile_0*100, 1),
          ) %>%
    rename(
        "Domain"="display_name_0",
        "Discipline"="display_name_1",
        "Count of records by domain"="nb_0",
        "Count of records by discipline"="nb_1",
        "Percentile of domain"="percentile_0",
        "Percentile of discipline"="percentile_1"
        )
  data_stats <- tibble(
    distance_type = c("Exact", "Close", "Related"),
    nb_orgs = c(total_nb_dec/5, total_nb_dec/(5/2), total_nb_dec/(5/3))
  )
  # If there are additional output steps, they would go here. For example, saving
  # more visualizations, or generating a report.
  return(list(plot = eda_plot, table = data_table, hero=disciplines_nb, stats = data_stats))
  # Diconnect to DB (at the end to prevent Connection to be garbage-collected)
  # dbDisconnect(conn, shutdown = TRUE)
  # Final message (for debug)
  # cat("Analysis complete. Check the 'output' folder for results.\n")
}
