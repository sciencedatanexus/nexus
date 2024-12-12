# ==============================================================================
# Title:        Plot Evolution of research over time 
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
rplot_cwhensn1 <- function(
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
# print(input_years_evolution)
# print(!!input_years_evolution[1])
# print(!!input_years_evolution[2])

data <- tbl(conn, 'project.records')%>%
  filter_scholarly_records(input_years_evolution[1], input_years_evolution[2]) %>%
  group_by(year_published) %>%
  summarise(
    nb_records = n(),
    avg_nb_authors = round(mean(nb_authors, na.rm=TRUE), digits = 1),
    median_nb_authors = round(median(nb_authors, na.rm = TRUE), digits = 1),
    .groups = 'drop_last' # `summarise()` has grouped output by "year_published". You can override using the `.groups` argument.
  ) %>% 
  arrange(year_published) %>% 
  collect() 
  # mutate(year_published = as.factor(year_published))

# Preview the first few rows of the data
# head(data)

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
total_nb <- format(sum(cleaned_data$nb_records), big.mark = ",", scientific = FALSE)
# Create a simple bar plot of customer satisfaction levels
# eda_plot <- ggplot()
# palette <- viridis(5, option = "magma")  # generate a few colours
# palette <- c("#55C667FF", "#DCE319FF")
eda_plot <- ggplot(cleaned_data, aes(x = year_published, y= nb_records)) +
  geom_line(linewidth=6, aes(colour=nb_records)) +
  geom_point(shape = 21, aes(colour =nb_records, fill=-nb_records), stroke=4, size=6) +
  geom_smooth(method = "lm", formula = y ~ x, se = FALSE, colour = "black", linetype = "dashed") + 
  scale_y_comma(limits=c(min(cleaned_data$nb_records)*0.95 - 1, max(cleaned_data$nb_records)*1.05)) +
  scale_fill_viridis_c() +
  scale_color_viridis_c() + 
  labs( 
    x = "Publication year", 
    y = "Count of records",
    title = "Evolution of the number of scolarly publications",
    subtitle= paste0("Estimates changes in an overall capability (N: ", total_nb, ", latest year in incomplete)."),
    caption="Brought to you by The Lens.org data."
  ) +
  theme(legend.position=NULL) +
  guides(colour = "none", fill = "none")
eda_plot <- ggplot_theme_style(eda_plot, ui_font_size)

# Display the plot
# print(eda_plot)

# Save the plot to a file
# ggsave("output/eda_plot.png", plot = eda_plot)

# ==============================================================================
# 6. Output Generation
# ==============================================================================
data_table <- cleaned_data %>%
  select(year_published, nb_records, avg_nb_authors, median_nb_authors) %>%
  arrange(desc(year_published)) %>%
  mutate(
      avg_nb_authors = round(avg_nb_authors, 1)
        ) %>%
  rename(
      "Year of publication"="year_published",
      "Count of records"="nb_records",
      "Average number of contributors by document"="avg_nb_authors",
      "Median number of contributors by document"="median_nb_authors"
      )
# If there are additional output steps, they would go here. For example, saving
# more visualizations, or generating a report.
return(list(plot = eda_plot, table = data_table, hero=total_nb))
# Diconnect to DB (at the end to prevent Connection to be garbage-collected)
dbDisconnect(conn, shutdown = "none")

# Final message (for debug)
# cat("Analysis complete. Check the 'output' folder for results.\n")
}