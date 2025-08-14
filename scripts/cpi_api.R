# Packages ----

# Set the package names to read in
packages <- c("tidyverse", "fredr", "openxlsx", "lubridate", "xts", "blsAPI")

# Install packages that are not yet installed
installed_packages <- packages %in% rownames(installed.packages())

if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Load the packages
invisible(lapply(packages, library, character.only = TRUE))

# Remove unneeded variables
rm(packages, installed_packages)

# File paths and environment variables ----

Sys.setenv(BLS_API_KEY = "f8f33dda5c304c16842d8ac450453d2c")

earliest_year_for_data_needs <- '2020' # Change this to a different value if needing data further back than 2000!
metro_shapefile_file_path <- "C:/Users/ianwe/Downloads/shapefiles/2023/CBSAs/cb_2023_us_cbsa_500k.shp"
census_division_shapefile_file_path <- "C:/Users/ianwe/Downloads/shapefiles/2023/Census Divisions/cb_2023_us_division_20m.shp"

output_file_path <- 'outputs/cpi_data.xlsx'
output_file_path_shape_file <- 'outputs/cpi_data.shp'

# Read in shape files ----

metro_shapefile <- st_read(metro_shapefile_file_path)
metro_shapefile <- metro_shapefile %>%
  select(NAME, GEOID, geometry) %>%
  rename(geo_name = NAME, geo_code = GEOID)

census_division_shapefile <- st_read(census_division_shapefile_file_path)
census_division_shapefile <- census_division_shapefile %>%
  select(NAME, GEOID, geometry) %>%
  rename(geo_name = NAME, geo_code = GEOID)

combined_shapefile <- metro_shapefile %>%
  rbind(census_division_shapefile)

# Read in data ----

series_ids <- c(
  "CUURS35CSA0", "CUURS35ESA0", "CUURS11ASA0", "CUURS23ASA0",
  "CUURS37ASA0", "CUURS48BSA0", "CUURS23BSA0", "CUURS37BSA0",
  "CUURS49ASA0", "CUURS35BSA0", "CUURS24ASA0", "CUURS12ASA0",
  "CUURS12BSA0", "CUURS48ASA0", "CUURS49CSA0", "CUURS24BSA0",
  "CUURS49ESA0", "CUURS49BSA0", "CUURS49DSA0", "CUURS35DSA0",
  "CUURS35ASA0", "CUURS49GSA0", "CUURS49FSA0",
  
  "CUUR0110SA0", "CUUR0120SA0", "CUUR0230SA0", "CUUR0240SA0",
  "CUUR0350SA0", "CUUR0360SA0", "CUUR0370SA0", "CUUR0480SA0",
  "CUUR0490SA0"
)

# Payload for API request
payload <- list(
  seriesid   = series_ids,
  startyear  = earliest_year_for_data_needs,
  endyear    = as.character(year(Sys.Date())),
  calculations = FALSE,
  annualaverage = FALSE
)

# Request data
response_json <- blsAPI(payload, 2)

# Parse JSON without flattening lists
json_data <- fromJSON(response_json, simplifyDataFrame = FALSE)

# Extract series list
series_list <- json_data$Results$series

# Function to process each series element into tidy df
to_df <- function(series) {
  tibble(
    seriesID   = series$seriesID,
    year       = as.integer(map_chr(series$data, "year")),
    period     = map_chr(series$data, "period"),
    periodName = map_chr(series$data, "periodName"),
    value      = as.numeric(map_chr(series$data, "value"))
  ) %>%
    filter(grepl("^M", period)) %>%  # Keep only monthly data
    mutate(
      month = match(periodName, month.name),
      date  = make_date(year, month, 1)
    ) %>%
    select(seriesID, date, value)
}

# Combine all series into one tidy dataframe
clean_df <- bind_rows(lapply(series_list, to_df)) %>%
  arrange(seriesID, date)

# Clean data ----

clean_df <- clean_df %>%
  mutate(geo_name = case_when(
    seriesID == "CUURS35CSA0" ~ "Atlanta-Sandy Springs-Roswell, GA",
    seriesID == "CUURS35ESA0" ~ "Baltimore-Columbia-Towson, MD",
    seriesID == "CUURS11ASA0" ~ "Boston-Cambridge-Newton, MA-NH",
    seriesID == "CUURS23ASA0" ~ "Chicago-Naperville-Elgin, IL-IN",
    seriesID == "CUURS37ASA0" ~ "Dallas-Fort Worth-Arlington, TX",
    seriesID == "CUURS48BSA0" ~ "Denver-Aurora-Centennial, CO",
    seriesID == "CUURS23BSA0" ~ "Detroit-Warren-Dearborn, MI",
    seriesID == "CUURS37BSA0" ~ "Houston-Pasadena-The Woodlands, TX",
    seriesID == "CUURS49ASA0" ~ "Los Angeles-Long Beach-Anaheim, CA",
    seriesID == "CUURS35BSA0" ~ "Miami-Fort Lauderdale-West Palm Beach, FL",
    seriesID == "CUURS24ASA0" ~ "Minneapolis-St. Paul-Bloomington, MN-WI",
    seriesID == "CUURS12ASA0" ~ "New York-Newark-Jersey City, NY-NJ",
    seriesID == "CUURS12BSA0" ~ "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD",
    seriesID == "CUURS48ASA0" ~ "Phoenix-Mesa-Chandler, AZ",
    seriesID == "CUURS49CSA0" ~ "Riverside-San Bernardino-Ontario, CA",
    seriesID == "CUURS24BSA0" ~ "St. Louis, MO-IL",
    seriesID == "CUURS49ESA0" ~ "San Diego-Chula Vista-Carlsbad, CA",
    seriesID == "CUURS49BSA0" ~ "San Francisco-Oakland-Fremont, CA",
    seriesID == "CUURS49DSA0" ~ "Seattle-Tacoma-Bellevue, WA",
    seriesID == "CUURS35DSA0" ~ "Tampa-St. Petersburg-Clearwater, FL",
    seriesID == "CUURS35ASA0" ~ "Washington-Arlington-Alexandria, DC-VA-MD-WV",
    seriesID == "CUURS49GSA0" ~ "Anchorage, AK",
    seriesID == "CUURS49FSA0" ~ "Urban Honolulu, HI",
    
    seriesID == "CUUR0110SA0" ~ "New England",
    seriesID == "CUUR0120SA0" ~ "Mid Atlantic",
    seriesID == "CUUR0230SA0" ~ "East North Central",
    seriesID == "CUUR0240SA0" ~ "West North Central",
    seriesID == "CUUR0350SA0" ~ "South Atlantic",
    seriesID == "CUUR0360SA0" ~ "East South Central",
    seriesID == "CUUR0370SA0" ~ "West South Central",
    seriesID == "CUUR0480SA0" ~ "Mountain",
    seriesID == "CUUR0490SA0" ~ "Pacific",
    
    T ~ NA
  )) %>%
  rename(cpi = value)

clean_df_yoy <- clean_df %>%
  pivot_wider(names_from = geo_name, values_from = cpi, id_cols = date)

clean_df_yoy <- clean_df_yoy %>%
  as.xts()
clean_df_yoy <- (clean_df_yoy - lag(clean_df_yoy, 12))/lag(clean_df_yoy, 12)

clean_df_yoy <- clean_df_yoy %>%
  as.data.frame() %>%
  rownames_to_column('date')

clean_df_yoy <- clean_df_yoy %>%
  pivot_longer(names_to = 'geo_name', values_to = 'cpi_yoy', cols = -c('date')) %>%
  mutate(date = as.Date(date))

clean_df <- clean_df %>%
  select(-seriesID) %>%
  left_join(clean_df_yoy, by = c('geo_name', 'date'))

# Output data ----

write.xlsx(data_final, output_file_path)

arc.check_product()

clean_df <- clean_df %>%
  left_join(combined_shapefile, by = 'geo_name')

clean_df <- st_as_sf(clean_df)
arc.write(data_final, output_file_path_shape_file, overwrite = T, validate = T)