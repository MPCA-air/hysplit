#! /usr/bin/env Rscript

# Historical HYSPLIT runs using NARR data

#C: && "C:\Users\dkvale\Documents\R\R-3.5.2\bin\i386\Rscript.exe" --no-save --no-restore "X:\Programs\Air_Quality_Programs\Air Monitoring Data and Risks\6 Air Data\MET Data\HYSPLIT\Historic_HYSPLIT_48hrs\run_HYSPLIT_history_ALL_HOURS.R"

#remotes::install_github("rich-iannone/SplitR")
#library(geosphere)
library(SplitR)
library(measurements)
library(tidyverse)
#library(janitor)

source("https://raw.githubusercontent.com/MPCA-air/hysplit/main/R/hysplit_traj.R")

# Load site locations
sites <- read_csv("X:/Agency_Files/Outcomes/Risk_Eval_Air_Mod/_Air_Risk_Evaluation/Staff folders/Dorian/AQI/MET data/Monitors and Rep Wx Stations.csv")

names(sites) <- gsub(" ", "_", tolower(names(sites)))

# Filter to one site per forecast city
sites <- filter(sites, !site_catid %in% c('27-017-7416'))

# Add toxics sites
if(FALSE) {
tox_sites <- read_csv("X:/Agency_Files/Outcomes/Risk_Eval_Air_Mod/_Air_Risk_Evaluation/Staff folders/Dorian/AQI/MET data/AirToxics_sites.csv")

tox_sites <- tox_sites %>%
             clean_names() %>%
             filter(year > 2010, 
                    !aqs_id_dashes %in% sites$site_catid,
                    !aqs_id_dashes %in% sites$alt_siteid) %>%
             group_by(aqs_id_dashes) %>%
             slice(1) %>%
             select(lat, long, city, site_name, report_name, address) %>%
             rename(site_catid = aqs_id_dashes,
                    `Air Monitr` = report_name,
                    short_name   = site_name,
                    monitor_lat  = lat,
                    monitor_long = long)

tox_sites <- read_csv("X:/Agency_Files/Outcomes/Risk_Eval_Air_Mod/_Air_Risk_Evaluation/Staff folders/Dorian/AQI/MET data/AirToxics_sites.csv")

write_csv(tox_sites, "X:/Agency_Files/Outcomes/Risk_Eval_Air_Mod/_Air_Risk_Evaluation/Staff folders/Dorian/AQI/MET data/toxics_sites.csv")
}

tox_sites <- read_csv("X:/Agency_Files/Outcomes/Risk_Eval_Air_Mod/_Air_Risk_Evaluation/Staff folders/Dorian/AQI/MET data/toxics_sites.csv")

sites <- bind_rows(sites, tox_sites)

# Prep for HYSPLIT model runs
setwd("C:/Users/dkvale/Desktop/Historic_HYSPLIT_48hrs")

met_folder <- "D:/HYSPLIT/NARR"   #"D:/NARR"

list.files(met_folder)

# Trajectory function to read all sites
aqi_traj <- function(date            = NULL,
                     receptor_height = NULL,
                     traj_hours      = NULL,
                     keep_hours      = "ALL",
                     met_dir         = met_folder,
                     met_files       = met_list) {

  # Trajectory table
  traj_forecast <- tibble()

  for (site in unique(sites$site_catid)) {

    site_df <- filter(sites, site_catid == site)

    print(site_df$site_catid)

    traj    <-  tryCatch(hysplit_traj(
                    lat          = round(site_df$monitor_lat, 4),
                    lon          = round(site_df$monitor_long, 4),
                    height       = receptor_height,
                    duration     = traj_hours,
                    run_period   = as.character(date),
                    daily_hours  = 17,
                    direction    = "backward",
                    met_type     = "NARR",
                    met_dir      =  met_dir,  
                    extended_met = T,
                    vert_motion  = 0,
                    model_height = 20000,
                    traj_name    = as.character(round(runif(1), 5)),
                    met_files    = met_files),
                    error = function(e) NA)

    if (class(traj) != "logical") {

      traj$site_catid      <- site
      traj$receptor_height <- receptor_height

      if (!tolower(keep_hours) %in% "all") {
        
        traj <- filter(traj, `hour.inc` %in% as.numeric(c(keep_hours)))
        
      }

      Sys.sleep(1)

      traj_forecast <- bind_rows(traj, traj_forecast)
    }
  }
  return(traj_forecast)
}


# Years to run
years <- 2014

yr <- 2014

for(yr in years) {

  # Days to run
  days <- tibble(date = rev(seq(as.Date(paste0(min(yr), "-01-01")),
                                as.Date(paste0(max(yr), "-12-31")), 1)))

  # Run HYSPLIT for 3 receptor heights
  heights <- c(10, 250, 500)

  ht <- 10
  
  for(ht in heights) {

    hys_archive <- tibble()

    # Loop through days
    for (i in 1:nrow(days)) {

      day <- days$date[i]

      print(day); print(ht)

      met_list     <- paste0("NARR", unique(c(format(day - 18, "%Y%m"), format(day, "%Y%m"), format(day + 18, "%Y%m"))))

      hys_day      <- aqi_traj(date            = day, 
                               receptor_height = ht, 
                               traj_hours      = 48,
                               met_dir         = "D:/HYSPLIT/NARR", 
                               met_files       = met_list)

      hys_archive  <- bind_rows(hys_day, hys_archive)

      closeAllConnections()
      
      unlink("traj-*", recursive = T)

    }

    hys_archive <- select(hys_archive, -receptor)
    
    hys_archive <- rename(hys_archive,
                          hours_back            = "hour.inc", 
                          parcel_height         = height, 
                          parcel_date           = date2,
                          final_receptor_date   = date,
                          final_receptor_height = receptor_height) 
    
    saveRDS(hys_archive, paste0(yr, "-HYSPLIT_archive_NARR_", ht, "m.rdata"))

    unlink("traj-*", recursive = T)
  }
}


if (FALSE) {
  # Join multiple years
  hys_all <- bind_rows(hys_archive, hys_archive2)

  # Update column names
  hys_all <- select(hys_all, -receptor, -pressure)

  names(hys_all)[c(5,9:10)] <- c("hours_backward", "origin_date", "receptor_date")

  hys_all$time_zone <- "GMT"

  # Save results
  saveRDS(hys_all, paste0("HYSPLIT archive using NARR data for ", years, "-", years + 1, ".rdata"))
}

##
