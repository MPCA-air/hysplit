# Download NARR files from NOAA
library(downloader)
library(openair)
library(tidyverse)

# Local MET directory
folder   <- "C:/Users/~/Desktop/NARR"

narr_dir <- "ftp://arlftp.arlhq.noaa.gov/narr/"

years    <- c(2008)

files    <- c()

for (year in years) {
   
   files <- c(paste0("NARR", year, formatC(1:12, width = 2, format = "d", flag = "0")), files)
}

# Add border months
files <- c(paste0("NARR", min(years) - 1, 12), files)

files <- c(files, paste0("NARR", max(years) + 1, "01"))

files <- unique(files) %>% sort()

files <- files[!files %in% c("NARR200912", "NARR201012", "NARR201701")]

# Download list of NARR met files by name
for (file in files[]) {
    
  print(file)
  
  start <- Sys.time()
  
  download(url      = paste0(narr_dir, file),
           destfile = paste0(folder, "/", file),
           method   = "auto",
           quiet    = FALSE,
           mode     = "wb",
           cacheOK  = FALSE)
  
  print(round(Sys.time() - start))
  
}

closeAllConnections()

##
