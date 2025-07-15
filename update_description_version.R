library(gh)
library(devtools)

# Get the latest release from the GitHub repo
release_info <- gh("GET /repos/bcgov/FAIB_DADMTOOLS/releases/latest")
latest_version <- release_info$tag_name
cat("Latest release version:", latest_version, "\n")

desc_file <- "DESCRIPTION"
desc_lines <- readLines(desc_file)
desc_lines <- sub("^Version:.*", paste0("Version: ", latest_version), desc_lines)

print(desc_lines)
writeLines(desc_lines, desc_file)
