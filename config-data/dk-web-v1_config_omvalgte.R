input_file       <- paste0("/home/",Sys.getenv("USER"),"/projects/etl2-netarkivet/config-data/omvalgte harvests.csv")
job_states       <- c(3,4) # DONE and FAILED
output_directory <- paste0("/home/",Sys.getenv("USER"),"/projects/etl2-netarkivet/output/")