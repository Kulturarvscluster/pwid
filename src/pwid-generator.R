#!/usr/bin/Rscript
args <- commandArgs(trailingOnly = TRUE)
message("pwid-generator.R <year> <path to corpus>")

##cat(args, sep = "\n")

########################################################################
## CONFIGURATION
##
chosen_year <- args[1] # E.g. 2006
corpus_path <- args[2] # E.g. /datapool/dk-web-solr/solr-corpus/2006_corpus_solr-parquet

archive <- "netarkivet.dk"
corpus_name <- paste0("corpus", chosen_year)

##corpus_path <- paste0("/datapool/dk-web-solr/solr-corpus/", chosen_year, "_corpus_solr-parquet-without-dedup")
pwidlist_path <- paste0("/projects/p002/pwid/",chosen_year,"_pwidlist")
##
## END CONFIGURATION
########################################################################


## load the nessacary libraries
library(sparklyr)
library(dplyr)
library(lubridate)
library(here)
library(lubridate)
library(stringr)

## load some util functions
source(here::here("src/spark_util.R"))

log <- function(s) {
    message("## SCRIPT: ",str_c(now(), " - ", s))
}

message("########################################################################")
message("##                                                                    ##")
message("##                        BEGIN PROGRAM                               ##")
message("##                       ",now())
message("##                                                                    ##")
message("########################################################################")
message("  ")
message("##                        CONFIGURATION                               ##")
message("## YEAR: ",chosen_year,"                                                         ##")
message("## CORPUS_PATH: ",corpus_path)
message("  ")
log("Connect to the Spark cluster")
##quit(save = "no")

conf <- spark_config()
conf$spark.dynamicAllocation.minExecutors <- 20 # secures at least 20 executors
conf$spark.sql.session.timeZone <- "UTC"

spark_disconnect_all()
sc <- spark_connect(
    master = 'yarn-client',
    app_name = paste0("pwidlist_",chosen_year),
    config = conf
)

log("Load the given Solr data")
solr_corpus <- spark_read_parquet(
    sc,
    name = corpus_name,
    path = corpus_path, # use corpus_path for 2006-2011, use corpus_repartitioned_path for 2012-2016
    memory = FALSE,
    overwrite = TRUE
)
number_of_pwids <- tally(solr_corpus) %>% collect()

log("Create a PWID data frame in Spark memory")
sdf_pwid <- solr_corpus %>% 
    mutate(pwid_datetime = from_unixtime(solr_unix_time,"yyyy-MM-dd'T'HH:mm:ss'Z'")) %>% 
    mutate(pwid = paste("urn","pwid", archive, pwid_datetime, "part", uri, sep = ":")) %>%
    select(pwid) %>% 
    compute(name = "sdf_pwid") # caches in Spark memory

message("## Generated ", number_of_pwids, " PWIDs")

log("Write the PWID list to HDFS")
spark_write_csv(
    sdf_pwid %>% sdf_coalesce(1),
    path = pwidlist_path,
    mode = "overwrite",
    header = FALSE
)
message("## data stored at", pwidlist_path)
message("##")
message("########################################################################")
message("##                                                                    ##")
message("##                        END OF PROGRAM                              ##")
message("##                       ",now())
message("##                                                                    ##")
message("########################################################################")
