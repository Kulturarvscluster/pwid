
#Stage 0
blacklist_file <- "config-data/blacklist.txt"
metadata_index_file <-
"config-data/meta-files.2018-02-06-nodup"
db_dump <- "netarkivet_20180123"


base_folder <- paste0("hdfs://KAC/user/",Sys.getenv("USER"),"/dk-web-v1-reexport")


# Stage 2
crawl_file <- paste0(base_folder, "/metadata/", chosen_year, "_file_paths_crawl.log.jobdir/part-*")

goodllines_output_folder <- paste0(base_folder,"/goodlines/")
bad_output_file <- paste0(goodllines_output_folder, chosen_year, "_badlines.parquet")
good_output_file <- paste0(goodllines_output_folder, chosen_year, "_goodlines.parquet")


# Stage 3
# Construct the file name for the crawl log file
goodlines_file <- good_output_file

# Path and file name for the parquet files
corpus_file <- paste0(base_folder, "/parquet/corpus/", chosen_year, "_corpus.parquet")




# Stage 4

solr_name <- paste0("solr", chosen_year)
solr_path <- paste0("hdfs:///datapool/dk-web-solr/solr-dump/solr-", chosen_year, "-parquet-1008")

corpus_name <- paste0("corpus", chosen_year)
corpus_path <- corpus_file

corpus_repartitioned_path <- paste0(base_folder, "/corpus/", chosen_year, "_corpus-repartioned.parquet")

solr_corpus_keys_path <- paste0(base_folder,"/solr_corpus_keys/", chosen_year, "_solr_relevant_entries" )


solr_corpus_name <- paste0("solr_corpus_", chosen_year)
solr_corpus_without_dedup_path <- paste0(base_folder, "/solr-corpus/", chosen_year, "_corpus_solr-parquet-without-dedup")

antijoined_parquet_path <- paste0(base_folder,"/solr_corpus_antijoin/", chosen_year, "_corpus_solr_antijoin-parquet")



#Stage 5

solr_corpus_path <- solr_corpus_without_dedup_path

linkgraph_path <- paste0(base_folder, "/linkgraphs/", chosen_year, "_linkgraph_timestamps.csv")

ingoing_links_path <- paste0(base_folder, "/linkgraphs/", chosen_year, "_ingoing_links.csv")


# Stage 6
offset_csv_path <- paste0(base_folder, "webpage_offsets/webpages_", chosen_year, "_offsets.csv")
