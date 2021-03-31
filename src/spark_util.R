## Utility script til de mere almindelige opgaver og shorthands


library(sparklyr)
library(dplyr)

#Log er en metode der gør det let at få timestamped log-entries. Det er praktisk
# når man vil time hvor langt tid noget tager
# Sæt log prefix for at sætte et prefix der kommer på alle log-linier
#
# Bruges som
#
# log("Spark is", "starting")
log_prefix <- ""
log <- function(..., prefix=log_prefix){
  cat(strftime(Sys.time()), prefix, paste0(...), "\n", sep="\t")
}

#Shorthand til åbne n (default 10) linier i en viewer
spark_view <- function(df, n=10){
  View(df %>% head(n) %>% collect())
}

#Shorthand til at få en spark-værdi pakket ud som en simpel R værdi
spark_value <- function(table){
  unlist(table %>% collect(), use.names = FALSE)
}

#Få antal linier i en dataframe som et tal
size_of_dataframe <- function(x){
  spark_value(x %>% tally())
}

#Cache dataframe without the hassle
spark_cache <- function(table, name){
  sc <- spark_connection(table)
  named_table <- table %>% sdf_register(name=name)
  tbl_cache(sc,name)
  named_table
}


write_csv_spark <- function(df, path){
  spark_write_csv(df %>% sdf_coalesce(1),path = path, mode = 'overwrite')
}

cache_dataframe <- function(df, name) {
  spdf <- spark_connection(df)
  if ( name %in% src_tbls(spdf) ) {
    tbl_uncache(spdf, name = name)
    dplyr::db_drop_table(spdf, name)
  }
  #Cache and checkpoint to remove heritance
  df_checkpointed <-  df %>% spark_cache(name) %>% sdf_checkpoint(eager = TRUE)
  #Uncache the pre-checkpointed cache
  tbl_uncache(spdf, name = name)
  #And recache it
  df_checkpointed  %>% spark_cache(name)
}


############ HDFS Ting ##########################

# From https://community.hortonworks.com/articles/34047/read-from-hdfs-in-r-script.html
#

# Install the rhdfs library, so you can work directly with HDFS files
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")

if(!require(rhdfs)){
  install.packages('rJava')
  install.packages(
    "https://github.com/RevolutionAnalytics/rhdfs/blob/master/build/rhdfs_1.0.8.tar.gz?raw=true",
    quiet = TRUE,
    repos=NULL)
  library(rhdfs)
}
hdfs.init()

#Hack to work around lack of skipTrash option
hdfs_delete_skipTrash <- function(filename){

  if (hdfs.exists(filename)){
    print(filename)
    hdfs.delete(filename);
  }
  trash_path <- paste0(".Trash/*/user/",Sys.getenv("USER"),"/", sub("hdfs:///","",sub("hdfs://KAC/","",filename)))
  if (hdfs.exists(trash_path)){
    print(trash_path)
    hdfs.delete(trash_path)
  }
}


