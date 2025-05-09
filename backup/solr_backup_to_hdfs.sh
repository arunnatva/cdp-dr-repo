#!/bin/bash
set -x
###########################################################################################################################
### This Script uses SOLRCTL command to take SnapShot of each Collection and then Export the Collection to HDFS backup Path.
### This script is part ofthe process that takes backup of SOLR Collections in On prem CDP cluster, copies backup collections 
### from HDFS to S3
### Once S3 Cross region replication pushes data to Plan Test DR S3 bucket, another script will be triggered in CDP Onprem/Cloud    ### DR cluster to restore SOLR Collections 
###########################################################################################################################

# Usage example:
# ./backup_to_hdfs.sh solr_backup_config.ini

if [[ ! -d "logs/solr" ]]; then
  mkdir -p logs/solr
fi


export LOG_FILE_NAME="logs/solr/"$(date +'%Y-%m-%d_%H_%M_%S')".log"
echo $LOG_FILE_NAME


# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}" >> $LOG_FILE_NAME
}


# Function to display usage
usage() {
  log_message "Usage: $0 /path/to/config.ini" 
  exit 1
}


if [[ $# -ne 1 ]]; then
   usage
fi   


# Create a Variable with Today's Date
#
# Source the Variables defined in the Configuration File
source "$1"

date_folder=$(date +%Y-%m-%d)
echo "date folder value : $date_folder"

# Write the Backup Date to a file so that the script that copies data from HDFS to S3 can read the correct date

echo "$date_folder" > "$BACKUP_DATE_FILE"


# Authenticate with Kerberos Keytab. 
# It is required to create the HDFS folder with Today's date to store SOLR collection backups

log_message "Authenticating with Kerberos Principal and Keytab"
kinit -kt "$KEYTAB" "$PRINCIPAL"
if [[ $? -eq 0 ]]; then
  log_message "Successfully authenticated with keytab"
else
  log_message "Error authenticating with keytab. Please check keytab and principal"
fi


## Create a HDFS folder with Today's Date

if hdfs dfs -test -d ${HDFS_BACKUP_DIR}"/"${date_folder} ; then

    log_message "the hdfs folder exists, delete it and create it again"
    hdfs dfs -rm -r -skipTrash ${HDFS_BACKUP_DIR}"/"${date_folder}
    hdfs dfs -mkdir -p ${HDFS_BACKUP_DIR}"/"${date_folder}
 
else

    log_message "the hdfs folder does not exist, create it"
    hdfs dfs -mkdir -p ${HDFS_BACKUP_DIR}"/"${date_folder}
 
fi

###GET LIST OF COLLECTIONS USING SOLRCTL COMMAND

COLLECTIONS_LIST=$(solrctl collection --list | awk '{print $1}')


### LOOP THROUGH LIST OF COLLECTIONS, CREATE SNAPSHOTS, AND EXPORT SNAPSHOTS TO HDFS

for COLLECTION in $COLLECTIONS_LIST

do

  log_message "coll name :  $COLLECTION"

  log_message "Taking Snapshot of the Collection : $COLLECTION"
  export COLL_SNAPSHOT=$COLLECTION"_SS_"$date_folder
  CMD_RESULT=$( solrctl --jaas "$JAAS_CONF" collection --create-snapshot "$COLL_SNAPSHOT" -c "$COLLECTION" )
  log_message "$CMD_RESULT"
  CMD_RESULT=""
  sleep 5

  log_message "Export Snapshot of the Collection to HDFS : $COLLECTION"
  CMD_RESULT=$( solrctl --jaas "$JAAS_CONF" collection --export-snapshot "$COLL_SNAPSHOT" -c "$COLLECTION" -d "$HDFS_BACKUP_DIR/$date_folder/" )
  log_message "$CMD_RESULT"
  CMD_RESULT=""
  sleep 5

done

log_message "All Solr backups completed successfully!"
