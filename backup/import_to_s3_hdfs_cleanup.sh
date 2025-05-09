#!/bin/bash
set -x
# Usage example:
# ./import_to_s3_and_cleanup.sh /path/to/config.ini

# Function to display usage
usage() {
  echo "Usage: $0 /path/to/config.ini"
  exit 1
}

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
}

source "$1"

# Check if all required arguments are provided
if [ -z "$HDFS_BACKUP_DIR" ] || [ -z "$S3_PATH" ] || [ -z "$MAPPERS" ] || [ -z "$PRINCIPAL" ] || [ -z "$KEYTAB" ] || [ -z "$BACKUP_DATE_FILE" ]; then
  usage
else
  echo "All args are passed"
fi


# Perform Kerberos authentication
log_message "Performing Kerberos authentication..."
kinit -kt ${KEYTAB} ${PRINCIPAL}

# Function to log errors
log_error() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ERROR: ${message}" >&2
}




# Function to import from HDFS to S3
import_to_s3() {
  local hdfs_backup_path=$1
  local s3_backup_path=$2

  log_message "Importing ${hdfs_backup_path} to S3..."

  hadoop distcp -m ${MAPPERS} ${hdfs_backup_path} ${s3_backup_path}/
  if [ $? -eq 0 ]; then
    log_message "Import completed successfully: ${s3_backup_path}"
  else
    log_error "Failed to import ${hdfs_backup_path} to ${s3_backup_path}"
    exit 1
  fi
}

# Function to clean up HDFS
cleanup_hdfs() {
  local hdfs_backup_path=$1

  log_message "Cleaning up HDFS path: ${hdfs_backup_path}"

  hadoop fs -rm -r -skipTrash ${hdfs_backup_path}
  if [ $? -eq 0 ]; then
    log_message "Cleanup completed successfully: ${hdfs_backup_path}"
  else
    log_error "Failed to clean up HDFS path: ${hdfs_backup_path}"
    exit 1
  fi
}


# Main script execution
log_message "Starting import to S3 and cleanup process..."

export BKP_DATE=$( cat $BACKUP_DATE_FILE )

echo "here is the backup date : $BKP_DATE"


if hdfs dfs -test -d ${HDFS_BACKUP_DIR}/${BKP_DATE}; then
   echo "HDFS Backup Directory exists"
else
   echo "HDFS Backup Directory ${HDFS_BACKUP_DIR}/${BKP_DATE} does not exist, exiting "
   exit 1
fi

if hdfs dfs -test -d ${S3_PATH}/${BKP_DATE}; then
  
   echo "S3 PATH for CURRENT DATE exist, so delete it so that new data is copied"
   hdfs dfs -rm -r -skipTrash ${S3_PATH}/${BKP_DATE}

else

   echo "S3 PATH does not exist, distcp will create it"
   
fi


#Import data from HDFS to S3
import_to_s3 ${HDFS_BACKUP_DIR}/${BKP_DATE} ${S3_PATH}

#Cleanup SOLR backup from HDFS, to conserve storage
cleanup_hdfs ${HDFS_BACKUP_DIR}/${BKP_DATE}

exit 0

log_message "All imports to S3 and HDFS cleanups completed successfully!"
