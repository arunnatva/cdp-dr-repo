#!/bin/bash

# Usage example:
# ./import_to_s3_hdfs_cleanup.sh -d /path/to/hdfs/backup/dir -b your_s3_bucket -m number_of_mappers -p principal -t keytab

# Function to display usage
usage() {
  echo "Usage: $0 -d HDFS_BACKUP_DIR -b S3_BUCKET -m MAPPERS -p PRINCIPAL -t KEYTAB"
  exit 1
}

# Parse command line arguments
while getopts ":d:b:m:p:t:" opt; do
  case $opt in
    d) HDFS_BACKUP_DIR="$OPTARG"
    ;;
    b) S3_BUCKET="$OPTARG"
    ;;
    m) MAPPERS="$OPTARG"
    ;;
    p) PRINCIPAL="$OPTARG"
    ;;
    t) KEYTAB="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
        usage
    ;;
    :) echo "Option -$OPTARG requires an argument." >&2
       usage
    ;;
  esac
done

# Check if all required arguments are provided
if [ -z "$HDFS_BACKUP_DIR" ] || [ -z "$S3_BUCKET" ] || [ -z "$MAPPERS" ] || [ -z "$PRINCIPAL" ] || [ -z "$KEYTAB" ]; then
  usage
fi

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
}

# Function to log errors
log_error() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ERROR: ${message}" >&2
}

# Function to import from S3 to HDfs
import_from_s3() {
  local backup_dir=$1
  local hdfs_backup_path="$HDFS_BACKUP_DIR/$BACKUP_DATE"
  local s3_backup_path=${backup_dir}
  log_message "Importing ${s3_backup_path} to hdfs..."
	echo "$backup_dir"
	echo "$hdfs_backup_path"
	echo "$s3_backup_path"

  hadoop distcp -m ${MAPPERS} ${s3_backup_path} ${hdfs_backup_path}/
  if [ $? -eq 0 ]; then
    log_message "Import completed successfully: ${s3_backup_path}"
  else
    log_error "Failed to import from ${s3_backup_path} to  ${hdfs_backup_path}."
    exit 1
  fi
}

# Main script execution
log_message "Starting import from s3 to HDFS..."

# Perform Kerberos authentication
log_message "Performing Kerberos authentication..."

kinit -kt "$KEYTAB" "$PRINCIPAL"

export BACKUP_DATE=$( basename $S3_BUCKET )

if hdfs dfs -test -d "$HDFS_BACKUP_DIR/$BACKUP_DATE"; then
   echo "HDFS backup date folder exists, so delete it"
   hdfs dfs -rm -r -skipTrash "$HDFS_BACKUP_DIR/$BACKUP_DATE"
fi
echo " Create HDFS Backup directory with Date folder"
hdfs dfs -mkdir -p "$HDFS_BACKUP_DIR/$BACKUP_DATE"


# List all backup directories in S3
backup_dirs=$(hadoop fs -ls ${S3_BUCKET} | awk '{print $8}')
if [ $? -ne 0 ]; then
  log_error "Failed to list backup directories in S3"
  exit 1
fi

for backup_dir in ${backup_dirs}; do
  import_from_s3 ${backup_dir}
done


