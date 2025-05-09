#!/bin/bash

# Function to display usage
usage() {
  echo "Usage: $0 -b S3_BUCKET -k KUDU_MASTER -j JAR_PATH -r RESOURCEMANAGER_HOSTNAME -p PRINCIPAL -t KEYTAB_PATH"
  exit 1
}

# Parse command line arguments
while getopts ":b:k:j:r:p:t:" opt; do
  case $opt in
    b) S3_BUCKET="$OPTARG"
    ;;
    k) KUDU_MASTER="$OPTARG"
    ;;
    j) JAR_PATH="$OPTARG"
    ;;
    r) RESOURCEMANAGER_HOSTNAME="$OPTARG"
    ;;
    p) PRINCIPAL="$OPTARG"
    ;;
    t) KEYTAB_PATH="$OPTARG"
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
if [ -z "$S3_BUCKET" ] || [ -z "$KUDU_MASTER" ] || [ -z "$JAR_PATH" ] || [ -z "$RESOURCEMANAGER_HOSTNAME" ] || [ -z "$PRINCIPAL" ] || [ -z "$KEYTAB_PATH" ]; then
  echo "Missing required arguments"
  usage
fi

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
}

# Function to backup Kudu table to S3 using Spark
backup_kudu_table() {
  local table=$1
  date_folder=$(date +%Y-%m-%d)
  echo "date of run : $date_folder" 
  local backup_dir="data/dr_backups/kudu/${date_folder}"
  local s3_backup_dir="s3a://${S3_BUCKET}/${backup_dir}"
  echo "s3 path : $s3_backup_dir"

  log_message "Backing up Kudu table ${table} to S3 using Spark..."

  # Verify the JAR file contains the required class
  if ! jar tf ${JAR_PATH} | grep -q "org/apache/kudu/backup/KuduBackup.class"; then
    log_message "ERROR: The JAR file does not contain the class org.apache.kudu.backup.KuduBackup"
    exit 1
  fi

  spark-submit --class org.apache.kudu.backup.KuduBackup \
    --master yarn \
    --deploy-mode cluster \
    --principal ${PRINCIPAL} \
    --keytab ${KEYTAB_PATH} \
    --conf spark.executor.memory=6g \
    --conf spark.driver.memory=6g \
    --conf spark.executor.cores=2 \
    --num-executors 10 \
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    --conf spark.yarn.resourcemanager.address=${RESOURCEMANAGER_HOSTNAME}:8032 \
    --conf spark.kudu.client.request_timeout_ms=60000 \
    --conf spark.kudu.client.operation_timeout_ms=60000 \
    ${JAR_PATH} \
    --kuduMasterAddresses ${KUDU_MASTER} \
    --rootPath ${s3_backup_dir} \
    ${table}
  
  
  if [ $? -eq 0 ]; then
    log_message "Kudu table ${table} backed up successfully to ${s3_backup_dir}"
  else
    log_message "ERROR: Failed to backup Kudu table ${table} to ${s3_backup_dir}"
    exit 1
  fi
}

# Main script execution
log_message "Starting Kudu backup process..."

# Perform Kerberos authentication
log_message "Performing Kerberos authentication..."
kinit -kt ${KEYTAB_PATH} ${PRINCIPAL}
if [ $? -ne 0 ]; then
  log_message "ERROR: Kerberos authentication failed"
  exit 1
fi

# List all Kudu tables
tables=$(kudu table list ${KUDU_MASTER})
if [ $? -ne 0 ]; then
  log_message "ERROR: Failed to list Kudu tables"
  exit 1
fi

for table in ${tables}; do
  backup_kudu_table ${table}
done

log_message "All backups completed successfully!"

