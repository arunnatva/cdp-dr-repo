#!/bin/bash
# Restore kudu tables from S3 objects
# Function to display usage
usage() {
  echo "Usage: $0 -b S3_BUCKET_AND_PATH -k KUDU_MASTER -j JAR_PATH -r RESOURCEMANAGER_HOSTNAME -p PRINCIPAL -t KEYTAB"
  exit 1
}

# Parse command line arguments
while getopts ":b:k:j:r:p:t:" opt; do
  case $opt in
    b) S3_BUCKET_AND_PATH="$OPTARG"
    ;;
    k) KUDU_MASTER="$OPTARG"
    ;;
    j) JAR_PATH="$OPTARG"
    ;;
    r) RESOURCEMANAGER_HOSTNAME="$OPTARG"
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
if [ -z "$S3_BUCKET_AND_PATH" ] || [ -z "$KUDU_MASTER" ] || [ -z "$JAR_PATH" ] || [ -z "$RESOURCEMANAGER_HOSTNAME" ] || [ -z "$PRINCIPAL" ] || [ -z "$KEYTAB" ]; then
  echo "Missing required arguments"
  usage
fi

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
}

# Function to delete all tables in the Kudu master
delete_all_tables() {
  log_message "Listing all tables in Kudu master..."
  tables=$(kudu table list ${KUDU_MASTER})

  if [ -z "$tables" ]; then
    log_message "No tables found in Kudu master"
    return
  fi

  for table in ${tables}; do
    log_message "Deleting table ${table}..."
    kudu table delete ${KUDU_MASTER} ${table}
    if [ $? -eq 0 ]; then
      log_message "Table ${table} deleted successfully"
    else
      log_message "ERROR: Failed to delete table ${table}"
      exit 1
    fi
  done
}

# Function to restore Kudu table from S3 using Spark
restore_kudu_table() {
  local table=$1
  local s3_backup_dir="s3a://${S3_BUCKET_AND_PATH}"

  log_message "Restoring Kudu table ${table} from S3 using Spark..."

  spark-submit --class org.apache.kudu.backup.KuduRestore \
    --master yarn \
    --deploy-mode client \
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
    log_message "Kudu table ${table} restored successfully from ${s3_backup_dir}"
  else
    log_message "ERROR: Failed to restore Kudu table ${table} from ${s3_backup_dir}"
    exit 1
  fi
}

# Main script execution
log_message "Starting Kudu restore process..."

# Perform Kerberos authentication
log_message "Performing Kerberos authentication..."
kinit -kt ${KEYTAB} ${PRINCIPAL}
if [ $? -ne 0 ]; then
  log_message "ERROR: Kerberos authentication failed"
  exit 1
fi

# Delete all tables in the Kudu master
delete_all_tables

# List all Kudu tables from the provided S3 path
log_message "Listing all tables in the backup path..."
tables_folders=$(aws s3 ls s3://${S3_BUCKET_AND_PATH}/ | grep -v '^PRE' | awk '{print $2}' | sed 's|/$||')

if [ -z "$tables_folders" ]; then
  log_message "ERROR: No tables found in S3 backup path"
  exit 1
fi

for table_folder in ${tables_folders}; do
  table=$(echo "$table_folder" | cut -d'-' -f2-)
  restore_kudu_table ${table}
done

log_message "All restores completed successfully!"
