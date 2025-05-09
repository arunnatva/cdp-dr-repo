#!/bin/bash

# Function to display usage
usage() {
  echo "Usage: $0 -b S3_BUCKET -m MAPPERS -p PRINCIPAL -t PASSWORD"
  exit 1
}

# Parse command line arguments
while getopts ":b:m:p:t:" opt; do
  case $opt in
    b) S3_BUCKET="$OPTARG"
    ;;
    m) MAPPERS="$OPTARG"
    ;;
    p) PRINCIPAL="$OPTARG"
    ;;
    t) PASSWORD="$OPTARG"
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
if [ -z "$S3_BUCKET" ] || [ -z "$MAPPERS" ] || [ -z "$PRINCIPAL" ] || [ -z "$PASSWORD" ]; then
  usage
fi

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
}

# Function to check if HBase snapshot exists
snapshot_exists() {
  local snapshot_name=$1
  echo "list_snapshots '${snapshot_name}'" | hbase shell -n | grep -w -q "\"${snapshot_name}\""
  return $?
}

# Function to create snapshot
create_snapshot() {
  local namespace=$1
  local table=$2
  local snapshot_name="${namespace}_${table}"

  log_message "Checking if snapshot ${snapshot_name} exists..."
  if snapshot_exists "${snapshot_name}"; then
    log_message "Snapshot ${snapshot_name} already exists. Deleting existing snapshot..."
    echo "delete_snapshot '${snapshot_name}'" | hbase shell -n
    if [ $? -ne 0 ]; then
      log_message "ERROR: Failed to delete existing snapshot ${snapshot_name}"
      return 1
    fi
  fi

  log_message "Creating snapshot for table ${namespace}:${table}..."
  echo "snapshot '${namespace}:${table}', '${snapshot_name}'" | hbase shell -n
  if [ $? -eq 0 ]; then
    log_message "Snapshot created successfully: ${snapshot_name}"
    export_snapshot_to_s3 ${namespace} ${snapshot_name}
  else
    log_message "ERROR: Failed to create snapshot for table ${namespace}:${table}"
  fi
}

# Function to export snapshot to S3
export_snapshot_to_s3() {
  local namespace=$1
  local snapshot_name=$2
  local date_folder=$(date +%Y-%m-%d)
  local backup_dir="data/dr_backups/hbase/${date_folder}"
  local s3_snapshot_dir="s3a://${S3_BUCKET}/$backup_dir/${namespace}/${snapshot_name}"

  log_message "Exporting snapshot ${snapshot_name} to S3..."
  hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot ${snapshot_name} -copy-to ${s3_snapshot_dir} -mappers ${MAPPERS}
  if [ $? -eq 0 ]; then
    log_message "Snapshot exported to S3 successfully: ${snapshot_name}"
  else
    log_message "ERROR: Failed to export snapshot to S3: ${snapshot_name}"
  fi
}

# Function to ignore system tables
is_system_table() {
  local table_name=$1
  [[ $table_name == hbase:* || $table_name == kudu:* || $table_name == system:* || $table_name == meta* ]]
}

# Main script execution
log_message "Starting HBase backup process..."

# Perform Kerberos authentication
log_message "Performing Kerberos authentication..."
echo $PASSWORD | kinit ${PRINCIPAL}

# Get list of namespaces
namespaces=$(echo "list_namespace" | hbase shell -n | grep -v "NAMESPACE" | grep -v "hbase" | grep -v "default" | grep -v "^SYSTEM$")
if [ $? -ne 0 ]; then
  log_message "ERROR: Failed to list namespaces"
  exit 1
fi 

for namespace in ${namespaces}; do
  log_message "Processing namespace: ${namespace}"
  # Get list of tables in the namespace
  tables=$(echo "list_namespace_tables '${namespace}'" | hbase shell -n | grep -v "^TABLE$" | grep -v "hbase" | grep -v "^SYSTEM$" | grep -v "row(s)" | grep -v "Took" | grep -v "seconds" | grep -v "=>" | grep -v "^[0-9]*$" | tr '\n' ' ' | sed 's/,//g')
  if [ $? -ne 0 ]; then
    log_message "ERROR: Failed to list tables for namespace ${namespace}"
    continue
  fi

  clean_tables=$(echo $tables | tr -d '[]"')

  # Process each table
  for table in ${clean_tables}; do
    if is_system_table ${table}; then
      log_message "Ignoring system table: ${table}"
    else
      log_message "Processing table: ${table}"
      create_snapshot "${namespace}" "${table}"
    fi
  done
done

log_message "All backups completed successfully!"
