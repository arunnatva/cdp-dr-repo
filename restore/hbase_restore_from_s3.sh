#!/bin/bash
# restore hbase tables from S3 objects
# Function to display usage
usage() {
  echo "Usage: $0 -b S3_BUCKET -m MAPPERS -p PRINCIPAL -t ABSOLUTE PATH OF KEYTAB LOCATION"
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
if [ -z "$S3_BUCKET" ] || [ -z "$MAPPERS" ]  || [ -z "$PRINCIPAL" ] || [ -z "$KEYTAB" ]; then
  usage
fi

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
}

# Function to restore snapshot from S3 to HBase
restore_snapshot_from_s3() {
  local namespace=$1
  local table=$2

  log_message "Restoring snapshot ${table} for table ${namespace}:${table_name} from S3..."

  local s3_snapshot_path="s3a://${S3_BUCKET}/${namespace}/${table}/"

  existing_snapshot_name=$(echo "list_snapshots" | hbase shell -n | grep ${table})
if [ ! -z "${existing_snapshot_name}" ]; then
    echo "Snapshot exists, deleting snapshot ${table} "
    delete_snapshot=$(echo "delete_snapshot '${table}'" | hbase shell -n )
    if [ $? -eq 0 ]; then
     log_message "Snapshot deleted ${table}"
    else
     log_message "ERROR: Failed to delete snapshot : ${table}"
     #return 0;
        #exit 1
  fi
    #return 0
  else
     echo "Snapsnot does not exist. Proceeding to copy from S3"
  fi


  # Use HBase's ExportSnapshot tool to copy the snapshot from S3 to HBase
  hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot ${table} -copy-from ${s3_snapshot_path} -copy-to hdfs:///hbase/ -mappers ${MAPPERS}
  if [ $? -eq 0 ]; then
    log_message "Snapshot copied from S3 to HBase successfully: ${s3_snapshot_path}"
  else
    log_message "ERROR: Failed to copy snapshot from S3 to HBase: ${s3_snapshot_path}"
    return 0;
        #exit 1
  fi

  # Use HBase shell to restore the snapshot
  log_message "Restoring snapshot ${table} into HBase..."
  echo "create_namespace '${namespace}'" | hbase shell -n
  if [ $? -eq 0 ]; then
    log_message "Namespace created successfully in HBase: ${namespace}"
  else
    log_message "ERROR: Failed to create namespace or namespace already exists in HBase: ${namespace}"
   #exit 1
  fi

  echo "restore_snapshot '${table}'" | hbase shell -n
  if [ $? -eq 0 ]; then
    log_message "Snapshot restored successfully in HBase: ${table}"
  else
    log_message "ERROR: Failed to restore snapshot in HBase: ${table}"
    #exit 1
  fi
}

# Main script execution
log_message "Starting HBase restore process..."

# Perform Kerberos authentication
log_message "Performing Kerberos authentication..."
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
echo "Klist ---"
klist

echo "Performing Hbase restore"
echo "Disbling the tables to allow restore"
tables=$(echo "list" | hbase shell -n | grep -v "TABLE" | grep -v "row(s)" | grep -v "hbase:" | grep -v "SYSTEM:" | grep -v "Took")

# Loop through each table
for table in $tables; do
  # Check if the table is Enabled
  is_enabled=$(echo "is_enabled  '$table'" | hbase shell -n | grep -o 'true' | head -n1)

  # If the table is disabled, enable it
  if [ "$is_enabled" == "true" ]; then
    echo "Table $table is enabled. Disbling it now..."
    echo "disable '$table'" | hbase shell -n
    echo "Table $table has been disabled."
  else
    echo "Table $table is already disabled. Continuing.."
  fi
done

# Get list of namespaces and tables from the S3 bucket
namespaces=$(aws s3 ls s3://${S3_BUCKET}/ | awk '{print $2}' | sed 's#/##')
if [ $? -ne 0 ]; then
  log_message "ERROR: Failed to list namespaces from S3 bucket"
  exit 1
fi

#echo $namespaces

for namespace in ${namespaces}; do
  log_message "Processing namespace: ${namespace}"

  tables=$(aws s3 ls s3://${S3_BUCKET}/${namespace}/ | awk '{print $2}' | sed 's#/##')
  for table in ${tables}; do
    # Get list of snapshots in the S3 bucket for the namespace
    echo "Table name: ${table}"
    snapshots=$(aws s3 ls s3://${S3_BUCKET}/${namespace}/${table}/ | awk '{print $2}' | grep "snapshot")
    #echo "**********"
    #echo $snapshots
    if [ $? -ne 0 ]; then
      log_message "ERROR: Failed to list snapshots for namespace ${namespace} for table ${table} from S3 bucket"
      continue
    fi
      restore_snapshot_from_s3 ${namespace} ${table} 
  done
done

echo "Enabling the restored tables"
tables=$(echo "list" | hbase shell -n | grep -v "TABLE" | grep -v "row(s)" | grep -v "hbase:" | grep -v "SYSTEM:" | grep -v "Took")

# Loop through each table
for table in $tables; do
  # Check if the table is disabled
  is_disabled=$(echo "is_disabled '$table'" | hbase shell -n | grep -o 'true' | head -n1)

  # If the table is disabled, enable it
  if [ "$is_disabled" == "true" ]; then
    echo "Table $table is disabled. Enabling it now..."
    echo "enable '$table'" | hbase shell -n
    echo "Table $table has been enabled."
  else
    echo "Table $table is already enabled."
  fi
done
echo "Destroying the Kerberos ticket"
kdestroy
