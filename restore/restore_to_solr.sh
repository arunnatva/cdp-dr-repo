#!/bin/bash
set -x
# Function to display usage
usage() {
  echo "Usage: $0 -j JAAS_CONF -b HDFS_BACKUP_LOCAION -p PRINCIPAL -t KEYTAB" >> $LOG_FILE_NAME
  exit 1
}


date_folder=$(date +%Y-%m-%d)
LOG_FILE_NAME="SOLR_RESTORE_LOG_${date_folder}_$(date +%s).txt"
VALIDATION_SOLR_REPORT="SOLR_VALIDATION_${date_folder}_$(date +%s).txt"

echo "Starting Restore process" > $LOG_FILE_NAME

# Parse command line arguments
while getopts ":j:b:p:t:" opt; do
  case $opt in
    j) JAAS_CONF="$OPTARG"
    ;;
    b) HDFS_BACKUP_LOCATION="$OPTARG"
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
if [ -z "$JAAS_CONF" ] || [ -z "$HDFS_BACKUP_LOCATION" ] || [ -z "$PRINCIPAL" ] || [ -z "$KEYTAB" ]; then
  usage
fi

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}" >> LOG_FILE_NAME
}

# Function to log errors
log_error() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ERROR: ${message}" >> $LOG_FILE_NAM
}


# Main script execution
log_message "Starting Restore from HDFS to Solr..."

# Perform Kerberos authentication
log_message "Performing Kerberos authentication..."
kinit -kt ${KEYTAB} ${PRINCIPAL}

log_message "Kinit Complete"
log_message "perofrming Klist"
klist
log_message "Klist complete"

# Create a file to store RESTORE command request IDs
export RESTORE_ID_FILE="/home/$USER/restore_id_file.txt"
rm -f "$RESTORE_ID_FILE"

backup_dirs=$(hadoop fs -ls ${HDFS_BACKUP_LOCATION} | awk '{print $8}')
if [ $? -ne 0 ]; then
  log_error "Failed to list backup directories in HDFS"
  exit 1
fi

echo "Bakcups to process: ${backup_dirs}"

for backup_dir in ${backup_dirs}; do
        HDFS_PATH=$(dirname "$backup_dir")
        LOCATION=$(basename "$backup_dir")

        IFS='_' read -ra elements <<< "$LOCATION"

        # Remove the last 2 elements
        result=("${elements[@]::${#elements[@]}-2}")

        # Join the remaining elements back into a string
        COLLECTION_NAME=$(IFS='_'; echo "${result[*]}")

        # Check if the collection exists
          solrctl --jaas "$JAAS_CONF" collection --list | grep -w $COLLECTION_NAME > /dev/null

        if [ $? -eq 0 ]; then
          log_message "Collection  $COLLECTION_NAME exists, proceed to delete"
          solrctl --jaas "$JAAS_CONF" collection --delete $COLLECTION_NAME
          if [ $? -eq 0 ]; then
            log_message "Collection '$COLLECTION_NAME' deleted successfully."
          else
            log_error "Failed to delete collection '$COLLECTION_NAME'."
          fi
        else
          log_message "Collection '$COLLECTION_NAME' does not exist."
        fi

        export RESTORE_ID="restore_id_"$( date +%H%M%s )
        export COLLECTION_BKUP=$( basename $backup_dir )

        log_message "solrctl collection --restore $COLLECTION_NAME -l $HDFS_BACKUP_LOCATION -b $COLLECTION_BKUP -i $RESTORE_ID -m 6"
        solrctl --jaas "$JAAS_CONF" collection --restore "$COLLECTION_NAME" -l "$HDFS_BACKUP_LOCATION" -b "$COLLECTION_BKUP" -i "$RESTORE_ID" -m 6

        solrctl --jaas "$JAAS_CONF" collection --request-status "$RESTORE_ID"
        echo "$RESTORE_ID" >> "$RESTORE_ID_FILE"

       if [ $? -eq 0 ]; then
          log_message "Import completed successfully: ${s3_backup_path}"
       else
          log_error "Failed to import ${hdfs_backup_path} to ${s3_backup_path}"
       fi
done


# Check status of each request ID to confirm that all requests are processed

res_ct=0
coll_ct=$( wc -l "$RESTORE_ID_FILE" | awk '{print $1}' )
echo $coll_ct

while (( "$res_ct" < "$coll_ct" ))
do
  res_ct=0

  for i in $(cat "$RESTORE_ID_FILE" )
  do
     echo $i
     solrctl --jaas "$JAAS_CONF" collection --request-status $i | grep running
     if [[ $? -eq 0 ]]; then
        echo "restore is running for $i"
     else
        let res_ct=$res_ct+1
     fi
     echo "res ct value $res_ct"
  done

done

log_message "All SOLR Collections are Restored !! "

log_message "Destroying the ticket"
kdestroy
