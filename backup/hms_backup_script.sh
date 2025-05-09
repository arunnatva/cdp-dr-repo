#!/bin/bash
set -x

# Function to display usage
usage() {
  echo "Usage: $0 <environment> <principal> <keytab-path> <s3-bucket-name>"
  echo "Example: $0 testenv svc_admin_acct@KERBEROS.REALM.COM /path/to/keytab/file cdpenv-s3-bucket"
  exit 1
}

# Check if required arguments are provided
if [ "$#" -ne 4 ]; then
  usage
fi


# Variables
ENV=$1
PRINCIPAL=$2
KEYTAB=$3
S3_BUCKET=$4
DATE_FOLDER=$(date +%Y-%m-%d)
S3_PATH="s3a://${S3_BUCKET}/data/dr_backups/hms/${DATE_FOLDER}"
LOCAL_BACKUP_DIR="$HOME/reports/hms_backup_exports/${DATE_FOLDER}/src_tables"
VIEWS_SOURCE_DIR="$HOME/reports/hms_backup_exports/${DATE_FOLDER}/views_source"
VIEWS_TARGET_DIR="$HOME/reports/hms_backup_exports/${DATE_FOLDER}/views_target"
LOCAL_DEBUG_DIR="$HOME/reports/hms_backup_exports/debug/${DATE_FOLDER}/debug"

echo "*** local backup dirs $LOCAL_BACKUP_DIR"
echo "*** view source dirs $VIEWS_SOURCE_DIR"


export PRINCIPAL=$2
export KEYTAB=$3

#do  kinit
kinit -kt "$KEYTAB" "$PRINCIPAL"

klist

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}" >> "${LOCAL_DEBUG_DIR}/script.log"
}

# Clean up local export directory if it exists
if [ -d "${LOCAL_BACKUP_DIR}" ]; then
  rm -rf /root/.hms-mirror/reports/hms_backup_exports/${DATE_FOLDER}
fi

#cleanup hdfs directories if it exists
if $(hadoop fs -test -d $S3_PATH) ; then
  hdfs dfs -rm -r -skipTrash $S3_PATH
fi

# Create local export and debug directories
mkdir -p ${LOCAL_BACKUP_DIR} || { echo "ERROR: Failed to create local backup directory"; exit 1; }
mkdir -p ${LOCAL_DEBUG_DIR} || { echo "ERROR: Failed to create debug directory"; exit 1; }
mkdir -p ${VIEWS_SOURCE_DIR} || { echo "ERROR: Failed to create VIEWS Source directory"; exit 1; }
mkdir -p ${VIEWS_TARGET_DIR} || { echo "ERROR: Failed to create VIEWS target directory"; exit 1; }

if [ "${ENV}" == "plantest" ]; then
    JDBC_STRING="jdbc:hive2://test-cdp-master0.cloudera.site:2181,test-cdp-master1.cloudera.site:2181,test-cdp-master0x.cloudera.site:2181/default;httpPath=cliservice;principal=hive/_HOST@KERBEROS.REALM.COM;retries=5;serviceDiscoveryMode=zooKeeper;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;transportMode=http;trustStorePassword=ltIVdp0w530gJO1zhGK4166Dd7;zooKeeperNamespace=hiveserver2"
else
        JDBC_STRING="jdbc:hive2://prod1-cdp-master0.cloudera.site:2181,prod1-cdp-master1.cloudera.site:2181,prod1-cdp-masterx0.cloudera.site:2181/default;httpPath=cliservice;principal=hive/_HOST@KERBEROS.REALM.COM;retries=5;serviceDiscoveryMode=zooKeeper;ssl=true;sslTrustStore=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks;transportMode=http;trustStorePassword=aBTFUp2I89rU8xIc09j2603kJg;zooKeeperNamespace=hiveserver2"
fi


copy_to_s3() {
  local SOURCE_DIR=$1
  local s3_path=$2

  mkdir -p "$SOURCE_DIR"/sqls
  cp "$SOURCE_DIR"/*.sql "$SOURCE_DIR"/sqls/

  log_message "Copying ${SOURCE_DIR} to S3 at ${s3_path}..."

  hdfs dfs -mkdir -p "$s3_path"

  # Copy files from local to S3
  hdfs dfs -put "$SOURCE_DIR/sqls" "$s3_path/"


  if [ $? -ne 0 ]; then
    log_message "ERROR: Failed to copy $SOURCE_DIR to S3"
    exit 1
  fi
}

#Main logic



#databases=$(beeline -u "${JDBC_STRING}" -n $BEELINE_USER -p $BEELINE_PASSWORD -e 'show databases;' 2>/dev/null | grep -v 'INFO' | grep -v 'WARNING' | grep -v '+')
databases=$(beeline -u "${JDBC_STRING}" -e 'show databases;' 2>/dev/null | grep -v 'INFO' | grep -v 'WARNING' | grep -v '+')
database_list=$(echo "$databases" | awk -F "|" '/[[:space:]]*[a-zA-Z0-9_]+[[:space:]]*/{gsub(/[[:space:]]*/,"",$2); print $2}' | paste -sd "," -)
#Remove database_name header from the list of available databases
database_list=$(echo "$database_list" | sed -e "s/\bdatabase_name\b//g" -e "s/,,/,/g" -e "s/^,//" -e "s/,$//")
IFS=',' read -r -a db_array <<< "$database_list"

chunk_size=90
num_elements=${#db_array[@]}
num_chunks=$(( (num_elements + chunk_size - 1) / chunk_size ))
for (( i=0; i<num_chunks; i++ )); do
                start=$(( i * chunk_size ))
                end=$(( start + chunk_size ))
                # Slice the array to get the current chunk
                chunk=("${db_array[@]:start:chunk_size}")
                # Process the current chunk
                echo  ${chunk[@]}
                /usr/local/bin/hms-mirror -d SCHEMA_ONLY -db ${chunk[@]} -o ${LOCAL_BACKUP_DIR} -rid -epl -e -accept ALL
                sleep 180
done



chunk_size=90
num_elements=${#db_array[@]}
num_chunks=$(( (num_elements + chunk_size - 1) / chunk_size ))
for (( i=0; i<num_chunks; i++ )); do
                start=$(( i * chunk_size ))
                end=$(( start + chunk_size ))
                # Slice the array to get the current chunk
                chunk=("${db_array[@]:start:chunk_size}")
                # Process the current chunk
                echo  ${chunk[@]}
                /usr/local/bin/hms-mirror -d SCHEMA_ONLY -db ${chunk[@]} -v -o ${VIEWS_SOURCE_DIR} -rid -epl -e -accept ALL
                sleep 180
done


# Find all .sql files in the source directory
find "$VIEWS_SOURCE_DIR" -type f -name "*.sql" | while read -r file; do
    # Check if the file contains the text "CREATE VIEW"
    if grep -q "CREATE VIEW" "$file"; then
    # Move the file to the target directory
        mv "$file" "$VIEWS_TARGET_DIR"
        echo "Moved $file to $VIEWS_TARGET_DIR"
    fi
done


# Copy the SRC_TABLES SQL  to S3
copy_to_s3 "$LOCAL_BACKUP_DIR" "${S3_PATH}/SRC_TABLES"

# Copy the SRC_TABLES SQL  to S3
copy_to_s3 "$VIEWS_TARGET_DIR" "${S3_PATH}/VIEWS_SQL"

log_message "Backup and export completed successfully."
