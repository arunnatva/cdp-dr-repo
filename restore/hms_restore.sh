#!/bin/bash
# Function to display usage
usage() {
  echo "Usage: $0 <directory> <workload-username> <path-to-keytab-file> <INPUT_DATE_FOLDER>"
  echo "Example: $0 hms_restore_20240722 service_hms_dr_account /path/to/service_hms_dr_account.keytab 2024-07-22"
  exit 1
}

# Check if required arguments are provided
if [ "$#" -ne 4 ]; then
  usage
fi

# Assign the directory argument to a variable

INPUT_DATE_FOLDER=$4
RESTORE_DIR="${1}_${INPUT_DATE_FOLDER}"
#REPAIR_FLAG=false
BEELINE_USER=$2
BEELINE_KEYTAB=$3
LOG_FILE="sql_execution.log"
TEMP_OUTPUT="temp_output.log"

echo -ne "Restore_dir $RESTORE_DIR"
echo -ne "INPUT_DATE_FOLDER $INPUT_DATE_FOLDER"
echo -ne "all prameters - $@"

log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}" >> "${RESTORE_DIR}/script.log"
}


run_dir_check()
{
# Check if the provided argument is a directory
if [  -d "$RESTORE_DIR" ]; then
  log_message "Directory exists. Deleting..."
  rm -rf ${RESTORE_DIR}
  else
  log_message " Directory does not exists so creating the same"
  mkdir -p ${RESTORE_DIR}
  #initialize log file
  touch ${RESTORE_DIR}/script.log
fi
}

run_kinit()
{ 

  if ! klist -s
  then
	#do  kinit
	kinit -kt "$BEELINE_KEYTAB" "$BEELINE_USER"
	klist
  else
	log_message "Kerberos token is already present"
	klist
  fi
}


copy_to_local_from_s3()
{
  S3_PATH="s3a://cdp-test-dr-cluster-cdp-001/data/dr_backups/hms/${INPUT_DATE_FOLDER}"
  hdfs dfs -test -d $S3_PATH
  if [ $? -eq 1 ]; then
	log_message "HMS backup Directory does not exists"
	exit 1
  fi
  log_message "Downloading the HMS backup file to $RESTORE_DIR"
  hadoop fs -copyToLocal $S3_PATH $RESTORE_DIR
  #hadoop fs -copyToLocal s3a://cdp-test-dr-cluster-cdp-001/data/dr_backups/hms//<Date>/ /tmp/hms-restore/

}

alter_purge_property()
{
log_message "In Alter Table Property module"
log_message "Current Directory is ${RESTORE_DIR}"

beeline -e "use sys;select d.name,t.tbl_name from dbs d,tbls t where t.db_id = d.db_id and t.tbl_type = 'EXTERNAL_TABLE' and (d.name like '%_p2%' or d.name like '%_c2%')" > output.txt
cat output.txt | grep -v "d.name" | grep -v "+" | grep -v "kudu" | awk '{print $2"."$4}' > table_list.txt

for table in $(cat table_list.txt) ; do
        echo "ALTER TABLE $table SET TBLPROPERTIES ('external.table.purge' = 'false');"
done > alter_table.hql

# run the alter table command
beeline -f alter_table.hql
}

restore_process()
{

log_message "In Restore Process module"
log_message "Loop through all files in the directory and apply the sed commands"
log_message "RESTORE_DIR is ${RESTORE_DIR}"


for file in "$RESTORE_DIR"/*/*/*/*.sql ; do
  if [ -f "$file" ]; then
    log_message "Processing $file"
    sed -i '/__HIVE_/d' "$file"
	
	
	#ALTER TABLE <table> SET TBLPROPERTIES('external.table.purge'='false');-Ensuring data is not deleted
	#DROP table IF exists- Cleanup step
	#Create table - Schema creation
	
    sed -i -E 's/CREATE EXTERNAL TABLE `([^`]*)`\(/\DROP TABLE IF EXISTS `\1`;\nCREATE EXTERNAL TABLE IF NOT EXISTS `\1`\(/g' "$file"
   	sed -i 's/CREATE VIEW `/CREATE VIEW IF NOT EXISTS `/g' "$file"
    sed -i -E ':a; /ALTER TABLE [^ ]+ ADD IF NOT EXISTS/{N; /;/!ba; s/(ALTER TABLE ([^ ]+) ADD IF NOT EXISTS[^;]*);/MSCK REPAIR TABLE \2 sync PARTITIONS;\nALTER TABLE \2 SET TBLPROPERTIES ("external.table.purge"="false");/}' "$file"
	
  fi
done

log_message "Loop through each SQL file in the directory for TABLES"
for sql_file in "$RESTORE_DIR"/*/SRC_TABLES/*/*.sql; do
  log_message "Running script: $sql_file" 
  beeline -f "$sql_file" 
done
log_message "SQL script execution completed for TABLES."


log_message "Loop through each SQL file in the directory for VIEWS"
sql_file="$RESTORE_DIR/VIEWS_SQL/*/*.sql"
for sql_file in "$RESTORE_DIR"/*/VIEWS_SQL/*/*.sql; do
  # Echo the current SQL file being executed
  log_message "Running script: $sql_file" 
  # Run beeline for each file
 beeline -f "$sql_file" 
done
log_message "SQL script execution completed for VIEWS."
}

run_kinit
run_dir_check
copy_to_local_from_s3
alter_purge_property
restore_process
