#!/bin/bash

# Function to display usage
usage() {
  echo "Usage: $0 <config file>"
  echo "Example: $0 /path/to/config.ini"
  exit 1
}

echo "number of arguments $#"
# Check if required arguments are provided
if [ "$#" -ne 1 ]; then
  usage
fi

source "$1"


#do  kinit
kinit -kt "$KEYTAB" "$PRINCIPAL"

DATE_FOLDER=$(date +%Y-%m-%d)
LOCAL_EXPORT_DIR="$HOME/ranger_exports/${DATE_FOLDER}"
S3_PATH="s3a://${S3_BUCKET}/data/dr_backups/ranger/${DATE_FOLDER}"
DEBUG_DIR="$HOME/ranger_exports/debug"

# Validate URL
if [[ ! ${RANGER_URL} =~ ^https?:// ]]; then
  echo "ERROR: Ranger URL must include http or https scheme."
  usage
fi

# Clean up local export directory if it exists
if [ -d "${LOCAL_EXPORT_DIR}" ]; then
  rm -rf ${LOCAL_EXPORT_DIR}
fi

# Create local export and debug directories
mkdir -p ${LOCAL_EXPORT_DIR} || { echo "ERROR: Failed to create local export directory"; exit 1; }
mkdir -p ${DEBUG_DIR} || { echo "ERROR: Failed to create debug directory"; exit 1; }

# Function to log messages
log_message() {
  local message=$1
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}"
  echo "$(date +'%Y-%m-%d %H:%M:%S') - ${message}" >> "${DEBUG_DIR}/script.log"
}

# Function to export data
export_data() {
  local endpoint=$1
  local output_file=$2
  local full_url="${RANGER_URL}:${PORT}${endpoint}"

  log_message "Exporting ${output_file} from Ranger..."
  log_message "Endpoint URL: ${full_url}"
  
  # Fetch the data after authentication
  local api_response=$(curl -ivk -H "Content-type:application/json" -u ${USERNAME}:${PASSWORD} -X GET "$full_url")

  # Save API response for debugging
  echo "$api_response" > "${DEBUG_DIR}/${output_file}_api_response.json"

  local http_status=$(curl -s -L --negotiate -u : -b cookies.txt -o /dev/null -w "%{http_code}" -X GET "$full_url")

  # Save HTTP status for debugging
  echo "HTTP Status: ${http_status}" > "${DEBUG_DIR}/${output_file}_http_status.txt"

  if [ $? -ne 0 ] || [ "${http_status}" -ne 200 ]; then
    log_message "ERROR: Failed to export ${output_file} from Ranger (HTTP Status: ${http_status})"
    exit 1
  else
    log_message "Successfully exported ${output_file} from Ranger"
    echo "$api_response" > "${LOCAL_EXPORT_DIR}/${output_file}"
  fi

  # Log the API response for debugging
  cat "${LOCAL_EXPORT_DIR}/${output_file}"
}

# Export Policies
export_data "/service/plugins/policies/exportJson" "policies.json"

# Export Users
export_data "/service/xusers/users" "users.json"

# Export Groups
export_data "/service/xusers/groups" "groups.json"

# Export Roles
export_data "/service/roles/roles" "roles.json"

# Remove existing files in S3 before copying
hdfs dfs -rm -r "${S3_PATH}"

# Function to copy from local to S3 using hdfs put
copy_to_s3() {
  local local_file=$1
  local s3_path=$2

  log_message "Copying ${local_file} to S3 at ${s3_path}..."

  hdfs dfs -mkdir "${s3_path}"

  # Copy files from local to S3
  hdfs dfs -put "${local_file}" "${s3_path}"

  if [ $? -ne 0 ]; then
    log_message "ERROR: Failed to copy ${local_file} to S3"
    exit 1
  fi
}

# Copy the exported JSON files to S3
copy_to_s3 "${LOCAL_EXPORT_DIR}/policies.json" "${S3_PATH}"
copy_to_s3 "${LOCAL_EXPORT_DIR}/users.json" "${S3_PATH}"
copy_to_s3 "${LOCAL_EXPORT_DIR}/groups.json" "${S3_PATH}"
copy_to_s3 "${LOCAL_EXPORT_DIR}/roles.json" "${S3_PATH}"

# Clean up local export directory
rm -rf "${LOCAL_EXPORT_DIR}" || { log_message "ERROR: Failed to clean up local export directory"; exit 1; }

log_message "Backup and export completed successfully."
