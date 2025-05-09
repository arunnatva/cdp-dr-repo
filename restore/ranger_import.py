import os
import sys
import requests
import subprocess
import logging
import csv
import json
import re
from datetime import datetime

import configparser

# Function to read Input parameters from a File
def read_config(file_path):
    # Create a ConfigParser object
    config = configparser.ConfigParser()
    # Read the configuration file
    config.read(file_path)
    # Accessing the values
    ranger_config = {
        'RANGER_URL': config.get('ranger_config', 'RANGER_URL'),
        'USERNAME': config.get('ranger_config', 'USERNAME'),
        'PASSWORD': config.get('ranger_config', 'PASSWORD'),
        'BACKUP_LOCATION': config.get('ranger_config', 'BACKUP_LOCATION'),
        'DR_S3_BUCKET': config.get('ranger_config', 'DR_S3_BUCKET'),
        'PROD_S3_BUCKET': config.get('ranger_config', 'PROD_S3_BUCKET'),
        'MAPPING_FILE': config.get('ranger_config', 'MAPPING_FILE'),
        'PRIMARY': config.get('ranger_config', 'PRIMARY'),
        'DR': config.get('ranger_config', 'DR')
    }
    return ranger_config


# Function to display usage
def usage():
    print("Usage: python script.py <configuration-file>")
    print("Example: python script.py /home/service_ranger_dr_account/workspace/dr_test/restore/ranger_config.ini")
    sys.exit(1)

# Check if required arguments are provided
if len(sys.argv) != 2:
    usage()

# Populate input parameters from Config file passed as command line argument
ranger_config = read_config(sys.argv[1])

RANGER_URL = ranger_config['RANGER_URL']
USERNAME = ranger_config['USERNAME']
PASSWORD = ranger_config['PASSWORD']
BACKUP_LOCATION = ranger_config['BACKUP_LOCATION']
DR_S3_BUCKET = ranger_config['DR_S3_BUCKET']
PROD_S3_BUCKET = ranger_config['PROD_S3_BUCKET']
MAPPING_FILE = ranger_config['MAPPING_FILE']
PRIMARY = ranger_config['PRIMARY']
DR = ranger_config['DR']


print("printing all variables from config file: ")
print(RANGER_URL, USERNAME, PASSWORD, BACKUP_LOCATION, DR_S3_BUCKET, PROD_S3_BUCKET, MAPPING_FILE, PRIMARY, DR)


PORT = 6182  # Assuming Ranger is running on port 6182
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")
LOCAL_DIR = f"/tmp/ranger_exports/{TODAY_DATE}"
S3_PATH = f"s3a://{DR_S3_BUCKET}/{BACKUP_LOCATION}"
DEBUG_DIR = f"/tmp/ranger_imports/debug/{TODAY_DATE}"

# Validate URL
if not RANGER_URL.startswith(('http://', 'https://')):
    print("ERROR: Ranger URL must include http or https scheme.")
    usage()

# Ensure the URL includes the specified port
if ':' not in RANGER_URL.split('//')[1]:
    RANGER_URL = f"{RANGER_URL}:{PORT}"

# Clean up local import directory if it exists
if os.path.exists(LOCAL_DIR):
    subprocess.run(['rm', '-rf', LOCAL_DIR], check=True)

# Create local import and debug directories
os.makedirs(LOCAL_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(filename=os.path.join(DEBUG_DIR, 'script.log'), level=logging.INFO, format='%(asctime)s - %(message)s')
log_message = logging.info

# Function to replace production bucket name with DR bucket name in JSON content
def replace_bucket_name(content, prod_bucket, dr_bucket):
    if isinstance(content, dict):
        return {k: replace_bucket_name(v, prod_bucket, dr_bucket) for k, v in content.items()}
    elif isinstance(content, list):
        return [replace_bucket_name(i, prod_bucket, dr_bucket) for i in content]
    elif isinstance(content, str):
        return content.replace(prod_bucket, dr_bucket)
    return content

# Function to replace primary string with DR string in JSON content
def replace_string(content, primary, dr):
    if isinstance(content, dict):
        return {k: replace_string(v, primary, dr) for k, v in content.items()}
    elif isinstance(content, list):
        return [replace_string(i, primary, dr) for i in content]
    elif isinstance(content, str):
        return content.replace(primary, dr)
    return content

# Function to sanitize file names
def sanitize_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

# Function to get existing policy details by name
def get_existing_policy_by_name(service_name, policy_name):
    full_url = f"{RANGER_URL}/service/plugins/policies?serviceName={service_name}&policyName={policy_name}"
    headers = {'Content-Type': 'application/json'}
    response = requests.get(full_url, auth=(USERNAME, PASSWORD), headers=headers)

    if response.status_code == 200:
        existing_policies = response.json().get('policies', [])
        for existing_policy in existing_policies:
            if existing_policy['name'] == policy_name:
                return existing_policy
    return None

# Function to compare policy content
def is_policy_content_equal(existing_policy, new_policy):
    fields_to_compare = ['resources', 'policyItems', 'denyPolicyItems', 'allowExceptions', 'denyExceptions']
    for field in fields_to_compare:
        if existing_policy.get(field) != new_policy.get(field):
            return False
    return True

# Function to update policy
def update_policy(existing_policy, policy):
    policy_id = existing_policy['id']
    policy['id'] = policy_id  # Ensure the policy ID is set correctly
    policy['service'] = existing_policy['service']  # Ensure the service is not changed
    full_url = f"{RANGER_URL}/service/plugins/policies/{policy_id}"
    headers = {'Content-Type': 'application/json'}
    policy_json = json.dumps(policy, indent=2)
    
    # Log existing and new policy JSON for debugging
    existing_policy_json = json.dumps(existing_policy, indent=2)
    new_policy_json = json.dumps(policy, indent=2)
    print(f"Existing policy JSON:\n{existing_policy_json}")
    print(f"Incoming policy JSON:\n{new_policy_json}")

    response = requests.put(full_url, auth=(USERNAME, PASSWORD), headers=headers, data=policy_json)

    response_text = response.text
    response_status = response.status_code

    policy_name = sanitize_filename(policy.get("name", "unknown_policy"))
    with open(os.path.join(DEBUG_DIR, f"{policy_name}_update_api_response.json"), 'w') as f:
        f.write(response_text)

    with open(os.path.join(DEBUG_DIR, f"{policy_name}_update_http_status.txt"), 'w') as f:
        f.write(f"HTTP Status: {response_status}")

    if response_status != 200:
        log_message(f"Failed to update policy {policy_name} to Ranger (HTTP Status: {response_status})")
        print(f"Failed to update policy {policy_name} to Ranger (HTTP Status: {response_status})")
        print(f"API Response: {response_text}")
    else:
        log_message(f"Successfully updated policy {policy_name} to Ranger")
        print(f"Successfully updated policy {policy_name} to Ranger")



# Function to import policy
def import_policy(policy, prod_bucket, dr_bucket, primary, dr):
    # Log original policy JSON for debugging
    if 'id' in policy:
        del policy['id']
    original_policy_json = json.dumps(policy, indent=2)
    print(f"Original policy JSON:\n{original_policy_json}")
    
    policy = replace_string(policy, primary, dr)
    policy = replace_bucket_name(policy, prod_bucket, dr_bucket)

    existing_policy = get_existing_policy_by_name(policy['service'], policy['name'])
    if existing_policy:
        if is_policy_content_equal(existing_policy, policy):
            log_message(f"Policy with name {policy['name']} and matching content already exists. Skipping import...")
            print(f"Policy with name {policy['name']} and matching content already exists. Skipping import...")
            return
        else:
            log_message(f"Policy with name {policy['name']} already exists but content differs. Updating policy...")
            print(f"Policy with name {policy['name']} already exists but content differs. Updating policy...")
            update_policy(existing_policy, policy)
            return

    full_url = f"{RANGER_URL}/service/plugins/policies"
    log_message(f"Importing policy to Ranger...")
    print(f"Importing policy to Ranger...")
    log_message(f"Endpoint URL: {full_url}")

    # Print JSON content for debugging
    policy_json = json.dumps(policy, indent=2)
    print(f"Policy content:")
    print(policy_json)

    headers = {'Content-Type': 'application/json'}
    response = requests.post(full_url, auth=(USERNAME, PASSWORD), headers=headers, data=policy_json)

    response_text = response.text
    response_status = response.status_code

    policy_name = sanitize_filename(policy.get("name", "unknown_policy"))
    with open(os.path.join(DEBUG_DIR, f"{policy_name}_api_response.json"), 'w') as f:
        f.write(response_text)

    with open(os.path.join(DEBUG_DIR, f"{policy_name}_http_status.txt"), 'w') as f:
        f.write(f"HTTP Status: {response_status}")

    if response_status != 200:
        log_message(f"Failed to import policy {policy_name} to Ranger (HTTP Status: {response_status})")
        print(f"Failed to import policy {policy_name} to Ranger (HTTP Status: {response_status})")
        print(f"API Response: {response_text}")

        # Check if the error is due to a duplicate key constraint
        if 'duplicate key value violates unique constraint' in response_text:
            log_message(f"Duplicate policy detected. Deleting existing policy {policy_name} and retrying import.")
            delete_existing_policy_by_name(policy['id'])
            response = requests.post(full_url, auth=(USERNAME, PASSWORD), headers=headers, data=policy_json)
            response_text = response.text
            response_status = response.status_code

            with open(os.path.join(DEBUG_DIR, f"{policy_name}_retry_api_response.json"), 'w') as f:
                f.write(response_text)

            with open(os.path.join(DEBUG_DIR, f"{policy_name}_retry_http_status.txt"), 'w') as f:
                f.write(f"HTTP Status: {response_status}")

            if response_status != 200:
                log_message(f"Failed to re-import policy {policy_name} to Ranger after deletion (HTTP Status: {response_status})")
                print(f"Failed to re-import policy {policy_name} to Ranger after deletion (HTTP Status: {response_status})")
                print(f"API Response: {response_text}")
            else:
                log_message(f"Successfully re-imported policy {policy_name} to Ranger after deletion")
                print(f"Successfully re-imported policy {policy_name} to Ranger after deletion")
    else:
        log_message(f"Successfully imported policy {policy_name} to Ranger")
        print(f"Successfully imported policy {policy_name} to Ranger")


# Function to delete existing policy by name
def delete_existing_policy_by_name(policy_id):
    full_url = f"{RANGER_URL}/service/plugins/policies/{policy_id}"
    headers = {'Content-Type': 'application/json'}
    response = requests.delete(full_url, auth=(USERNAME, PASSWORD), headers=headers)

    if response.status_code == 200:
        log_message(f"Successfully deleted existing policy {policy_name} from Ranger")
        print(f"Successfully deleted existing policy {policy_name} from Ranger")
    else:
        log_message(f"Failed to delete existing policy {policy_name} from Ranger (HTTP Status: {response.status_code})")
        print(f"Failed to delete existing policy {policy_name} from Ranger (HTTP Status: {response.status_code})")
        print(f"API Response: {response.text}")


# Function to import data
def import_data(endpoint, input_file, primary, dr):
    full_url = f"{RANGER_URL}{endpoint}"
    log_message(f"Importing {input_file} to Ranger...")
    print(f"Importing {input_file} to Ranger...")
    log_message(f"Endpoint URL: {full_url}")

    # Print JSON content for debugging
    with open(input_file, 'r') as f:
        content = f.read()
        content = replace_string(content, primary, dr)
        content = replace_bucket_name(json.loads(content), PROD_S3_BUCKET, DR_S3_BUCKET)
        content_json = json.dumps(content, indent=2)
        print(f"Content of {input_file}:")
        print(content_json)

    headers = {'Content-Type': 'application/json'}
    response = requests.post(full_url, auth=(USERNAME, PASSWORD), headers=headers, data=content_json)

    response_text = response.text
    response_status = response.status_code
    
    input_file_sanitized = sanitize_filename(os.path.basename(input_file))
    with open(os.path.join(DEBUG_DIR, f"{input_file_sanitized}_api_response.json"), 'w') as f:
        f.write(response_text)

    with open(os.path.join(DEBUG_DIR, f"{input_file_sanitized}_http_status.txt"), 'w') as f:
        f.write(f"HTTP Status: {response_status}")

    if response_status != 200:
        log_message(f"Failed to import {input_file} to Ranger (HTTP Status: {response_status})")
        print(f"Failed to import {input_file} to Ranger (HTTP Status: {response_status})")
        print(f"API Response: {response_text}")
    else:
        log_message(f"Successfully imported {input_file} to Ranger")
        print(f"Successfully imported {input_file} to Ranger")

# Function to replace AD groups in JSON files
def replace_ad_groups(json_file):
    log_message(f"Replacing AD groups in {json_file} using mapping file {MAPPING_FILE}...")
    print(f"Replacing AD groups in {json_file} using mapping file {MAPPING_FILE}...")
    with open(MAPPING_FILE, 'r') as f:
        reader = csv.reader(f)
        mappings = [row for row in reader if len(row) == 2]

    with open(json_file, 'r') as f:
        content = f.read()

    for primary_group, dr_group in mappings:
        log_message(f"Replacing {primary_group} with {dr_group}...")
        print(f"Replacing {primary_group} with {dr_group}...")
        content = content.replace(primary_group, dr_group)

    with open(json_file, 'w') as f:
        f.write(content)

    log_message(f"Finished replacing AD groups in {json_file}.")
    print(f"Finished replacing AD groups in {json_file}.")

# Function to remove HTTP headers from JSON file
def remove_http_headers(json_file):
    log_message(f"Removing HTTP headers from {json_file}...")
    print(f"Removing HTTP headers from {json_file}...")
    with open(json_file, 'r') as f:
        lines = f.readlines()

    start_line = next((i for i, line in enumerate(lines) if line.startswith('{')), None)
    if start_line is None:
        log_message(f"No JSON content found in {json_file}")
        print(f"No JSON content found in {json_file}")
        sys.exit(1)

    with open(json_file, 'w') as f:
        f.writelines(lines[start_line:])

    log_message(f"Finished removing HTTP headers from {json_file}.")
    print(f"Finished removing HTTP headers from {json_file}.")

# Function to copy from S3 to local using hdfs get
def copy_from_s3(s3_file, local_path):
    log_message(f"Copying {s3_file} from S3 to {local_path}...")
    print(f"Copying {s3_file} from S3 to {local_path}...")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    result = subprocess.run(['hdfs', 'dfs', '-get', s3_file, local_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode != 0:
        log_message(f"ERROR: Failed to copy {s3_file} from S3")
        print(f"ERROR: Failed to copy {s3_file} from S3")
        log_message(f"STDOUT: {result.stdout}")
        print(f"STDOUT: {result.stdout}")
        log_message(f"STDERR: {result.stderr}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)
    else:
        log_message(f"Successfully copied {s3_file} to {local_path}")
        print(f"Successfully copied {s3_file} to {local_path}")

# Function to get existing role details by name
def get_existing_role_by_name(role_name):
    full_url = f"{RANGER_URL}/service/roles/roles"
    headers = {'Content-Type': 'application/json'}
    response = requests.get(full_url, auth=(USERNAME, PASSWORD), headers=headers)

    if response.status_code == 200:
        existing_roles = response.json().get('roles', [])
        for existing_role in existing_roles:
            if existing_role['name'] == role_name:
                return existing_role
    return None

# Function to compare role content
def is_role_content_equal(existing_role, new_role):
    existing_groups = existing_role.get('groups', [])
    new_groups = new_role.get('groups', [])
    return existing_groups == new_groups


# Function to update role
def update_role(existing_role, role):
    role_id = existing_role['id']
    role['id'] = role_id  # Ensure the role ID is set correctly
    full_url = f"{RANGER_URL}/service/roles/roles/{role_id}"
    headers = {'Content-Type': 'application/json'}
    role_json = json.dumps(role, indent=2)
    response = requests.put(full_url, auth=(USERNAME, PASSWORD), headers=headers, data=role_json)

    response_text = response.text
    response_status = response.status_code

    role_name = sanitize_filename(role.get("name", "unknown_role"))
    with open(os.path.join(DEBUG_DIR, f"{role_name}_update_api_response.json"), 'w') as f:
        f.write(response_text)

    with open(os.path.join(DEBUG_DIR, f"{role_name}_update_http_status.txt"), 'w') as f:
        f.write(f"HTTP Status: {response_status}")

    if response_status != 200:
        log_message(f"Failed to update role {role_name} to Ranger (HTTP Status: {response_status})")
        print(f"Failed to update role {role_name} to Ranger (HTTP Status: {response_status})")
        print(f"API Response: {response_text}")

        # Check if the error message contains information about a missing group
        missing_group_match = re.search(r'Group with name: (\S+) does not exist', response_text)
        if missing_group_match:
            missing_group = missing_group_match.group(1)
            with open(os.path.join(DEBUG_DIR, 'missing_ad_groups.txt'), 'a') as f:
                f.write(f"{missing_group}\n")
    else:
        log_message(f"Successfully updated role {role_name} to Ranger")
        print(f"Successfully updated role {role_name} to Ranger")


# Function to import role
def import_role(role):
    # Remove the id field to allow Ranger to auto-generate a new ID
    if 'id' in role:
        del role['id']

    if 'guid' in role:
        del role['guid']
    
    # Remove spaces in group names
    role = remove_spaces_in_group_names(role)

    existing_role = get_existing_role_by_name(role['name'])
    if existing_role:
        if is_role_content_equal(existing_role, role):
            log_message(f"Role with name {role['name']} and matching content already exists. Skipping import...")
            print(f"Role with name {role['name']} and matching content already exists. Skipping import...")
            return
        else:
            log_message(f"Role with name {role['name']} already exists but content differs. Updating role...")
            print(f"Role with name {role['name']} already exists but content differs. Updating role...")
            update_role(existing_role, role)
            return

    full_url = f"{RANGER_URL}/service/roles/roles"
    log_message(f"Importing role {role['name']} to Ranger...")
    print(f"Importing role {role['name']} to Ranger...")
    log_message(f"Endpoint URL: {full_url}")

    headers = {'Content-Type': 'application/json'}
    role_json = json.dumps(role, indent=2)
    response = requests.post(full_url, auth=(USERNAME, PASSWORD), headers=headers, data=role_json)

    response_text = response.text
    response_status = response.status_code

    role_name = sanitize_filename(role.get("name", "unknown_role"))
    with open(os.path.join(DEBUG_DIR, f"{role_name}_api_response.json"), 'w') as f:
        f.write(response_text)

    with open(os.path.join(DEBUG_DIR, f"{role_name}_http_status.txt"), 'w') as f:
        f.write(f"HTTP Status: {response_status}")

    if response_status != 200:
        log_message(f"Failed to import role {role_name} to Ranger (HTTP Status: {response_status})")
        print(f"Failed to import role {role_name} to Ranger (HTTP Status: {response_status})")
        print(f"API Response: {response_text}")

        # Check if the error message contains information about a missing group
        missing_group_match = re.search(r'Group with name: (\S+) does not exist', response_text)
        if missing_group_match:
            missing_group = missing_group_match.group(1)
            with open(os.path.join(DEBUG_DIR, 'missing_ad_groups.txt'), 'a') as f:
                f.write(f"{missing_group}\n")

        missing_service_match = re.search(r'no service found with name\[(\S+)\]', response_text)
        if missing_service_match:
            missing_service = missing_service_match.group(1)
            with open(os.path.join(DEBUG_DIR, 'missing_service.txt'), 'a') as f:
                f.write(f"{missing_service}\n")        
    else:
        log_message(f"Successfully imported role {role_name} to Ranger")
        print(f"Successfully imported role {role_name} to Ranger")



def remove_spaces_in_group_names(content):
    def strip_whitespace(s):
        return ''.join(s.split())

    if isinstance(content, dict):
        for k, v in content.items():
            if k == 'groups' and isinstance(v, list):
                content[k] = [{'name': strip_whitespace(group['name']), 'isAdmin': group['isAdmin']} for group in v if isinstance(group, dict)]
            else:
                content[k] = remove_spaces_in_group_names(v)
    elif isinstance(content, list):
        return [remove_spaces_in_group_names(i) for i in content]
    return content



def kinit(principal, password):
    try:
        process = subprocess.Popen(['kinit', principal], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = process.communicate(input=password)
        if process.returncode == 0:
            print("Kerberos authentication successful")
        else:
            print(f"Kerberos authentication failed: {stderr}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Copy the exported JSON files from S3 to local
kinit(USERNAME, PASSWORD)
json_files = ["policies.json", "roles.json"]
for file in json_files:
    copy_from_s3(f"{S3_PATH}/{file}", f"{LOCAL_DIR}/{file}")

# Remove HTTP headers and replace AD groups in JSON files
for file in json_files:
    local_file = f"{LOCAL_DIR}/{file}"
    remove_http_headers(local_file)
    replace_ad_groups(local_file)

# Import JSON files to Ranger
# Import roles one by one
roles_file = f"{LOCAL_DIR}/roles.json"
with open(roles_file, 'r') as f:
    roles = json.load(f)
    for role in roles['roles']:
        import_role(role)

# Import policies one by one
policies_file = f"{LOCAL_DIR}/policies.json"
with open(policies_file, 'r') as f:
    policies = json.load(f)
    for policy in policies['policies']:
        import_policy(policy, PROD_S3_BUCKET, DR_S3_BUCKET, PRIMARY, DR)

# Clean up local import directory
subprocess.run(['rm', '-rf', LOCAL_DIR], check=True)
log_message("Restore completed successfully.")
print("Restore completed successfully.")

