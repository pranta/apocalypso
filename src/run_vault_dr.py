#!/usr/bin/env python3
#--------------------------------------------------------------------------------
# """run_vault_dr.py: Runs Vault Disaster Recovery"""
# __author__      = "Pranta Das"
# __copyright__   = "(C) Copyright 2020, Hulu"
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------------
import sys, os, requests, json, pdb, base64, getpass, time, random, configparser, boto3

debug = False

# HTTP verbs
GET='GET'
POST='POST'
DELETE='DELETE'

#--------------------------------------------------------------------------------
# Functions to print and check usage validating correct number of arguments
# and correctly set environment variables.
#--------------------------------------------------------------------------------

def print_usage():
    print ("Usage:", sys.argv[0], "{failover|failback} {prod|staging|test}")
    sys.exit()

def check_usage():
# Check for the correct number of arguments. If no argument is specified, error out.
  if len(sys.argv) <= 2:
    print ("Error: Too few or incorrect arguments.")
    print_usage()

#--------------------------------------------------------------------------------
# Function to XOR 2 similar length strings used to
# decode the encoded DR token, returned from Vault,
# and XOR it to the OTP
#--------------------------------------------------------------------------------
def xor_bytes(s1, s2): 
  if len(s1) != len(s2):
    return None
  s1_array = bytearray(s1, 'utf-8')
  s2_array = bytearray(s2, 'utf-8')
  buf = bytearray(s1, 'utf-8')
  for i in range(len(s1)):
    buf[i] = s1_array[i] ^ s2_array[i]
  return buf.decode()

#--------------------------------------------------------------------------------
# Function to make HTTP requests
#--------------------------------------------------------------------------------
def http_request(session, verb, url, payload, hdrs):
  MAX_RETRIES=10  
  num_retries = 0
  while num_retries <= MAX_RETRIES:
    try:
      if session == None:
        response = requests.request(verb, url, json=payload, headers=hdrs, verify=False)
      else:
        response = session.request(verb, url, data=payload, headers=hdrs, verify=False)
      response.raise_for_status()
      return response.json()
    except json.decoder.JSONDecodeError as jde:
      return response
    except requests.exceptions.HTTPError as errh:
        print ("HTTPError:", errh, "occurred while executing",verb,":",url)
        print ("Details:", response.text)
        num_retries += 1
        if num_retries > MAX_RETRIES: 
          break
        backoff_before_retry(num_retries)
        continue
    except requests.exceptions.ConnectionError as errc:
        print ("ConnectionError:", errc, "occurred while executing", verb,":",url)
        print ("Details:", response.text)
        if hasattr(errc, 'message'):
          print ("Message:", errc.message)
        num_retries += 1
        if num_retries > MAX_RETRIES: 
          break
        backoff_before_retry(num_retries)
        continue
    except requests.exceptions.Timeout as errt:
        print ("Timeout:", errt, "occurred while executing", verb,":",url)
        print ("Details:", response.text)
        if hasattr(errt, 'message'):
          print ("Message:", errt.message)
        num_retries += 1
        if num_retries > MAX_RETRIES: 
          break
        backoff_before_retry(num_retries)
        continue
    except requests.exceptions.RequestException as err:
        print ("RequestException:", err, "occurred while executing", verb, ":",url)
        print ("Details:", response.text)
        if hasattr(err, 'message'):
          print ("Message:", err.message)
        num_retries += 1
        if num_retries > MAX_RETRIES: 
          break
        backoff_before_retry(num_retries)
        continue
  if num_retries > MAX_RETRIES:
    print("Exceeded maximum of", MAX_RETRIES, "retries.")
    print("Aborting script")
    sys.exit()

#--------------------------------------------------------------------------------
# Function to perform exponential backoff
#--------------------------------------------------------------------------------
def backoff_before_retry(num_retries):

  jitter = random.randint(1,999)/1000

  backoff_interval = 2**(num_retries-1)*30 + jitter

  print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
        "*** Retry attempt",num_retries,"after exponential backoff of", 
        str(format(backoff_interval,'.3f')), "seconds")

  time.sleep(backoff_interval)

  return

#--------------------------------------------------------------------------------
# Function to change the CNAME of the Vault Cluster in AWS Route 53
#--------------------------------------------------------------------------------
def update_cname_record(hosted_zone_id, source, target, aws_aki, aws_sk):

  client = boto3.client( 'route53',
                        aws_access_key_id=aws_aki,
                        aws_secret_access_key=aws_sk)

  try:
    response = client.change_resource_record_sets(HostedZoneId=hosted_zone_id,
                                  ChangeBatch= {
                                     'Comment': 'update %s -> %s' % (source, target),
                                     'Changes': [{
                                       'Action': 'UPSERT',
                                       'ResourceRecordSet': {
                                         'Name': source,
                                         'Type': 'CNAME',
                                         'TTL': 300,
                                         'ResourceRecords': [{'Value': target}]
                                       }
                                     }]
                                  })
  except Exception as e:
    print(e)
    sys.exit()
#--------------------------------------------------------------------------------
# Main program
#--------------------------------------------------------------------------------
check_usage()

# Disable all requests warnings
requests.packages.urllib3.disable_warnings()

# Check to see if the vault token environment variable is set
vault_token = os.getenv('VAULT_TOKEN')
# If not, prompt the operator for token
if vault_token == None:
  while vault_token == None or vault_token == '':
    vault_token = getpass.getpass(prompt="Enter Vault Token:")

# Get the AWS access key id and secret key
aws_aki = os.getenv('AWS_ACCESS_KEY_ID')
aws_sk = os.getenv('AWS_SECRET_KEY')

if aws_aki == None:
  while aws_aki == None or aws_aki == '':
    aws_aki = input("Enter AWS ACCESS KEY ID:") 
if aws_sk == None:
  while aws_sk == None or aws_sk == '':
    aws_sk = getpass.getpass("Enter AWS SECRET KEY:")

# Read the first argument as the dr_mode: 'failover' or 'failback'
dr_mode = sys.argv[1]
# Read the second argument as the environment
environment = sys.argv[2]

 
# Read the config file to get all the cluster domain names & DNS server.
config = configparser.RawConfigParser()
config.read('vault_dr.cfg')

# Read variables for primary and secondary domains based on environment and dr_mode
prod_primary_vault_cluster_domain = config.get('Vault-Cluster-Prod', 
                                               'prod_primary_vault_cluster_domain')

prod_secondary_vault_cluster_domain = config.get('Vault-Cluster-Prod', 
                                               'prod_secondary_vault_cluster_domain')
prod_cluster_cname = config.get('Vault-Cluster-Prod', 'prod_cluster_cname')

staging_primary_vault_cluster_domain = config.get('Vault-Cluster-Staging', 
                                               'staging_primary_vault_cluster_domain')

staging_secondary_vault_cluster_domain = config.get('Vault-Cluster-Staging', 
                                               'staging_secondary_vault_cluster_domain')
staging_cluster_cname = config.get('Vault-Cluster-Staging', 'staging_cluster_cname')

test_primary_vault_cluster_domain = config.get('Vault-Cluster-Test', 
                                               'test_primary_vault_cluster_domain')

test_secondary_vault_cluster_domain = config.get('Vault-Cluster-Test', 
                                               'test_secondary_vault_cluster_domain')
test_cluster_cname = config.get('Vault-Cluster-Test', 'test_cluster_cname')

# Read the Route 53 Hosted Zone ID for the Vault cluster
vault_cluster_zone_id = config.get('AWS-Route-53', 'HostedZoneID')

# Depending on the dr_mode, assign the primary and secondary clusters based on environment
if dr_mode == 'failover':
  if environment == 'prod': 
    primary_vault_cluster_domain=prod_primary_vault_cluster_domain
    secondary_vault_cluster_domain=prod_secondary_vault_cluster_domain
    cluster_cname=prod_cluster_cname
  elif environment == 'staging':
    primary_vault_cluster_domain=staging_primary_vault_cluster_domain
    secondary_vault_cluster_domain=staging_secondary_vault_cluster_domain
    cluster_cname=staging_cluster_cname
  elif environment == 'test':
    primary_vault_cluster_domain=test_primary_vault_cluster_domain
    secondary_vault_cluster_domain=test_secondary_vault_cluster_domain
    cluster_cname=test_cluster_cname
  else:
    print_usage()
elif dr_mode == 'failback':
  if environment == 'prod': 
    secondary_vault_cluster_domain=prod_primary_vault_cluster_domain
    primary_vault_cluster_domain=prod_secondary_vault_cluster_domain
    cluster_cname=prod_cluster_cname
  elif environment == 'staging':
    secondary_vault_cluster_domain=staging_primary_vault_cluster_domain
    primary_vault_cluster_domain=staging_secondary_vault_cluster_domain
    cluster_cname=staging_cluster_cname
  elif environment == 'test':
    secondary_vault_cluster_domain=test_primary_vault_cluster_domain
    primary_vault_cluster_domain=test_secondary_vault_cluster_domain
    cluster_cname=test_cluster_cname
  else:
    print_usage()
else:
  print_usage()

# Set the vault token
hdrs = {'X-Vault-Token': vault_token } 

#---------------------------------------------------------------------------------------
# Note: STEPS 1 & 2 (ENABLING DR REPLICATION ON PRIMARY & SECONDARY) are assumed to be
# already done either via the UI or via CLI or API.  The rest of the steps are 
# implemented here.
#---------------------------------------------------------------------------------------
# STEP 3: PROMOTE DR SECONDARY TO PRIMARY
#---------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------
# Step 3-A: Ensure that the secondary has replication in the correct state
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
     "*** About to check the replication status on the secondary cluster:", 
     secondary_vault_cluster_domain)

url = secondary_vault_cluster_domain + '/v1/sys/replication/dr/status'
  
payload =  {}

if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)

response = http_request(None, GET, url, payload, hdrs)

if debug:
  print(response)

# The response from the secondary will be a JSON document like this:
#{
#  "data": {
#    "cluster_id": "d4095d41-3aee-8791-c421-9bc7f88f7c3e",
#    "known_primary_cluster_addrs": [
#      "https://127.0.0.1:8201"
#    ],
#    "last_remote_wal": 241,
#    "merkle_root": "56794a98e52598f35974024fba6691f047e772e9",
#    "mode": "secondary",
#    "primary_cluster_addr": "https://127.0.0.1:8201",
#    "secondary_id": "3",
#    "state": "stream-wals"
#  },
#}

response_dict = json.loads(json.dumps(response))

repl_mode = response_dict.get('data').get('mode')
repl_state = response_dict.get('data').get('state')

if repl_mode != 'secondary': 
  print("Error: The current secondary vault cluster", secondary_vault_cluster_domain,
        "has a replication mode of", repl_mode)
  print("Aborting script")
  sys.exit()
if repl_state != 'stream-wals':
  print("Error: The current secondary vault cluster", secondary_vault_cluster_domain,
        "has a replication state of", repl_state)
  print("Aborting script")
  sys.exit()

#---------------------------------------------------------------------------------------
# Step 3-B: Cancel any DR token generation process on the secondary if any are active
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
   "*** About to cancel any active DR token generation process on the secondary cluster:", 
   secondary_vault_cluster_domain)

url = secondary_vault_cluster_domain +\
   '/v1/sys/replication/dr/secondary/generate-operation-token/attempt' 
  
payload =  {}

if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)

response = http_request(None, DELETE, url, payload, hdrs)

if debug:
  print(response)
#---------------------------------------------------------------------------------------
# Step 3-C: Start the DR operation token generation process
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
   "*** About to start the DR token generation process on the secondary cluster:", 
   secondary_vault_cluster_domain)

url = secondary_vault_cluster_domain +\
   '/v1/sys/replication/dr/secondary/generate-operation-token/attempt' 
  
payload =  {}

if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)
response = http_request(None, POST, url, payload, hdrs)

if debug:
  print(response)

# The response will be a JSON document like this:
#{
#  "started": true,
#  "nonce": "2dbd10f1-8528-6246-09e7-82b25b8aba63",
#  "progress": 0,
#  "required": 3,
#  "encoded_token": "",
#  "otp": "2vPFYG8gUSW9npwzyvxXMug0",
#  "otp_length" :24,
#  "complete": false
#}


response_dict = json.loads(json.dumps(response))
complete = response_dict.get('complete') 
otp = response_dict.get('otp')
nonce = response_dict.get('nonce')

payload =  {}
i = 1

#---------------------------------------------------------------------------------------
# Step 3-D: Continue the DR token generation process by prompting for the Recovery keys
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to continue the DR token generation process on the secondary cluster:", 
      secondary_vault_cluster_domain)

url = secondary_vault_cluster_domain + '/v1/sys/replication/dr/secondary/generate-operation-token/update' 

while complete == False:
# Check to see if the secondary vault recovery key environment variable is set
  unseal_key = os.getenv('VAULT_RECOVERY_KEY_'+str(i))
# If not, prompt for the recovery key
  if unseal_key == None:
    while unseal_key == None or unseal_key == '':
     unseal_key = getpass.getpass(prompt="Enter Vault Recovery Key " + str(i) + ":")

  os.putenv('VAULT_RECOVERY_KEY_'+str(i),unseal_key)
  payload = { "key": unseal_key, "nonce": nonce }

  if debug:
    print('URL=',url,'Payload=',payload,'Headers=',hdrs)
  response = http_request(None, POST, url, payload, hdrs)
  if debug:
    print(response)
  # The intermediate response will be a JSON document like this:
  #{
  #  "started": true,
  #  "nonce": "2dbd10f1-8528-6246-09e7-82b25b8aba63",
  #  "progress": 1 or 2,
  #  "required": 3,
  #  "encoded_token": "",
  #  "otp": "2vPFYG8gUSW9npwzyvxXMug0",
  #  "otp_length" :24,
  #  "complete": false
  #}

  response_dict = json.loads(json.dumps(response))
  # So, let's parse out the complete status
  complete = response_dict.get('complete') 
  i+=1
# The final response will be a JSON document like this:
#{
#  "started": true,
#  "nonce": "2dbd10f1-8528-6246-09e7-82b25b8aba63",
#  "progress": 3,
#  "required": 3,
#  "pgp_fingerprint": "",
#  "complete": true,
#  "encoded_token": "FPzkNBvwNDeFh4SmGA8c+w=="
#}
##Decode the encoded token
dr_operation_token = xor_bytes(base64.b64decode(response_dict.get('encoded_token') + '==').decode(),otp)

if dr_operation_token == None:
  print("Unable to decode a valid DR operation token. Length of decoded token is", 
         len(base64.b64decode(response_dict.get('encoded_token') + '==')),
         "while length of OTP is",len(otp),"- unable to perform XOR operation.")
  sys.exit()
  
#---------------------------------------------------------------------------------------
# Step 3-E: Finally promote the secondary to primary
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to promote the current secondary to a primary on the secondary cluster:", secondary_vault_cluster_domain)
payload = { "dr_operation_token": dr_operation_token }

url = secondary_vault_cluster_domain + '/v1/sys/replication/dr/secondary/promote' 

if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)
response = http_request(None, POST, url, payload, hdrs)
if debug:
  print(response)
#---------------------------------------------------------------------------------------
# STEP 4: UPDATE THE DNS CNAME TO POINT TO THE NEW PRIMARY
#---------------------------------------------------------------------------------------
# Update the CNAME of the Vault Cluster in AWS Route 53
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to change the CNAME", cluster_cname, "in DNS to point to the new primary", secondary_vault_cluster_domain)

update_cname_record(vault_cluster_zone_id, cluster_cname, secondary_vault_cluster_domain, aws_aki, aws_sk)

#---------------------------------------------------------------------------------------
# Sleep and wait for DNS changes to propagate
#---------------------------------------------------------------------------------------
dns_propagation_delay = os.getenv('DNS_PROPAGATION_DELAY')

if dns_propagation_delay == None:
  dns_propagation_delay = 60
else:
  dns_propagation_delay = int(dns_propagation_delay)

# Sleep for dns_propagation_delay seconds so that the new secondary is ready to be updated.
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to sleep for", dns_propagation_delay, 
       "seconds for DNS changes to propagate before demoting old primary", 
       primary_vault_cluster_domain)
time.sleep(dns_propagation_delay)

#---------------------------------------------------------------------------------------
# STEP 5: DEMOTE DR PRIMARY TO SECONDARY
#---------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------
# Step 5-A: Demote the primary to a secondary
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to demote the old primary to a secondary", primary_vault_cluster_domain)
hdrs = {'X-Vault-Token': vault_token } 
url = primary_vault_cluster_domain + '/v1/sys/replication/dr/primary/demote' 

if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)
response = http_request(None, POST, url, {}, hdrs)
if debug:
  print(response)

if environment == 'prod':
  secondary_id = primary_vault_cluster_domain[17:25]
elif environment == 'staging':
  secondary_id = primary_vault_cluster_domain[17:28]
elif environment == 'test':
  secondary_id = primary_vault_cluster_domain[17:25]

#---------------------------------------------------------------------------------------
# Step 5-B: Generate a new secondary activation token on the new secondary cluster
#---------------------------------------------------------------------------------------

print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to generate a new secondary-token from the new primary", secondary_vault_cluster_domain)

payload = { "id":  secondary_id+str(random.randint(1,9999999999)) }
# Concatenate the primary cluster domain and the token command to form the URL
url = secondary_vault_cluster_domain + '/v1/sys/replication/dr/primary/secondary-token'
##
# Send the POST request to generate a secondary token.
if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)
response = http_request(None, POST, url, payload, hdrs)

if debug:
  print(response)
# The response will be a JSON document like this:
#{ 
# "request_id": "",
# "lease_id": "",
# "lease_duration": 0,
# "renewable": false,
# "data": null,
# "warnings": null,
# "wrap_info": {
#   "token": "fb79b9d3-d94e-9eb6-4919-c559311133d6",
#   "ttl": 300,
#   "creation_time": "2016-09-28T14:41:00.56961496-04:00",
#   "wrapped_accessor": ""
# }
#} 

# So, let's parse out the secondary token
response_dict = json.loads(json.dumps(response))
secondary_token = response_dict.get('wrap_info').get('token')
#---------------------------------------------------------------------------------------
# Step 5-C: Generate a DR token on the new secondary cluster
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to start generating a new DR operation token on the new secondary", primary_vault_cluster_domain)
url = primary_vault_cluster_domain + '/v1/sys/replication/dr/secondary/generate-operation-token/attempt' 
  
payload =  {}

if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)
response = http_request(None, POST, url, payload, hdrs)
# The response will be a JSON document like this:
#{
#  "started": true,
#  "nonce": "2dbd10f1-8528-6246-09e7-82b25b8aba63",
#  "progress": 0,
#  "required": 3,
#  "encoded_token": "",
#  "otp": "2vPFYG8gUSW9npwzyvxXMug0",
#  "otp_length" :24,
#  "complete": false
#}

response_dict = json.loads(json.dumps(response))
# So, let's parse out the complete status, otp and nonce
complete = response_dict.get('complete') 
otp = response_dict.get('otp')
nonce = response_dict.get('nonce')

payload =  {}
i = 1

#---------------------------------------------------------------------------------------
# Step 5-D: Continue the DR token generation process by prompting for the Recovery keys
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to continue generating a new DR operation token on the new secondary", primary_vault_cluster_domain)
while complete == False:
  # Check to see if the secondary vault recovery key environment variable is set
  unseal_key = os.getenv('VAULT_RECOVERY_KEY_'+str(i))
  # If not, prompt for the recovery key
  if unseal_key == None:
    while unseal_key == None or unseal_key == '':
	      unseal_key = getpass.getpass(prompt="Enter Vault Recovery Key " + str(i) + ":")
  payload = { "key": unseal_key, "nonce": nonce }
  url = primary_vault_cluster_domain + '/v1/sys/replication/dr/secondary/generate-operation-token/update' 

  if debug:
    print('URL=',url,'Payload=',payload,'Headers=',hdrs)
  response = http_request(None, POST, url, payload, hdrs)
  # The intermediate response will be a JSON document like this:
  #{
  #  "started": true,
  #  "nonce": "2dbd10f1-8528-6246-09e7-82b25b8aba63",
  #  "progress": 1,
  #  "required": 3,
  #  "encoded_token": "",
  #  "otp": "2vPFYG8gUSW9npwzyvxXMug0",
  #  "otp_length" :24,
  #  "complete": false
  #}

  response_dict = json.loads(json.dumps(response))
  # So, let's parse out the complete status
  complete = response_dict.get('complete') 
  i+=1
# The final response will be a JSON document like this:
#{
#  "started": true,
#  "nonce": "2dbd10f1-8528-6246-09e7-82b25b8aba63",
#  "progress": 3,
#  "required": 3,
#  "pgp_fingerprint": "",
#  "complete": true,
#  "encoded_token": "FPzkNBvwNDeFh4SmGA8c+w=="
#}
##Decode the encoded token
dr_operation_token = xor_bytes(base64.b64decode(response_dict.get('encoded_token') + '==').decode(),otp)

#---------------------------------------------------------------------------------------
# Step 5-E: Update DR Secondary with new Secondary token
#---------------------------------------------------------------------------------------
print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** About to update the new secondary with the secondary token", primary_vault_cluster_domain)
payload = { "dr_operation_token": dr_operation_token, 
            "token": secondary_token,
            "primary_api_addr": "https://"+cluster_cname 
          }
url = primary_vault_cluster_domain + '/v1/sys/replication/dr/secondary/update-primary'
if debug:
  print('URL=',url,'Payload=',payload,'Headers=',hdrs)
response = http_request(None, POST, url, payload, hdrs)
if debug:
  print(response)


print(time.strftime("[%a, %d %b %Y %H:%M:%S]", time.localtime()),
      "*** Vault Disaster Recovery Operation Successful. Failed over from", 
        primary_vault_cluster_domain, "to", secondary_vault_cluster_domain)

#---------------------------------------------------------------------------------------
# End of program run_vault_dr.py
#---------------------------------------------------------------------------------------
