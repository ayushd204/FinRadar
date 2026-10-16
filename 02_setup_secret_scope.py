# Databricks notebook source
secret_scope = "finradar_scope"

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url  = ctx.apiUrl().getOrElse(None)     
api_token = ctx.apiToken().getOrElse(None)  # personal access token for this session

print(api_url)
print(api_token)  # handle securely, do not log in real code

# COMMAND ----------

import requests
import json

DATABRICKS_INSTANCE = api_url  # Replace with your workspace URL
DATABRICKS_TOKEN = api_token  # Replace with your PAT

scope_name = secret_scope  
backend_type = "DATABRICKS"     # Use "AZURE_KEYVAULT" if integrating with Key Vault


# COMMAND ----------

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope_name
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

# COMMAND ----------

if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")
else:
    print("Failed to create secret scope.")
    print("Status Code:", response.status_code)
    print("Response:", response.text)

#configure your kafka !

kafka_bootstrap_servers = "#####"
kafka_topic ="credit_card_transactions"
kafka_api_key ="####"
kafka_api_secret ="######"

kafka_connection_details = json.dumps({
    "bootstrap_servers": kafka_bootstrap_servers,
    "topic": kafka_topic,
    "api_key": kafka_api_key,
    "api_secret": kafka_api_secret
})


print(kafka_connection_details)

# COMMAND ----------

secret_key='kafka_connection_details'
secret_value=kafka_connection_details

#previously a scope was created and now we put the payload into the scope
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope_name,
    "key": secret_key,
    "string_value": secret_value
}


response = requests.post(url, headers=headers, data=json.dumps(payload))


if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope_name}'.")
else:
    print("Failed to put the secret details.")
    print("Status:", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

try:
    retrieved_json = dbutils.secrets.get(
        scope=scope_name,
        key='kafka_connection_details'
    )
    
    print("Secret retrieved successfully.")
    
    parsed = json.loads(retrieved_json)
    print("Parsed JSON:")
    print(parsed)
    
except Exception as e:
    print("Secret verification failed:")
    print(str(e))


# COMMAND ----------

import requests
import json

secret_key = 'gmail_api_key'       # Name of the secret entry
secret_value = 'tktb juyo rvcc ksjp' # Value to store securely

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope_name,
    "key": secret_key,
    "string_value": secret_value
}


response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope_name}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

