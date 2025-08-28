import os
import requests
from dotenv import load_dotenv
from getDBConnection import create_db_connection

# Load environment variables
load_dotenv('config.env')

# Global list to store tokens
token_cache = []

def fetch_warehouse_id(connection, tenant):
    
    query = "SELECT id FROM warehouse WHERE is_setup = 1 AND tenant = %s LIMIT 1" if not tenant.startswith('ar') else "SELECT id FROM arsenal WHERE is_setup = 1 AND tenant = %s LIMIT 1"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (tenant))
            result = cursor.fetchone()
            return result[0]
    except Exception as e:
        print(f"Failed to fetch warehouse ID for tenant {tenant}: {e}")
        return None

def switch_token(warehouse_id, tenant , warehouse_type = 'warehouse'):
    url = f'https://wms.mercuryonline.co/api/user/auth/switch/{warehouse_type}/{warehouse_id}'    
    headers = {
        'Authorization': f'{os.getenv("EPR_TOKEN")}',
        'Content-Type': 'application/json'
    }
    try:
        print(f"Switching to tenant {tenant}")
        response = requests.post(url, headers=headers, timeout=10)
        if response.status_code == 200:
            token = response.json()['token']
            print(f"Token obtained for tenant: {tenant}")
            # Store token in list
            token_cache.append({
                'tenant': tenant,
                'token': token,
                'warehouse_id': warehouse_id
            })
            return token
        else:
            print(f"Error switching to tenant {tenant}: {response.status_code} - {response.text}")
            return None
    except requests.RequestException as e:
        print(f"Exception while switching tenant {tenant}: {e}")
        return None

def get_token_for_tenant(tenant):
    # Check if token already exists in cache
    for cached_token in token_cache:
        if cached_token['tenant'] == tenant:
            print(f"Using cached token for tenant: {tenant}")
            return cached_token['token']
    
    # If not in cache, fetch new token
    print(f"Fetching new token for tenant: {tenant}")
    connection = create_db_connection('mercury')
    warehouse_id = fetch_warehouse_id(connection, tenant)
    connection.close()
    warehouse_type = 'arsenal' if tenant.startswith('ar') else 'warehouse'
    if warehouse_id:
        return switch_token(warehouse_id, tenant , warehouse_type)
    else:
        print(f"No warehouse found for tenant: {tenant}")
        return None

def get_cached_tokens():
    return token_cache

def clear_token_cache():
    token_cache.clear()
    print("Token cache cleared") 