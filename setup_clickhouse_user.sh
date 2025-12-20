#!/bin/bash
set -e

# 1. Get Admin Token
echo "Obtaining Admin Token..."
ADMIN_TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=admin&client_secret=password&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')

if [ "$ADMIN_TOKEN" == "null" ]; then
  echo "Failed to get admin token"
  exit 1
fi

# 2. Create Principal 'clickhouse_user'
echo "Creating Principal 'clickhouse_user'..."
CREATE_RESPONSE=$(curl -s -X POST http://localhost:8181/api/management/v1/principals \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "clickhouse_user",
    "type": "SERVICE",
    "clientId": "clickhouse_user",
    "credential": { "main": "clickhouse_secret" }
  }')

echo "$CREATE_RESPONSE"

# Extract credentials (if creation was successful)
CLIENT_ID=$(echo "$CREATE_RESPONSE" | jq -r '.credentials.clientId // empty')
CLIENT_SECRET=$(echo "$CREATE_RESPONSE" | jq -r '.credentials.clientSecret // empty')

if [ -n "$CLIENT_ID" ] && [ -n "$CLIENT_SECRET" ]; then
  echo "Created user with ID: $CLIENT_ID"
  # Update create_db.sql with new credentials
  sed -i "s/catalog_credential = '.*'/catalog_credential = '$CLIENT_ID:$CLIENT_SECRET'/" create_db.sql
  echo "Updated create_db.sql with new credentials."
else
  echo "Warning: Could not extract credentials (maybe user already exists?). Using existing create_db.sql config."
fi

# 3. Create Principal Role 'clickhouse_role'
echo -e "\nCreating Principal Role 'clickhouse_role'..."
curl -s -X POST http://localhost:8181/api/management/v1/principal-roles \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "clickhouse_role"}'

# 4. Assign Role to Principal
echo -e "\nAssigning 'clickhouse_role' to 'clickhouse_user'..."
curl -s -X PUT http://localhost:8181/api/management/v1/principals/clickhouse_user/principal-roles \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "clickhouse_role"}'

# 5. Grant TABLE_READ_DATA and TABLE_WRITE_DATA on catalog 'lakehouse' to 'catalog_admin' role
# This ensures that the role has actual privileges.
echo -e "\nGranting privileges to 'catalog_admin' role in 'lakehouse'..."

# TABLE_READ_DATA
curl -s -X PUT http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "catalog", "privilege": "TABLE_READ_DATA"}'

# TABLE_WRITE_DATA (as suggested by the issue)
curl -s -X PUT http://localhost:8181/api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "catalog", "privilege": "TABLE_WRITE_DATA"}'

# 6. Assign 'catalog_admin' role to 'clickhouse_role'
echo -e "\nAssigning 'catalog_admin' (catalog role) to 'clickhouse_role' (principal role)..."
curl -s -X PUT http://localhost:8181/api/management/v1/principal-roles/clickhouse_role/catalog-roles/lakehouse \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "catalog_admin"}'

echo -e "\nDone."
