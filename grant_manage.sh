#!/bin/bash
ADMIN_TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials&client_id=admin&client_secret=password&scope=PRINCIPAL_ROLE:ALL' \
  | jq -r '.access_token')

# Grant catalog_manage_access
curl -s -X PUT http://localhost:8181/api/management/v1/principal-roles/clickhouse_role/catalog-roles/lakehouse \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "catalog_manage_access"}'

echo "Granted catalog_manage_access"
