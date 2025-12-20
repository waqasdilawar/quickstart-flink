CREATE DATABASE demo
  ENGINE = DataLakeCatalog('http://polaris:8181/api/catalog/v1')
    SETTINGS
      catalog_type = 'rest',
      storage_endpoint = 'http://minio:9000/',
      warehouse = 'lakehouse',
      catalog_credential = '0ddd91466e5af269:548c2c6457077be724af6c6b343089d3',
      oauth_server_uri = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
      auth_scope = 'PRINCIPAL_ROLE:clickhouse_role',
      aws_access_key_id = 'admin',
      aws_secret_access_key = 'password';
