# Apache Iceberg & MinIO - Data Lake Deep Dive

> 📖 **See Also**: [Apache Polaris Catalog Documentation](../polaris/README-POLARIS.md) for in-depth coverage of the catalog service.

## Table of Contents
1. [Overview](#overview)
2. [Data Lake Architecture](#data-lake-architecture)
3. [Apache Iceberg Fundamentals](#apache-iceberg-fundamentals)
4. [Apache Polaris Catalog Overview](#apache-polaris-catalog-overview)
5. [MinIO Object Storage](#minio-object-storage)
6. [Flink Integration](#flink-integration)
7. [Dynamic Routing Implementation](#dynamic-routing-implementation)
8. [Schema & Partitioning](#schema--partitioning)
9. [Configuration Reference](#configuration-reference)

---

## Overview

This pipeline uses a modern **Data Lake** architecture consisting of three components:

| Component | Role | Why |
|-----------|------|-----|
| **Apache Iceberg** | Table format | ACID transactions, schema evolution, time travel |
| **Apache Polaris** | Catalog service | Vendor-neutral REST API for table metadata |
| **MinIO** | Object storage | S3-compatible local storage for Parquet files |

```mermaid
flowchart TB
    subgraph "Flink Job"
        IC[IcebergMessageEventSink]
        SF[IcebergSinkFunction]
        IC --> SF
    end
    
    subgraph "Data Lake"
        subgraph "Metadata Layer"
            POL[Apache Polaris<br/>REST Catalog]
            PG[(PostgreSQL<br/>Catalog DB)]
            POL --> PG
        end
        
        subgraph "Storage Layer"
            MIN[(MinIO<br/>S3-Compatible)]
            D1[/metadata/<br/>manifest files/]
            D2[/data/<br/>parquet files/]
            MIN --> D1
            MIN --> D2
        end
    end
    
    SF -->|"Register Table"| POL
    SF -->|"Write Parquet"| MIN
    POL -.->|"Track Location"| MIN
```

---

## Data Lake Architecture

### Separation of Concerns

```mermaid
graph LR
    subgraph "Compute Engines"
        FL[Flink]
        SP[Spark]
        TR[Trino]
        CH[ClickHouse]
    end
    
    subgraph "Catalog (Polaris)"
        CAT[REST API<br/>:8181]
        META[(Table Metadata)]
    end
    
    subgraph "Storage (MinIO)"
        S3[S3 API<br/>:9000]
        FILES[(Parquet Files)]
    end
    
    FL --> CAT
    SP --> CAT
    TR --> CAT
    CH --> CAT
    
    CAT --> META
    CAT --> S3
    S3 --> FILES
```

**Benefits:**
- **Multi-Engine Access**: Same tables readable by Flink, Spark, Trino, ClickHouse
- **Decoupled Scaling**: Scale compute independently from storage
- **Vendor Neutral**: No lock-in to specific cloud or engine

---

## Apache Iceberg Fundamentals

### What is Iceberg?

Apache Iceberg is a **table format** (not a database or engine) that sits between compute engines and storage. It provides:

1. **ACID Transactions**: Atomic writes, no partial results
2. **Schema Evolution**: Add/remove/rename columns without rewriting data
3. **Time Travel**: Query historical versions of data
4. **Partition Evolution**: Change partitioning strategy without rewriting
5. **Hidden Partitioning**: No need to know partition columns to query

### Iceberg Table Structure

```mermaid
graph TB
    subgraph "Iceberg Table: profanity_messages"
        CAT[Catalog Entry<br/>Polaris]
        
        subgraph "Metadata"
            M[metadata.json<br/>Current snapshot]
            S1[snapshot-1.avro<br/>Manifest list]
            S2[snapshot-2.avro<br/>Latest manifest list]
        end
        
        subgraph "Manifest Files"
            MF1[manifest-1.avro<br/>File tracking]
            MF2[manifest-2.avro<br/>File tracking]
        end
        
        subgraph "Data Files (Parquet)"
            P1[data-001.parquet]
            P2[data-002.parquet]
            P3[data-003.parquet]
        end
        
        CAT --> M
        M --> S1
        M --> S2
        S2 --> MF1
        S2 --> MF2
        MF1 --> P1
        MF1 --> P2
        MF2 --> P3
    end
```

### File Layout in MinIO

```
s3://lakehouse/
└── raw_messages/
    ├── profanity_messages/
    │   ├── metadata/
    │   │   ├── v1.metadata.json
    │   │   ├── v2.metadata.json
    │   │   └── snap-*.avro
    │   └── data/
    │       ├── timestamp_day=2025-01-15/
    │       │   ├── 00000-0-*.parquet
    │       │   └── 00001-0-*.parquet
    │       └── timestamp_day=2025-01-16/
    │           └── 00000-0-*.parquet
    └── safe_messages/
        ├── metadata/
        └── data/
```

---

## Apache Polaris Catalog Overview

> 📖 **For complete Polaris documentation**, see [README-POLARIS.md](../polaris/README-POLARIS.md)

### What is Polaris?

**Apache Polaris** is an open-source implementation of the Iceberg REST Catalog specification. It provides:

- **Centralized Metadata**: Single source of truth for all Iceberg tables
- **REST API**: Standard HTTP interface for table operations
- **OAuth2 Security**: Token-based authentication
- **RBAC**: Role-based access control for catalogs and tables

**Quick Summary:**
- Polaris stores table metadata in PostgreSQL
- Exposes REST API on port 8181
- Tracks file locations in MinIO/S3
- Handles authentication and authorization

For details on OAuth2 flow, RBAC configuration, API endpoints, and troubleshooting, refer to the [dedicated Polaris documentation](../polaris/README-POLARIS.md).

### Polaris Startup Sequence

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant Boot as polaris-bootstrap
    participant POL as Polaris
    participant Setup as polaris-setup
    participant Flink

    Note over PG: Container starts first
    PG->>PG: Initialize POLARIS database
    
    Note over Boot: Waits for PG healthy
    Boot->>PG: Apply schema migrations
    Boot->>PG: Create admin credentials
    Boot-->>Boot: Exit successfully
    
    Note over POL: Waits for bootstrap complete
    POL->>PG: Connect to database
    POL->>POL: Start REST API on :8181
    
    Note over Setup: Waits for Polaris healthy
    Setup->>POL: POST /oauth/tokens (get token)
    Setup->>POL: POST /catalogs (create 'lakehouse')
    Setup->>POL: POST /namespaces (create 'raw_messages')
    Setup->>POL: POST /tables (create 'profanity_messages')
    Setup->>POL: POST /tables (create 'safe_messages')
    
    Note over Flink: Waits for setup complete
    Flink->>POL: Connect via REST catalog
    Flink->>Flink: Start streaming to tables
```

### Polaris API Reference

> 📖 See [README-POLARIS.md - REST Catalog API](../polaris/README-POLARIS.md#rest-catalog-api) for complete API documentation with examples.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/catalog/v1/oauth/tokens` | POST | OAuth2 token exchange |
| `/api/management/v1/catalogs` | POST | Create catalog |
| `/api/catalog/v1/{catalog}/namespaces` | POST | Create namespace |
| `/api/catalog/v1/{catalog}/namespaces/{ns}/tables` | POST | Create table |
| `/api/catalog/v1/{catalog}/namespaces/{ns}/tables/{table}` | GET | Get table metadata |

### RBAC Configuration

> 📖 See [README-POLARIS.md - RBAC & Permissions](../polaris/README-POLARIS.md#rbac--permissions) for detailed permission model.

```mermaid
flowchart TB
    subgraph "Principals (Users)"
        ROOT[root<br/>Admin User]
        CH[clickhouse_user<br/>Service Account]
    end
    
    subgraph "Principal Roles"
        DE[data_engineer]
        CR[clickhouse_role]
    end
    
    subgraph "Catalog Roles"
        CA[catalog_admin]
    end
    
    subgraph "Privileges"
        P1[CATALOG_MANAGE_CONTENT]
        P2[TABLE_READ_DATA]
        P3[TABLE_WRITE_DATA]
    end
    
    ROOT --> DE
    CH --> CR
    DE --> CA
    CR --> CA
    CA --> P1
    CA --> P2
    CA --> P3
```

---

## MinIO Object Storage

### Configuration

MinIO provides S3-compatible object storage for Iceberg data files:

```yaml
minio:
  image: minio/minio:latest
  environment:
    MINIO_ROOT_USER: admin
    MINIO_ROOT_PASSWORD: password
  command: ["server", "/data", "--console-address", ":9001"]
  ports:
    - "9000:9000"   # S3 API
    - "9001:9001"   # Console UI
```

### Bucket Setup

```bash
# Create lakehouse bucket for Iceberg tables
mc mb minio/lakehouse

# Set public access (for local dev only!)
mc anonymous set public minio/lakehouse
```

### Accessing Data

| Interface | URL | Credentials |
|-----------|-----|-------------|
| S3 API | `http://localhost:9000` | admin / password |
| Web Console | `http://localhost:9001` | admin / password |

---

## Flink Integration

### IcebergMessageEventSink.java

This class configures the connection between Flink and the Iceberg/Polaris/MinIO stack:

```java
public void addSink(DataStream<MessageEvent> stream) {
    // Catalog configuration
    Map<String, String> catalogProps = new HashMap<>();
    catalogProps.put("uri", "http://polaris:8181/api/catalog");
    catalogProps.put("credential", "admin:password");
    catalogProps.put("warehouse", "lakehouse");
    catalogProps.put("scope", "PRINCIPAL_ROLE:ALL");
    
    // S3/MinIO configuration
    catalogProps.put("s3.endpoint", "http://minio:9000");
    catalogProps.put("s3.access-key-id", "admin");
    catalogProps.put("s3.secret-access-key", "password");
    catalogProps.put("s3.path-style-access", "true");
    
    // OAuth2 configuration
    catalogProps.put("rest.auth.type", "oauth2");
    catalogProps.put("oauth2-server-uri", 
        "http://polaris:8181/api/catalog/v1/oauth/tokens");
    
    // IO implementation
    catalogProps.put("io-impl", 
        "org.apache.iceberg.aws.s3.S3FileIO");
}
```

### Connection Flow

```mermaid
sequenceDiagram
    participant Flink
    participant Polaris
    participant MinIO

    Flink->>Polaris: POST /oauth/tokens<br/>client_credentials
    Polaris-->>Flink: access_token
    
    Flink->>Polaris: GET /lakehouse/namespaces/raw_messages/tables/profanity_messages
    Polaris-->>Flink: Table metadata (schema, location, snapshot)
    
    Flink->>MinIO: PUT /lakehouse/raw_messages/profanity_messages/data/*.parquet
    MinIO-->>Flink: 200 OK
    
    Note over Flink: On checkpoint
    Flink->>Polaris: POST /lakehouse/.../commit<br/>new snapshot
    Polaris-->>Flink: 200 OK (table updated)
```

---

## Dynamic Routing Implementation

### IcebergSinkFunction.java

The key innovation is **dynamic table routing** based on message content:

```java
// Define schema for all tables
private static final Schema FILTERED_MESSAGE_SCHEMA = new Schema(
    Types.NestedField.optional(1, "account_id", Types.StringType.get()),
    Types.NestedField.optional(2, "message_id", Types.StringType.get()),
    Types.NestedField.optional(3, "message_body", Types.StringType.get()),
    Types.NestedField.optional(4, "correlation_id", Types.StringType.get()),
    Types.NestedField.optional(5, "message_status", Types.StringType.get()),
    Types.NestedField.optional(6, "timestamp", Types.TimestampType.withoutZone()),
    Types.NestedField.optional(7, "profanity_type", Types.StringType.get())
);

// Dynamic routing generator
DynamicIcebergSink.forInput(rowDataStream)
  .generator((row, out) -> {
    String profanityType = row.getString(6).toString();
    
    // Route based on classification
    String tableName = "PROFANITY".equals(profanityType) 
        ? "profanity_messages" 
        : "safe_messages";
    
    out.collect(new DynamicRecord(
      TableIdentifier.of(namespace, tableName),
      branch,
      FILTERED_MESSAGE_SCHEMA,
      row,
      partitionSpec,
      DistributionMode.HASH,
      1  // sort order ID
    ));
  })
  .catalogLoader(CatalogLoader.rest(catalogName, new Configuration(), catalogProps))
  .writeParallelism(writeParallelism)
  .immediateTableUpdate(true)
```

### Routing Diagram

```mermaid
flowchart TB
    IN[Incoming MessageEvent]
    CONV[DataToRowConverter<br/>POJO → RowData]
    GEN{DynamicRecord<br/>Generator}
    
    T1[profanity_messages<br/>Table]
    T2[safe_messages<br/>Table]
    
    M1[(MinIO<br/>profane data)]
    M2[(MinIO<br/>safe data)]
    
    IN --> CONV
    CONV --> GEN
    GEN -->|"profanity_type = PROFANITY"| T1
    GEN -->|"profanity_type = SAFE"| T2
    T1 --> M1
    T2 --> M2
```

---

## Schema & Partitioning

### Table Schema

Both `profanity_messages` and `safe_messages` share this schema:

| Column | Iceberg Type | Description |
|--------|--------------|-------------|
| `account_id` | string | User account identifier |
| `message_id` | string | Unique message ID |
| `message_body` | string | Message text content |
| `correlation_id` | string | Request correlation ID |
| `message_status` | string | Delivery status |
| `timestamp` | timestamp | Event time (no timezone) |
| `profanity_type` | string | PROFANITY or SAFE |

### Partition Strategy

Tables are partitioned by `day(timestamp)`:

```java
PartitionSpec partitionSpec = PartitionSpec.builderFor(FILTERED_MESSAGE_SCHEMA)
    .day("timestamp")
    .build();
```

**Benefits:**
- Efficient time-range queries
- Automatic file organization by date
- Predictable file sizes per day

**File Layout:**
```
profanity_messages/data/
├── timestamp_day=2025-01-15/
│   └── 00000-0-xxx.parquet
└── timestamp_day=2025-01-16/
    └── 00000-0-yyy.parquet
```

---

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CATALOG_NAME` | `polaris` | Iceberg catalog name |
| `POLARIS_URI` | `http://polaris:8181/api/catalog` | Polaris REST endpoint |
| `POLARIS_CREDENTIAL` | `admin:password` | OAuth2 client credentials |
| `POLARIS_WAREHOUSE` | `lakehouse` | Iceberg warehouse name |
| `POLARIS_SCOPE` | `PRINCIPAL_ROLE:ALL` | OAuth2 scope |
| `S3_ENDPOINT` | `http://minio:9000` | MinIO S3 endpoint |
| `S3_ACCESS_KEY` | `admin` | MinIO access key |
| `S3_SECRET_KEY` | `password` | MinIO secret key |
| `S3_PATH_STYLE_ACCESS` | `true` | Use path-style URLs |
| `CLIENT_REGION` | `us-east-1` | AWS region (dummy for MinIO) |
| `ICEBERG_NAMESPACE` | `raw_data` | Iceberg namespace |
| `ICEBERG_BRANCH` | `main` | Iceberg branch |
| `WRITE_PARALLELISM` | `1` | Sink parallelism |
| `ICEBERG_TARGET_FILE_SIZE_BYTES` | `134217728` | Target file size (128MB) |
| `ICEBERG_DISTRIBUTION_MODE` | `HASH` | Distribution mode |

### Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| **401 Unauthorized** | Invalid OAuth token | Check `POLARIS_CREDENTIAL` |
| **Table not found** | Table not created | Run `polaris-setup` again |
| **S3 connection error** | Wrong MinIO endpoint | Verify `S3_ENDPOINT` |
| **No data in MinIO** | Checkpoint not triggered | Wait 5 minutes or trigger savepoint |

---

*This documentation is part of the Real-Time Profanity Filtering Pipeline project.*
