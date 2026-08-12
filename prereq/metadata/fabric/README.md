# Fabric metadata adapter prerequisite

Fabric is currently an explicit unsupported boundary in AstraDataV3. Before a
production DDL or repository can be implemented, select exactly one execution
substrate:

1. Fabric Warehouse SQL, or
2. Fabric Lakehouse/Spark SQL.

The decision must also identify:

- authentication and secret resolution;
- the supported SQL/transaction client;
- atomic queue-claim semantics;
- optimistic watermark update semantics;
- artifact deployment/execution mechanism; and
- the target catalog/workspace/environment identifier.

Until those decisions are approved, the API must continue to fail fast for a
Fabric execution request. A generic ODBC connection is not evidence of Fabric
runtime support.

