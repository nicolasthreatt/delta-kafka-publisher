# Delta Kafka Publisher

Small Java utility for reading rows from a Delta Lake table with Delta Kernel and publishing them to Kafka as JSON.

The project is config-driven and focused on a single job: scan a Delta table and publish each record to a Kafka topic.

## Required Environment Variables

```bash
export DELTA_TABLE_PATH="abfss://container@account.dfs.core.windows.net/path/to/table"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="delta-records"
```

## Optional Environment Variables

```bash
export DELTA_FILTER_COLUMN="customer_id"
export DELTA_FILTER_VALUE="123"
export MAX_RECORDS="1000"

export AZURE_STORAGE_ACCOUNT="your-storage-account"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_TENANT_ID="your-tenant-id"
```

`DELTA_FILTER_COLUMN` and `DELTA_FILTER_VALUE` are applied together as a string equality predicate.

`MAX_RECORDS=0` means no explicit limit.

If Azure OAuth variables are omitted, the connector uses the default Hadoop configuration. That lets you rely on ambient credentials when available.

## Build

```bash
mvn clean package
```

## Run

```bash
java -jar target/delta-kafka-publisher-1.0-SNAPSHOT.jar
```

## License

MIT. See `LICENSE`.

## Notes

- Add CI for `mvn -q -DskipTests package` or `mvn test`.
- Keep `target/`, `.env`, and IDE files out of version control.
