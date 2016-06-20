# cassandra-backup
A cassandra backup script

# Options Support
optional arguments:
  -h, --help            show this help message and exit
  --cf CF               export a column family. The name must include the
                        keyspace, e.g. "system.schema_columns". Can be
                        specified multiple times
  --export-file EXPORT_FILE
                        export data to the specified file
  --compress COMPRESS   compress data file
  --s3 S3               upload file to SS3 service
  --s3-key S3_KEY       AWS_ACCESS_KEY_ID for S3
  --s3-secret S3_SECRET
                        AWS_SECRET_ACCESS_KEY for S3
  --s3-bucket-name S3_BUCKET_NAME
                        S3 bucket name, default "backup"
  --s3-location S3_LOCATION
                        S3 location, value: ["singapore", "tokyo"], default
                        "USA"
  --filter FILTER       export a slice of a column family according to a CQL
                        filter. This takes essentially a typical SELECT query
                        stripped of the initial "SELECT ... FROM" part (e.g.
                        "system.schema_columns where keyspace_name
                        ='OpsCenter'", and exports only that data. Can be
                        specified multiple times
  --host HOST           the address of a Cassandra node in the cluster
                        (localhost if omitted)
  --port PORT           the port of a Cassandra node in the cluster (9042 if
                        omitted)
  --import-file IMPORT_FILE
                        import data from the specified file
  --keyspace KEYSPACE   export a keyspace along with all its column families.
                        Can be specified multiple times
  --no-create           don't generate create (and drop) statements
  --no-insert           don't generate insert statements
  --password PASSWORD   set password for authentication (only if protocol-
                        version is set)
  --protocol-version PROTOCOL_VERSION
                        set protocol version (required for authentication)
  --quiet               quiet progress logging
  --sync                import data in synchronous mode (default asynchronous)
  --username USERNAME   set username for auth (only if protocol-version is
                        set)
