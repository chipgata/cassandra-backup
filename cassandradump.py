#!/usr/bin/env python
import argparse
import sys
import itertools
import codecs
import zipfile
import os
import datetime

try:
    import cassandra
    import cassandra.concurrent
except ImportError:
    sys.exit('Python Cassandra driver not installed. You might try \"pip install cassandra-driver\".')

try:
    import boto
    import boto.s3
    from boto.s3.key import Key
    from boto.s3.connection import Location
except ImportError:
    sys.exit('Python Boto not installed. You might try \"pip install boto\".')

from cassandra.auth import PlainTextAuthProvider #For protocol_version 2
from cassandra.cluster import Cluster

TIMEOUT = 120.0
FETCH_SIZE = 100
DOT_EVERY = 1000
CONCURRENT_BATCH_SIZE = 1000

args = None

def cql_type(v):
    try:
        return v.data_type.typename
    except AttributeError:
        return v.cql_type

def to_utf8(s):
    return codecs.decode(s, 'utf-8')

def log_quiet(msg):
    if not args.quiet:
        sys.stdout.write(msg)
        sys.stdout.flush()


def table_to_cqlfile(session, keyspace, tablename, flt, tableval, filep):
    if flt is None:
        query = 'SELECT * FROM "' + keyspace + '"."' + tablename + '"'
    else:
        query = 'SELECT * FROM ' + flt

    rows = session.execute(query)

    cnt = 0

    def make_non_null_value_encoder(typename):
        if typename == 'blob':
            return session.encoder.cql_encode_bytes
        elif typename.startswith('map'):
            return session.encoder.cql_encode_map_collection
        elif typename.startswith('frozen'):
            return session.encoder.cql_encode_map_collection
        elif typename.startswith('set'):
            return session.encoder.cql_encode_set_collection
        elif typename.startswith('list'):
            return session.encoder.cql_encode_list_collection
        else:
            return session.encoder.cql_encode_all_types

    def make_value_encoder(typename):
        e = make_non_null_value_encoder(typename)
        return lambda v : session.encoder.cql_encode_all_types(v) if v is None else e(v)

    def make_value_encoders(tableval):
        return dict((to_utf8(k), make_value_encoder(cql_type(v))) for k, v in tableval.columns.iteritems())

    def make_row_encoder(tableevel):
        partitions = dict(
            (has_counter, list(to_utf8(k) for k, v in columns))
            for has_counter, columns in itertools.groupby(tableval.columns.iteritems(), lambda (k, v): cql_type(v) == 'counter')
        )

        keyspace_utf8 = to_utf8(keyspace)
        tablename_utf8 = to_utf8(tablename)

        counters = partitions.get(True, [])
        non_counters = partitions.get(False, [])
        columns = counters + non_counters

        if len(counters) > 0:
            def row_encoder(values):
                set_clause = ", ".join('%s = %s + %s' % (c, c,  values[c]) for c in counters if values[c] != 'NULL')
                where_clause = " AND ".join('%s = %s' % (c, values[c]) for c in non_counters)
                return 'UPDATE "%(keyspace)s"."%(tablename)s" SET %(set_clause)s WHERE %(where_clause)s' % dict(
                        keyspace = keyspace_utf8,
                        tablename = tablename_utf8,
                        where_clause = where_clause,
                        set_clause = set_clause,
                )
        else:
            columns = list(counters + non_counters)
            def row_encoder(values):
                return 'INSERT INTO "%(keyspace)s"."%(tablename)s" (%(columns)s) VALUES (%(values)s)' % dict(
                        keyspace = keyspace_utf8,
                        tablename = tablename_utf8,
                        columns = ', '.join('"{}"'.format(c) for c in columns if values[c]!="NULL"),
                        values = ', '.join(values[c] for c in columns if values[c]!="NULL"),
                )
        return row_encoder

    value_encoders = make_value_encoders(tableval)
    row_encoder = make_row_encoder(tableval)

    for row in rows:
        values = dict((to_utf8(k), to_utf8(value_encoders[k](v))) for k, v in row.iteritems())
        filep.write("%s;\n" % row_encoder(values))

        cnt += 1

        if (cnt % DOT_EVERY) == 0:
            log_quiet('.')

    if cnt > DOT_EVERY:
        log_quiet('\n')


def can_execute_concurrently(statement):
    if args.sync:
        return False

    if statement.upper().startswith('INSERT') or statement.upper().startswith('UPDATE'):
        return True
    else:
        return False


def import_data(session):
    f = codecs.open(args.import_file, 'r', encoding = 'utf-8')

    cnt = 0

    statement = ''
    concurrent_statements = []

    for line in f:
        statement += line
        if statement.endswith(";\n"):
            if can_execute_concurrently(statement):
                concurrent_statements.append((statement, None))

                if len(concurrent_statements) >= CONCURRENT_BATCH_SIZE:
                    cassandra.concurrent.execute_concurrent(session, concurrent_statements)
                    concurrent_statements = []
            else:
                if len(concurrent_statements) > 0:
                    cassandra.concurrent.execute_concurrent(session, concurrent_statements)
                    concurrent_statements = []

                session.execute(statement)

            statement = ''

            cnt += 1
            if (cnt % DOT_EVERY) == 0:
                log_quiet('.')

    if len(concurrent_statements) > 0:
        cassandra.concurrent.execute_concurrent(session, concurrent_statements)

    if statement != '':
        session.execute(statement)

    if cnt > DOT_EVERY:
        log_quiet('\n')

    f.close()


def get_keyspace_or_fail(session, keyname):
    keyspace = session.cluster.metadata.keyspaces.get(keyname)

    if not keyspace:
        sys.stderr.write('Can\'t find keyspace "' + keyname + '"\n')
        sys.exit(1)

    return keyspace


def get_column_family_or_fail(keyspace, tablename):
    tableval = keyspace.tables.get(tablename)

    if not tableval:
        sys.stderr.write('Can\'t find table "' + tablename + '"\n')
        sys.exit(1)

    return tableval


def export_data(session):
    selection_options = 0

    if args.keyspace is not None:
        selection_options += 1

    if args.cf is not None:
        selection_options += 1

    if args.filter is not None:
        selection_options += 1

    if selection_options > 1:
        sys.stderr.write('--cf, --keyspace and --filter can\'t be combined\n')
        sys.exit(1)

    path = os.path.abspath(args.export_file)
    target = os.path.dirname(path)
    if not os.path.exists(target):
        os.makedirs(target)

    f = codecs.open(args.export_file, 'w', encoding = 'utf-8')

    keyspaces = None

    if selection_options == 0:
        log_quiet('Exporting all keyspaces\n')
        keyspaces = []
        for keyspace in session.cluster.metadata.keyspaces.keys():
            if keyspace not in ('system', 'system_traces'):
                keyspaces.append(keyspace)

    if args.keyspace is not None:
        if args.keyspace[0].find(',') != -1:
            keyspaces = args.keyspace[0].split(',')
        else:
            keyspaces = args.keyspace

    if keyspaces is not None:
        for keyname in keyspaces:
            if keyname.strip() != '':
                keyspace = get_keyspace_or_fail(session, keyname)

                if not args.no_create:
                    log_quiet('Exporting schema for keyspace ' + keyname + '\n')
                    f.write('DROP KEYSPACE IF EXISTS "' + keyname + '";\n')
                    f.write(keyspace.export_as_string() + '\n')

                for tablename, tableval in keyspace.tables.iteritems():
                    if tableval.is_cql_compatible:
                        if not args.no_insert:
                            log_quiet('Exporting data for column family ' + keyname + '.' + tablename + '\n')
                            table_to_cqlfile(session, keyname, tablename, None, tableval, f)

    if args.cf is not None:
        for cf in args.cf:
            if '.' not in cf:
                sys.stderr.write('Invalid keyspace.column_family input\n')
                sys.exit(1)

            keyname = cf.split('.')[0]
            tablename = cf.split('.')[1]

            keyspace = get_keyspace_or_fail(session, keyname)
            tableval = get_column_family_or_fail(keyspace, tablename)

            if tableval.is_cql_compatible:
                if not args.no_create:
                    log_quiet('Exporting schema for column family ' + keyname + '.' + tablename + '\n')
                    f.write('DROP TABLE IF EXISTS "' + keyname + '"."' + tablename + '";\n')
                    f.write(tableval.export_as_string() + ';\n')

                if not args.no_insert:
                    log_quiet('Exporting data for column family ' + keyname + '.' + tablename + '\n')
                    table_to_cqlfile(session, keyname, tablename, None, tableval, f)

    if args.filter is not None:
        for flt in args.filter:
            stripped = flt.strip()
            cf = stripped.split(' ')[0]

            if '.' not in cf:
                sys.stderr.write('Invalid input\n')
                sys.exit(1)

            keyname = cf.split('.')[0]
            tablename = cf.split('.')[1]


            keyspace = get_keyspace_or_fail(session, keyname)
            tableval = get_column_family_or_fail(keyspace, tablename)

            if not tableval:
                sys.stderr.write('Can\'t find table "' + tablename + '"\n')
                sys.exit(1)

            if not args.no_insert:
                log_quiet('Exporting data for filter "' + stripped + '"\n')
                table_to_cqlfile(session, keyname, tablename, stripped, tableval, f)

    f.close()

    if args.compress is not None:
        print 'Creating archive ' + target
        filename = 'backup_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '.zip'

        zf = zipfile.ZipFile(target + '/' + filename, mode='w')
        try:
            print 'adding ' + path
            zf.write(path)
        finally:
            print 'closing'
            zf.close()

    if args.s3 is not None:
        if args.s3_key is None:
            sys.stderr.write('Miss --s3-key option \n')
            sys.exit(1)

        if args.s3_secret is None:
            sys.stderr.write('Miss --s3-secret option \n')
            sys.exit(1)

        conn = boto.connect_s3(args.s3_key, args.s3_secret)
        bucket_name = args.s3_key.lower() +"-emobi-backup"
        location = Location.DEFAULT


        if args.s3_location is None:
            if args.s3_location == 'singapore':
                location = Location.APSoutheast
            elif args.s3_location == 'tokyo':
                location = Location.APNortheast
            else:
                sys.stderr.write('Location value should be "singapore" or "tokyo" \n')

        if args.s3_bucket_name is not None:
            bucket_name = args.s3_bucket_name

        if conn.lookup(bucket_name) is None:
            bucket = conn.create_bucket(bucket_name, location=location)
        else:
            bucket = conn.get_bucket(bucket_name)

        print 'Upload backup file to S3 service: '+target + '/' + filename
        k = Key(bucket)
        k.key = filename
        k.set_contents_from_filename(target + '/' + filename, cb=percent_cb, num_cb=10)


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def get_credentials(self):
    return {'username': args.username, 'password': args.password}

def setup_cluster():
    if args.host is None:
        nodes = ['localhost']
    else:
        nodes = [args.host]

    if args.port is None:
        port = 9042
    else:
        port = args.port

    cluster = None

    if args.protocol_version is not None:
        auth = None

        if args.username is not None and args.password is not None:
            if args.protocol_version == 1:
                auth = get_credentials
            elif args.protocol_version > 1:
                auth = PlainTextAuthProvider(username=args.username, password=args.password)

        cluster = Cluster(contact_points=nodes, protocol_version=args.protocol_version, auth_provider=auth, load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(nodes))
    else:
        cluster = Cluster(contact_points=nodes, port=port, load_balancing_policy=cassandra.policies.WhiteListRoundRobinPolicy(nodes))

    session = cluster.connect()

    session.default_timeout = TIMEOUT
    session.default_fetch_size = FETCH_SIZE
    session.row_factory = cassandra.query.ordered_dict_factory
    return session


def cleanup_cluster(session):
    session.cluster.shutdown()
    session.shutdown()


def main():
    global args

    parser = argparse.ArgumentParser(description='A data exporting tool for Cassandra inspired from mysqldump, with some added slice and dice capabilities.')
    parser.add_argument('--cf', help='export a column family. The name must include the keyspace, e.g. "system.schema_columns". Can be specified multiple times', action='append')
    parser.add_argument('--export-file', help='export data to the specified file')
    parser.add_argument('--compress', help='compress data file')
    parser.add_argument('--s3', help='upload file to SS3 service')
    parser.add_argument('--s3-key', help='AWS_ACCESS_KEY_ID for S3')
    parser.add_argument('--s3-secret', help='AWS_SECRET_ACCESS_KEY for S3')
    parser.add_argument('--s3-bucket-name', help='S3 bucket name, default "backup"')
    parser.add_argument('--s3-location', help='S3 location, value: ["singapore", "tokyo"], default "USA"')
    parser.add_argument('--filter', help='export a slice of a column family according to a CQL filter. This takes essentially a typical SELECT query stripped of the initial "SELECT ... FROM" part (e.g. "system.schema_columns where keyspace_name =\'OpsCenter\'", and exports only that data. Can be specified multiple times', action='append')
    parser.add_argument('--host', help='the address of a Cassandra node in the cluster (localhost if omitted)')
    parser.add_argument('--port', help='the port of a Cassandra node in the cluster (9042 if omitted)')
    parser.add_argument('--import-file', help='import data from the specified file')
    parser.add_argument('--keyspace', help='export a keyspace along with all its column families. Can be specified multiple times', action='append')
    parser.add_argument('--no-create', help='don\'t generate create (and drop) statements', action='store_true')
    parser.add_argument('--no-insert', help='don\'t generate insert statements', action='store_true')
    parser.add_argument('--password', help='set password for authentication (only if protocol-version is set)')
    parser.add_argument('--protocol-version', help='set protocol version (required for authentication)', type=int)
    parser.add_argument('--quiet', help='quiet progress logging', action='store_true')
    parser.add_argument('--sync', help='import data in synchronous mode (default asynchronous)', action='store_true')
    parser.add_argument('--username', help='set username for auth (only if protocol-version is set)')
    args = parser.parse_args()

    if args.import_file is None and args.export_file is None:
        sys.stderr.write('--import-file or --export-file must be specified\n')
        sys.exit(1)

    if args.import_file is not None and args.export_file is not None:
        sys.stderr.write('--import-file and --export-file can\'t be specified at the same time\n')
        sys.exit(1)

    session = setup_cluster()

    if args.import_file:
        import_data(session)
    elif args.export_file:
        export_data(session)

    cleanup_cluster(session)


if __name__ == '__main__':
    main()
