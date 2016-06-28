#!/usr/bin/env python
import argparse
import sys
import itertools
import codecs
import zipfile
import os
import datetime
import shutil

try:
    from fabric.api import (env, execute, hide, run, sudo)
except ImportError:
    sys.exit('Python fabric not installed. You might try \"pip install fabric and sudo apt-get install fabric\".')

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

def log_quiet(msg):
    if not args.quiet:
        sys.stdout.write(msg)
        sys.stdout.flush()

def get_credentials(self):
    return {'username': args.username, 'password': args.password}

def get_keyspace_or_fail(session, keyname):
    keyspace = session.cluster.metadata.keyspaces.get(keyname)

    if not keyspace:
        sys.stderr.write('Can\'t find keyspace "' + keyname + '"\n')
        sys.exit(1)

    return keyspace

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


def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))


def export_data(session):
    selection_options = 0
    cqlsh_path = "/usr/bin/cqlsh"
    host = "localhost"
    if args.host is not None:
        host = args.host
    if args.keyspace is not None:
        selection_options += 1

    if args.cassandra_bin_dir is not None:
        cqlsh_path = "{!s}/cqlsh".format(args.cassandra_bin_dir)

    if selection_options > 1:
        sys.stderr.write('--cf, --keyspace and --filter can\'t be combined\n')
        sys.exit(1)

    if not os.path.isfile(cqlsh_path):
        sys.stderr.write('cqlsh can\'t be found, please use --cassandra-bin-dir option\n')
        sys.exit(1)

    target = args.export_dir
    path = target+"/"+datetime.datetime.now().strftime('%Y%m%d%H')

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
                schemaDir = path + "/" + keyname
                recordDir = path + "/" + keyname + "/tables"

                if not os.path.exists(schemaDir):
                    os.makedirs(schemaDir)
                if not os.path.exists(recordDir):
                    os.makedirs(recordDir)

                log_quiet('Exporting data schema of table ' + keyname  + '\n')

                backup_schema_command = '%(cqlsh)s %(host)s -e "DESC KEYSPACE %(keyname)s" > %(schemaDir)s/%(keyname)s.cql'
                schema_cmd = backup_schema_command % dict(
                    cqlsh=cqlsh_path,
                    schemaDir=schemaDir,
                    keyname=keyname,
                    host=host
                )

                if args.username is not None and args.password is not None:
                    backup_schema_command = '%(cqlsh)s -u %(user)s -p  %(password)s %(host)s  -e "DESC KEYSPACE %(keyname)s" > %(schemaDir)s/%(keyname)s.cql'
                    schema_cmd = backup_schema_command % dict(
                        cqlsh=cqlsh_path,
                        schemaDir=schemaDir,
                        keyname=keyname,
                        host=host,
                        user=args.username,
                        password=args.password
                    )

                os.system(schema_cmd)

                keyspace = get_keyspace_or_fail(session, keyname)
                for tablename, tableval in keyspace.tables.iteritems():
                    if tableval.is_cql_compatible:
                        log_quiet('Exporting data records of table ' + keyname + '.' + tablename + '\n')
                        backup_record_command = '%(cqlsh)s %(host)s -e "COPY %(keyname)s.%(tablename)s TO \'%(recordDir)s/%(tablename)s.csv\'"'
                        record_cmd = backup_record_command % dict(
                            cqlsh=cqlsh_path,
                            recordDir=recordDir,
                            tablename=tablename,
                            keyname=keyname,
                            host=host
                        )
                        if args.username is not None and args.password is not None:
                            backup_record_command = '%(cqlsh)s -u %(user)s -p  %(password)s %(host)s -e "COPY %(keyname)s.%(tablename)s TO \'%(recordDir)s/%(tablename)s.csv\'"'
                            record_cmd = backup_record_command % dict(
                                cqlsh=cqlsh_path,
                                recordDir=recordDir,
                                tablename=tablename,
                                keyname=keyname,
                                host=host,
                                user=args.username,
                                password=args.password
                            )
                        os.system(record_cmd)



    if args.compress is not None:
        print 'Creating archive ' + target
        filename = 'backup_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + '.zip'

        zf = zipfile.ZipFile(path + '/' + filename, 'w', zipfile.ZIP_DEFLATED)
        try:
            for keyname in keyspaces:
                print 'adding ' + path + "/" + keyname
                #zf.write(path + "/" + keyname)
                zipdir(path + "/" + keyname, zf)
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

        print 'Upload backup file to S3 service: ' + path + '/' + filename
        k = Key(bucket)
        k.key = filename
        k.set_contents_from_filename(path + '/' + filename, cb=percent_cb, num_cb=10)

def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

def main():
    global args

    parser = argparse.ArgumentParser(description='A data exporting tool for Cassandra, with some added slice and dice capabilities.')
    parser.add_argument('--export-dir', help='export data to the specified directory')
    parser.add_argument('--compress', help='compress data file')
    parser.add_argument('--cassandra-bin-dir', help='cassandra bin directory')
    parser.add_argument('--s3', help='upload file to SS3 service')
    parser.add_argument('--s3-key', help='AWS_ACCESS_KEY_ID for S3')
    parser.add_argument('--s3-secret', help='AWS_SECRET_ACCESS_KEY for S3')
    parser.add_argument('--s3-bucket-name', help='S3 bucket name, default "backup"')
    parser.add_argument('--s3-location', help='S3 location, value: ["singapore", "tokyo"], default "USA"')
    parser.add_argument('--filter', help='export a slice of a column family according to a CQL filter. This takes essentially a typical SELECT query stripped of the initial "SELECT ... FROM" part (e.g. "system.schema_columns where keyspace_name =\'OpsCenter\'", and exports only that data. Can be specified multiple times', action='append')
    parser.add_argument('--host', help='the address of a Cassandra node in the cluster (localhost if omitted)')
    parser.add_argument('--port', help='the port of a Cassandra node in the cluster (9042 if omitted)')
    parser.add_argument('--import-dir', help='import data from the specified directory')
    parser.add_argument('--keyspace', help='export a keyspace along with all its column families. Can be specified multiple times', action='append')
    parser.add_argument('--username', help='set username for auth (only if protocol-version is set)')
    parser.add_argument('--password', help='set password for authentication (only if protocol-version is set)')
    parser.add_argument('--protocol-version', help='set protocol version (required for authentication)', type=int)
    parser.add_argument('--quiet', help='quiet progress logging', action='store_true')

    args = parser.parse_args()

    if args.import_dir is None and args.export_dir is None:
        sys.stderr.write('--import-dir or --export-dir must be specified\n')
        sys.exit(1)

    if args.import_dir is not None and args.export_file is not None:
        sys.stderr.write('--import-dir and --export-dir can\'t be specified at the same time\n')
        sys.exit(1)

    session = setup_cluster()

    if args.import_dir:
        #import_data(session)
        sys.stderr.write('--import-dir feature updating\n')
    elif args.export_dir:
        if not os.path.exists(args.export_dir):
            os.makedirs(args.export_dir)
        export_data(session)

    cleanup_cluster(session)


if __name__ == '__main__':
    main()