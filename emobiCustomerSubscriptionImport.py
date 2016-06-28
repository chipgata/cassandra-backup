#!/usr/bin/env python
import argparse
import sys
import itertools
import codecs
import zipfile
import os
import datetime
import shutil
import time

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

import csv

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


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def unix_time_millis(dt):
    return long(unix_time(dt) * 1000.0)


def customer_subscription(session):

    #0 Read data from xls file
    f = open('Workbook1.csv', 'rU')
    try:
        reader = csv.reader(f)
        for row in reader:

            # 1 Get the customer id using the subscriber phone
            cusCmd = "select id from emobi.customer where identities['MSISDN'] = '+"+row[0]+"' allow filtering"
            print "--->Customer select cmd: "+cusCmd
            customers = session.execute(cusCmd)

            for cus in customers:
                if 'id' in cus:
                    #time = datetime.datetime.strptime(row[1], "%d.%m.%Y. %H:%M:%S").timestamp()
                    timeCreated = unix_time_millis(datetime.datetime.strptime(row[1], "%d.%m.%Y. %H:%M:%S"))
                    print timeCreated
                    print "--------->Get purchase session of customer: "+str(cus["id"])
                    # 2 Get the subscription and guid from purchase_session using the customer id
                    purCms = "select url_params from emobi.purchase_session where customer_id = "+str(cus["id"])+" allow filtering"
                    print "--------------->purchase session select cmd: " + purCms
                    purs = session.execute(purCms)
                    for sess in purs:

                        curlCmd = 'curl -i -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d \'{"subscriptionId":"'+sess["url_params"]["productReference"][0]+'","customerId":"'+str(cus["id"])+'","startDisplayDate":'+str(timeCreated)+',"status":"ACTIVE","affiliate":{"name":"AIRPUSH","guid":"'+sess["url_params"]["guid"][0]+'"}}\' http://api.emobi-sys.com:8095/v1/subs/subscription'
                        print "------------------>insert customer subs cmd: "+curlCmd
                        #os.system(curlCmd)

                        # 4 then check if customer subscription is created successfully using the below curl script:
                        checkCmd = 'curl -X GET http://api.emobi-sys.com:8095/v1/subs/customer/'+str(cus["id"])
                        print "------------------>check customer subs cmd: " + checkCmd
                        # os.system(checkCmd)
                else:
                    print "===>Customer not exist, ignore"

            print "\n---------------------------------End------------------------------"


    finally:
        f.close()

    exit()





def main():
    global args

    parser = argparse.ArgumentParser(description='A Emobi utils tool for Cassandra, with some added slice and dice capabilities.')
    parser.add_argument('--function', help='quiet progress logging')
    parser.add_argument('--host', help='the address of a Cassandra node in the cluster (localhost if omitted)')
    parser.add_argument('--port', help='the port of a Cassandra node in the cluster (9042 if omitted)')
    parser.add_argument('--username', help='set username for auth (only if protocol-version is set)')
    parser.add_argument('--password', help='set password for authentication (only if protocol-version is set)')
    parser.add_argument('--protocol-version', help='set protocol version (required for authentication)', type=int)
    parser.add_argument('--quiet', help='quiet progress logging', action='store_true')

    args = parser.parse_args()

    if args.function is None :
        sys.stderr.write('--function must be specified\n')
        sys.exit(1)



    session = setup_cluster()


    if args.function == "customerSubscriptionImport":
        customer_subscription(session)

    cleanup_cluster(session)


if __name__ == '__main__':
    main()