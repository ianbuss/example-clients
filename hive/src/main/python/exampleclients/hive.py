#!/usr/bin/env python

import logging
import os
import pwd
import subprocess
import sys
import uuid

# We are talking to Hive using the impala module
# Both Hive and Impala can use the HS2 protocol
from impala.dbapi import connect

DEFAULT_HS2_PORT = 10000
DEFAULT_HS2_PRINCIPAL = "hive"
DEFAULT_HDFS_PORT = 50070
DEFAULT_USER_PRINC = pwd.getpwuid(os.getuid()).pw_name

logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s')

LOG = logging.getLogger(__name__)


def connect_to_hs2(host, port, auth_mech, server_princ):
    return connect(host=host, port=port, auth_mechanism=auth_mech,
                   kerberos_service_name=server_princ)


def get_opt_arg(args, opt, index):
    if index >= len(args):
        LOG.error("No supplied argument for %s" % opt)
        usage(-1)
    else:
        return args[index]


def usage(code):
    print >> sys.stderr, "Usage: %s -h HS2_HOST -q QUERY [-p PORT] [-s SERVER_PRINC] [-u USER_PRINC] [-k] [-t KEYTAB]" % \
                         sys.argv[0]
    sys.exit(code)


def login_via_keytab(keytab, user):
    os.environ['KRB5CCNAME'] = '/tmp/%s' % uuid.uuid1()
    code = subprocess.call(['kinit','-k','t',keytab,user])
    if code == 0:
        return True
    else:
        return False


def destroy_keytab():
    code = subprocess.call(['kdestroy'])
    if code == 0:
        return True
    else:
        return False


def parse_args():
    config = {
        'server_port': DEFAULT_HS2_PORT,
        'server_princ': DEFAULT_HS2_PRINCIPAL,
        'hdfs_port': DEFAULT_HDFS_PORT,
        'user_princ': DEFAULT_USER_PRINC,
        'secure': False
    }
    args = sys.argv[1:]
    for i in range(len(args)):
        if args[i] == "-h":
            config['server_host'] = get_opt_arg(args, "-h", i + 1)
        elif args[i] == "-p":
            config['server_port'] = int(get_opt_arg(args, "-p", i + 1))
        elif args[i] == "-q":
            config['query'] = get_opt_arg(args, "-q", i + 1)
        elif args[i] == "-s":
            config['server_princ'] = get_opt_arg(args, "-s", i + 1)
        elif args[i] == "-u":
            config['user_princ'] = get_opt_arg(args, "-u", i + 1)
        elif args[i] == "-k":
            config['secure'] = True
        elif args[i] == "-t":
            config['keytab'] = get_opt_arg(args, "-t", i + 1)
        elif args[i] == "-P":
            config['hdfs_port'] = int(get_opt_arg(args, "-P", i + 1))
        elif args[i] == "-H":
            config['hdfs_host'] = get_opt_arg(args, "-H", i + 1)

    return config


if __name__ == "__main__":
    config = parse_args()
    if 'server_host' not in config:
        LOG.error("HS2 hostname not supplied via -h option")
        usage(-1)
    if 'query' not in config:
        LOG.error("No query specified via -q")
        usage(-1)

    auth_mech = 'NOSASL'
    if config['secure']:
        auth_mech = 'GSSAPI'

    if 'keytab' in config and not login_via_keytab(config['keytab']):
        LOG.error("Could not login with keytab")
        sys.exit(-1)

    hs2_client = connect_to_hs2(config['server_host'], config['server_port'], auth_mech,
                                config['server_princ'])

    cursor = hs2_client.cursor()
    cursor.execute(config['query'])

    print cursor.description

    for row in cursor:
        print row

    cursor.close()
    hs2_client.close()

    if 'keytab' in config and not destroy_keytab():
        LOG.warn('Could not kdestroy, removing directory %s', os.environ['KRB5CCNAME'])
        os.removedirs(os.environ['KRB5CCNAME'])
