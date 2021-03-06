#!/usr/bin/env python

import base64, json, os, stat, sys, getopt
from time import sleep

from ambari_client import AmbariClient
from ambari_error import AmbariError
from sh_client import *
from sh_error import ShError,SshError


def print_help():
    print 'Usage:'
    print 'test.py'
    print 'test.py -c <config>'
    print 'test.py -n <case_number>'
    print 'test.py -c <config> -n <case_number>'


def main(argv):
    config_file = "../conf/config"
    case_number = ""
    try:
        opts, args = getopt.getopt(argv,"hc:n:")
    except getopt.GetoptError:
        print_help()
        exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_help()
            exit()
        elif opt in ("-c"):
            config_file = arg
        elif opt in ("-n"):
            case_number = arg

    try:
        if int(oct((os.stat(config_file)).st_mode)[-2:]) > 0:
            print("Please set the correct permission on the config file, aborting...")
            exit(1)
        config = json.loads(open(config_file, "r").read())
        ac = AmbariClient(config["hostname"]\
                          ,config["port"]\
                          ,config["cluster_name"]\
                          ,base64.b64encode("{0}:{1}".format(config["user"],config["password"]))\
                          ,config["ssl"])
        shc = ShClient()
        hst = HstClient(shc)
        asc = AmbariServerClient(shc)
        ssh_clients = dict((host,SshClient(host, "root", "root")) for host in ac.get_hosts())
        bckp_client = BackupClient(shc,"000122","/root/backup")
    except OSError as e:
        print(e.strerror)
        exit(1)
    except KeyError as e:
        print("Config json is not valid, please check")
        exit(1)
    except AmbariError as e:
        print(e.message)
        exit(1)
    except SshError as e:
        print(e.message)
        exit(1)
    except ShError as e:
        print(e.message)
        exit(1)

    try:
        hst.capture_bundle()
        for service in ["KNOX","SMARTSENSE","AMBARI_METRICS","AMBARI_INFRA"]:
            ac.turn_on_maintenance_mode_for_service(service)
            ac.stop_service(service)
            sleep(5)
            ac.start_service(service)
            ac.turn_off_maintenance_mode_for_service(service)
        asc.stop()
        asc.start()
        for host, ssh in ssh_clients.iteritems():
            print("SSH into {0} host".format(host))
            aa = AmbariAgent(ssh)
            aa.stop()
            aa.start()
        bckp_client.create_backup()
    except AmbariError as e:
        print(e.message)
        exit(1)
    except ShError as e:
        print(e.message)
        exit(1)
    except SshError as e:
        print(e.message)
        exit(1)


if __name__== "__main__":
    main(sys.argv[1:])
