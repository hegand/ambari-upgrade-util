from subprocess import check_output, CalledProcessError
from time import sleep
from datetime import datetime
from os import path
import copy, shlex

from sh_error import HstError,AmbariServerError,SshError,ShError


class ShClient(object):
    def __init__(self):
        self.checking()
        self.is_sudo = self.checking_sudo()

    @staticmethod
    def checking():
        try:
            check_output("whoami")
        except CalledProcessError as e:
            raise ShError("Some problems occurred during ShClient initialization: {0}".format(e.message))

    @staticmethod
    def checking_sudo():
        try:
            return check_output(["sudo", "whoami"]).split("\n")[0] == 'root'
        except CalledProcessError as e:
            return False

    @staticmethod
    def construct_command(command, runasuser=None):
        if type(command) is not str:
            raise ShError("Command must be string, but it is a {0}".format(type(command)))
        _command = copy.deepcopy(command)
        if runasuser == "root":
            _command = "sudo {0}".format(_command)
        elif runasuser is not None:
            _command = "sudo -u {0} {1}".format(runasuser, _command)
        return _command

    def run(self, command, runasuser=None, shell=False):
        if runasuser is not None and not self.is_sudo:
            raise ShError("The current user does not have sudo right")
        _command = self.construct_command(command, runasuser)
        if not shell:
            _command = shlex.split(_command)
        try:
            return check_output(_command, shell=shell)
        except CalledProcessError as e:
            raise ShError(e.message)


class SshClient(ShClient):
    def __init__(self, hostname, remoteuser=None, runaslocaluser=None):
        self.base_command = self.construct_ssh_base_command(hostname, remoteuser, runaslocaluser)
        self.hostname = hostname

    def construct_command(self, command, runasuser=None):
        return self.base_command + super(SshClient,self).construct_command(command, runasuser)

    @staticmethod
    def construct_ssh_base_command(hostname, remoteuser, runaslocaluser):
        command = "ssh"
        if remoteuser is not None:
            command = "{0} {1}@{2}".format(command, remoteuser, hostname)
        else:
            command = "{0} {1}".format(command, hostname)
        if runaslocaluser == "root":
            command = "sudo {0}".format(command)
        elif runaslocaluser is not None:
            command = "sudo -u {0} {1}".format(runaslocaluser, command)
        return "{0} TERM=dumb".format(command)


class HstClient(object):
    def __init__(self, sh_client, exec_path="/usr/sbin/hst"):
        self.sh_client = sh_client
        self.exec_path = exec_path

    def capture_bundle(self):
        try:
            print("Capturing smartsense bundle...")
            self.sh_client.run([self.exec_path, "capture"], "root")
            print("Smartsense boundle captured")
        except ShError as e:
            raise HstError("Smartsense capture has been failed, please check hst logs for details")


class AmbariServerClient(object):
    def __init__(self, sh_client, exec_path="/usr/sbin/ambari-server"):
        self.sh_client = sh_client
        self.exec_path = exec_path

    def stop(self):
        try:
            print("Stopping ambari-server...")
            if self.running():
                self.sh_client.run([self.exec_path, "stop"], "root")
            else:
                raise AmbariServerError("Ambari server is not running")
            sleep(10)
            if self.running():
                raise AmbariServerError("Stopping ambari-server was not successful, please check")
            print("Ambari server has been stopped successfully")
        except ShError as e:
            raise AmbariServerError("Some problems have been occurred during stopping ambari-server")

    def start(self):
        try:
            print("Starting ambari-server...")
            self.sh_client.run([self.exec_path, "start"], "root")
            sleep(10)
            if not self.running():
                raise AmbariServerError("Starting ambari-server was not successful, please check")
            print("Ambari server has been started successfully")
        except ShError as e:
            raise AmbariServerError("Some problems have been occurred during starting ambari-server")

    def running(self):
        try:
            self.sh_client.run([self.exec_path,"status"],"root")
            return True
        except ShError as e:
            return False


class AmbariAgent(object):
    def __init__(self, sh_client, exec_path="/usr/sbin/ambari-agent"):
        self.sh_client = sh_client
        self.exec_path = exec_path

    def stop(self):
        print("Stopping ambari-agent...")
        if self.running():
            self.sh_client.run([self.exec_path,"stop"],"root")
        else:
            raise SshError("Ambari-agent is not running")
        sleep(10)
        if self.running():
            raise SshError("Stopping ambari-agent was not successful, please check")
        print("Ambari-agent has been stopped successfully")

    def start(self):
        print("Starting ambari-agent...")
        if not self.running():
            self.sh_client.run([self.exec_path,"start"],"root")
        else:
            raise SshError("Ambari-agent is already running")
        sleep(10)
        if not self.running():
            raise SshError("Starting ambari-agent was not successful, please check")
        print("Ambari-agent has been started successfully")

    def running(self):
        try:
            self.sh_client.run([self.exec_path,"status"],"root")
            return True
        except ShError as e:
            return False


class BackupClient(object):
    def __init__(self, sh_client, case_number, backup_base_dir):
        self.sh_client = sh_client
        self.case_number = case_number
        self.backup_dir = path.expanduser("{0}{1}case{2}/".format(backup_base_dir, "/" if not backup_base_dir.endswith("/") else "", self.case_number))
        self.holland_client = HollandClient(self.sh_client)

    def create_backup_dir(self):
        try:
            self.sh_client.run(["mkdir",self.backup_dir])
        except CalledProcessError as e:
            raise ShError(e.message)

    def create_backup(self):
        tms = datetime.now().strftime('%Y-%m-%d_%H:%M')
        _command = ("readlink -e /etc/ambari-* /etc/smartsense* " +
                   "/etc/hst /etc/yum.repos.d /usr/hdp/current/zeppelin-server/notebook " +
                   "/etc/zeppelin /var/lib/ambari-server/ambari-env.sh")
        files = " ".join(self.sh_client.run(_command, "root", True).split("\n")[:-1])gi
        self.create_backup_dir()
        _tar_command = shlex.split("tar --ignore-failed-read -cvzf {0}backup-files_{1}.tar.gz {2}"\
                                  .format(self.backup_dir,tms,files))
        self.holland_client.create_backup(_tar_command,"root")
        self.sh_client.run("cp -aL {0} {1}".format(self.holland_client.get_newest_backup_dir(), self.backup_dir), "root")


class HollandClient(object):
    def __init__(self, sh_client, exec_path="/usr/sbin/holland"):
        self.sh_client = sh_client
        self.exec_path = exec_path

    def create_backup(self,backupset="default"):
        self.sh_client.run(self.exec_path,"backup {0}".format(backupset),"root")

    def get_base_backup_dir(self,backupset="default"):
        return "{0}/{1}".format(self.sh_client.run("/bin/grep -r 'backup_dir' /etc/holland/holland.conf", "root").split(" = ")[1].split("\n")[0],backupset)

    def get_newest_backup_dir(self,backupset="default"):
        return "{0}newest/".format(self.get_base_backup_dir(backupset))
