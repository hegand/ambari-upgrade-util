from subprocess import check_output, CalledProcessError
from time import sleep
from datetime import datetime
from os import path
import shlex

from sh_error import HstError,AmbariServerError,SshError,ShError


class ShClient(object):
    def __init__(self):
        self.checking()
        self.sudo = self.checking_sudo()

    def checking(self):
        try:
            self.run("whoami")
        except CalledProcessError as e:
            raise ShError("Some problems occurred during {0} initialization: {1}".format(self.__class__.__name__, e.message))

    def checking_sudo(self):
        try:
            if self.run(["sudo","whoami"]) == 'root':
                return True
            else:
                return False
        except CalledProcessError as e:
            return False

    @staticmethod
    def construct_command(command, runasuser=None):
        if type(command) is str:
            _command = [command]
        elif type(command) is list:
            _command = command
        else:
            raise ShError("Command must be string or list")
        if runasuser == "root":
            _command = ["sudo"] + _command
        elif runasuser is not None:
            _command = ["sudo","-u",runasuser] + _command
        return _command

    def run(self,command,runasuser=None,shell=False):
        if runasuser is not None and not self.sudo:
            raise ShError("The current user does not have sudo right")
        _command = self.construct_command(command,runasuser)
        try:
            return check_output(_command,shell=shell)
        except CalledProcessError as e:
            raise ShError(e.message)


class SshClient(ShClient):
    def __init__(self, hostname, remoteuser=None, runaslocaluser=None):
        self.base_command = self.construct_ssh_base_command(hostname, remoteuser, runaslocaluser)
        self.hostname = hostname

    def construct_command(self, command, runasuser=None):
        return self.base_command + super(SshClient,self).construct_command(command,runasuser)

    @staticmethod
    def construct_ssh_base_command(hostname, remoteuser, runaslocaluser):
        command = ["ssh"]
        if remoteuser is not None:
            command.append("{0}@{1}".format(remoteuser,hostname))
        else:
            command.append(hostname)
        if runaslocaluser == "root":
            command = ["sudo"] + command
        elif runaslocaluser is not None:
            command = ["sudo","-u", runaslocaluser] + command
        return command + ["TERM=dumb"]


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
        self.backup_dir = path.expanduser("{0}case{0}/".format(backup_base_dir,self.case_number))
        self.holland_client = HollandClient(self.sh_client)

    def create_backup_dir(self):
        try:
            self.sh_client.run(["mkdir",self.backup_dir])
        except CalledProcessError as e:
            raise ShError(e.message)

    def create_backup(self):
        tms = datetime.now().strftime('%Y-%m-%d_%H:%M')
        _command = shlex.split("ls -la /etc/ambari-* /etc/smartsense* " +
                               "/etc/hst /etc/yum.repos.d /usr/hdp/current/zeppelin-server/notebook " +
                               "/etc/zeppelin /var/lib/ambari-server/ambari-env.sh " +
                               "| xargs readlink -e")
        files = " ".join(self.sh_client.run(_command, "root", True).split("\n")[:-1])
        self.create_backup_dir()
        _tar_command = shlex.split("tar --ignore-failed-read -cvzf {0}backup-files_{1}.tar.gz {2}"\
                                  .format(self.backup_dir,tms,files))
        self.holland_client.create_backup(_tar_command,"root")
        self.sh_client.run(["cp","-aL", self.holland_client.get_newest_backup_dir(), self.backup_dir],"root")


class HollandClient(object):
    def __init__(self, sh_client, exec_path="/usr/sbin/holland"):
        self.sh_client = sh_client
        self.exec_path = exec_path

    def create_backup(self,backupset="default"):
        self.sh_client.run(self.exec_path,["backup",backupset],"root")

    def get_base_backup_dir(self,backupset="default"):
        return "{0}/{1}".format(self.sh_client.run(["/bin/grep", "-r", "backup_dir", "/etc/holland/holland.conf"], "root").split(" = ")[1].split("\n")[0],backupset)

    def get_newest_backup_dir(self,backupset="default"):
        return "{0}newest/".format(self.get_base_backup_dir(backupset))
