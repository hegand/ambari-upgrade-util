from subprocess import check_output, CalledProcessError
from time import sleep

from sh_error import HstError,AmbariServerError,SshError,ShError


class ShClient(object):
    def __init__(self):
        self.checking()

    def checking(self):
        try:
            self.run("whoami")
        except CalledProcessError as e:
            raise ShError("Some problems occurred during {0} initialization: {1}".format(self.__class__.__name__, e.message))

    @staticmethod
    def construct_command(command, runasuser=None):
        if type(command) is str:
            _command = [command]
        elif type(command) is list:
            _command = command
        else:
            raise ShError("Command must be string or list")
        if runasuser is not None:
            _command = ["sudo","-u",runasuser] + _command
        return _command

    def run(self,command,runasuser=None):
        _command = self.construct_command(command,runasuser)
        try:
            return check_output(_command)
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
        if runaslocaluser is not None:
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
