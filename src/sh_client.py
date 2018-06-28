from subprocess import check_output, CalledProcessError
from time import sleep

from sh_error import HstError,AmbariServerError,SshError,ShError


class ShClient(object):
    def __init__(self,name,client_path=None):
        self.client_path = self.get_client_path(name,client_path)

    @staticmethod
    def get_client_path(name,client_path):
        if not client_path:
            try:
                return check_output(["which",name]).split("\n")[0]
            except CalledProcessError as e:
                raise ShError("{0} executeble is not on the PATH, please provide a valid path or install {0}".format(name))
        else:
            try:
                check_output([client_path,"--version"])
                return client_path
            except OSError as e:
                raise ShError("{0} path is not valid: {1}".format(name,e.strerror))

    def run(self, command, runasuser=None):
        if type(command) is str:
            _command = [command]
        elif type(command) is list:
            _command = command
        else:
            raise TypeError("Command must be string or list")
        if runasuser:
            _command = ["sudo","-u",runasuser] + _command
        try:
            check_output(["TERM=dumb"] + _command)
        except CalledProcessError as e:
            raise SshError(e.message)


class HstClient(ShClient):
    def __init__(self,client_path=None):
        ShClient.__init__(self,"hst",client_path)

    def capture_bundle(self):
        try:
            print("Capturing smartsense bundle...")
            self.run([self.client_path,"capture"],"root")
            print("Smartsense boundle captured")
        except CalledProcessError as e:
            raise HstError("Smartsense capture has been failed, please check hst logs for details")


class AmbariServer(ShClient):
    def __init__(self,client_path=None):
        ShClient.__init__(self,"ambari-server",client_path)

    def stop(self):
        try:
            print("Stopping ambari-server...")
            if self.running():
                self.run([self.client_path,"stop"],"root")
            else:
                raise AmbariServerError("Ambari server is not running")
            sleep(10)
            if self.running():
                raise AmbariServerError("Stopping ambari-server was not successful, please check")
            print("Ambari server has been stopped successfully")
        except CalledProcessError as e:
            raise AmbariServerError("Some problems have been occurred during stopping ambari-server")

    def start(self):
        try:
            print("Starting ambari-server...")
            self.run([self.client_path,"start"],"root")
            sleep(10)
            if not self.running():
                raise AmbariServerError("Starting ambari-server was not successful, please check")
            print("Ambari server has been started successfully")
        except CalledProcessError as e:
            raise AmbariServerError("Some problems have been occurred during starting ambari-server")

    def running(self):
        try:
            self.run(["[ -f /var/run/ambari-server/ambari-server.pid ] && exit || exit 1"],"root")
            return True
        except CalledProcessError as e:
            return False


class SshClient(object):
    def __init__(self, hostname, username=None, runasuser=None):
        command = ["ssh"]
        if runasuser:
            command.append("{0}@{1}".format(username,hostname))
        else:
            command.append(hostname)
        if username:
            command = ["sudo","-u",runasuser] + command
        self.base_command = command + ["TERM=dumb"]
        self.hostname = hostname
        self.checking()

    def checking(self):
        self.run("whoami")

    def run(self, command, runasremoteuser=None):
        if type(command) is str:
            _command = [command]
        elif type(command) is list:
            _command = command
        else:
            raise TypeError("Command must be string or list")
        if runasremoteuser:
            _command = ["sudo","-u",runasremoteuser] + _command
        try:
            check_output(self.base_command + _command)
        except CalledProcessError as e:
            raise SshError(e.message)


class AmbariAgent(SshClient):
    def __init__(self, hostname, username=None, runasuser=None):
        SshClient.__init__(self,hostname,username,runasuser)

    def stop(self):
        print("Stopping ambari-agent on {0}...".format(self.hostname))
        if self.running():
            self.run(["ambari-agent","stop"],"root")
        else:
            raise SshError("Ambari-agent is not running on {0}".format(self.hostname))
        sleep(10)
        if self.running():
            raise SshError("Stopping ambari-agent was not successful on {0}, please check".format(self.hostname))
        print("Ambari-agent has been stopped successfully on {0}".format(self.hostname))

    def start(self):
        print("Starting ambari-agent on {0}...".format(self.hostname))
        if not self.running():
            self.run(["ambari-agent","start"],"root")
        else:
            raise SshError("Ambari-agent is already running on {0}".format(self.hostname))
        sleep(10)
        if not self.running():
            raise SshError("Starting ambari-agent was not successful on {0}, please check".format(self.hostname))
        print("Ambari-agent has been started successfully on {0}".format(self.hostname))

    def running(self):
        try:
            self.run(["[ -f /var/run/ambari-agent/ambari-agent.pid ] && exit || exit 1"],"root")
            return True
        except SshError as e:
            return False
