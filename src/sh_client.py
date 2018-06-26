from subprocess import check_output, CalledProcessError

from sh_error import HstError,AmbariServerError


class HstClient(object):
    def __init__(self,client_path=None):
        self.client_path = self.get_client_path(client_path)

    def get_client_path(self, client_path):
        if not client_path:
            try:
                return check_output(["which","hst"]).split("\n")[0]
            except CalledProcessError as e:
                raise HstError("hst executeble is not on the PATH, please provide a valid path or install hst")
        else:
            try:
                check_output([client_path,"--version"])
                return client_path
            except OSError as e:
                raise HstError("hst path is not valid: {0}".format(e.strerror))

    def capture_bundle(self):
        try:
            print("Capturing smartsense bundle...")
            check_output(["sudo",self.client_path,"capture"])
            print("Smartsense boundle captured")
        except CalledProcessError as e:
            raise HstError("Smartsense capture has been failed, please check hst logs for details")


class AmbariServer(object):
    def __init__(self, client_path=None):
        self.client_path = self.get_path(client_path)

    def get_path(self, client_path):
        if not client_path:
            try:
                return check_output(["which","ambari-server"]).split("\n")[0]
            except CalledProcessError as e:
                raise AmbariServerError("ambari-server executeble is not on the PATH, please provide a valid path or install ambari-server")
        else:
            try:
                check_output([client_path,"--version"])
                return client_path
            except OSError as e:
                raise AmbariServerError("ambari-server path is not valid: {0}".format(e.strerror))

    def stop(self):
        try:
            print("Stopping ambari-server...")
            check_output(["sudo",self.client_path,"stop"])
            if self.check_if_running():
                raise AmbariServerError("Stopping ambari-server was not successful, please check")
            print("Ambari server has been stopped successfully")
        except CalledProcessError as e:
            raise AmbariServerError("Some problem have been occurred during stopping ambari-server")

    def start(self):
        try:
            print("Starting ambari-server...")
            check_output(["sudo",self.client_path,"start"])
            if not self.check_if_running():
                raise AmbariServerError("Starting ambari-server was not successful, please check")
            print("Ambari server has been started successfully")
        except CalledProcessError as e:
            raise AmbariServerError("Some problem have been occurred during starting ambari-server")

    def check_if_running(self):
        try:
            check_output(["sudo","cat","/var/run/ambari-server/ambari-server.pid"])
            return True
        except CalledProcessError as e:
            return False