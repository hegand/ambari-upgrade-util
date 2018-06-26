from subprocess import check_output, CalledProcessError

from sh_error import HstError,AmbariServerError


class HstClient(object):
    def __init__(self,client_path=None):
        self.client_path = self.get_client_path(client_path)

    def get_client_path(self, client_path):
        if not client_path:
            try:
                return check_output(["which","hst"])
            except CalledProcessError as e:
                raise HstError("hst executeble is not on the PATH, please provide a valid path or install hst")
        else:
            try:
                check_output(["hst","--version"])
                return client_path
            except OSError as e:
                raise HstError("hst path is not valid: {0}".format(e.strerror))

    def capture_bundle(self):
        try:
            path = self.client_path
            check_output([path,"capture"])
            print("Smartsense boundle captured")
        except CalledProcessError as e:
            raise HstError("Smartsense capture has been failed, please check hst logs for details")


class AmbariServer(object):
    def __init__(self, client_path=None):
        self.client_path = self.get_path(client_path)

    def get_path(self, client_path):
        if not client_path:
            try:
                return check_output(["which","ambari-server"])
            except CalledProcessError as e:
                raise AmbariServerError("ambari-server executeble is not on the PATH, please provide a valid path or install ambari-server")
        else:
            try:
                check_output(["ambari-server","--version"])
                return client_path
            except OSError as e:
                raise AmbariServerError("ambari-server path is not valid: {0}".format(e.strerror))

    def stop_ambari_server(self):
        try:
            check_output(["sudo","ambari-server stop"])
        except CalledProcessError as e:
            raise AmbariServerError("Some problem have been occurred during stopping ambari-server: {0}".format(e.message))

    def start_ambari_server(self):
        try:
            check_output(["sudo","ambari-server start"])
        except CalledProcessError as e:
            raise AmbariServerError("Some problem have been occurred during starting ambari-server: {0}".format(e.message))