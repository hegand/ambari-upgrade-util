import urllib2,json
from time import sleep

from ambari_service_payload_templates import ServiceStartStopPayloadTemplate, ServiceMaintenanceModePayloadTemplate
from ambari_error import AmbariError


class AmbariClient(object):
    def __init__(self,hostname,port,cluster_name,creds,ssl=False):
        self.hostname = hostname
        self.port = port
        self.cluster_name = cluster_name
        self.creds = creds
        self.base_url = "http{0}://{1}:{2}/api/v1/clusters/{3}/".format("s" if ssl else "", self.hostname, self.port, self.cluster_name)
        self.test_base_url()
        self.service_list = self.get_service_list()

    def test_base_url(self):
        url = self.base_url[:-len(self.cluster_name)-1]
        data = self.get_json(url)
        try:
            cluster_names = [x["Clusters"]["cluster_name"] for x in data["items"]]
            if self.cluster_name in cluster_names:
                return
            else:
                raise AmbariError("This cluster ({0}) is not managed by this ambari instance ({1}:{2})".format(self.cluster_name,self.hostname,str(self.port)))
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def request(self, url, data, type=None):
        request = urllib2.Request(url, data, {"X-Requested-By": "ambari", "Authorization": "Basic {0}".format(self.creds)})
        if type is not None:
            request.get_method = lambda: type
        try:
            connection = urllib2.urlopen(request)
            return connection.read()
        except urllib2.HTTPError as e:
            if e.code >= 400:
                raise AmbariError(json.loads(e.read())["message"])
        except urllib2.URLError as e:
            raise AmbariError("Please check the url, {0} is not valid or the server is not responding. {1}".format(url,e.reason))

    def get(self,url):
        return self.request(url, None)

    def get_json(self,url):
        try:
            return json.loads(self.get(url))
        except ValueError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def post(self, url, data):
        return self.request(url, data)

    def post_json(self,url,data):
        try:
            return json.loads(self.post(url,data))
        except ValueError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def put(self, url, data):
        return self.request(url, data, "PUT")

    def put_json(self,url,data):
        try:
            return json.loads(self.put(url,data))
        except ValueError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def get_hosts(self):
        url = "{0}hosts/".format(self.base_url)
        resp = self.get_json(url)
        try:
            return [x["Hosts"]["host_name"] for x in resp["items"]]
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def get_service_list(self):
        url = "{0}services/".format(self.base_url)
        resp = self.get_json(url)
        try:
            return [x["ServiceInfo"]["service_name"] for x in resp["items"]]
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def get_service_info(self, service_name):
        url = "{0}services/{1}".format(self.base_url,service_name)
        return self.get_json(url)

    def get_request_info(self, reqid):
        url = "{0}requests/{1}".format(self.base_url,reqid)
        return self.get_json(url)

    def get_service_maintenance_state(self, service_name):
        try:
            return self.get_service_info(service_name)["ServiceInfo"]["maintenance_state"]
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def get_service_state(self, service_name):
        try:
            return self.get_service_info(service_name)["ServiceInfo"]["state"]
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def get_request_state(self, reqid):
        try:
            return self.get_request_info(reqid)["Requests"]["request_status"]
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def turn_maintenance_mode_for_service(self, action, service_name):
        if service_name not in self.service_list:
            raise AmbariError("{0} is not available on this cluster".format(service_name))
        if action == self.get_maintenance_mode_state_for_service(service_name):
            raise AmbariError("{0} is already in {1} maintenance state on this cluster".format(service_name,action))
        url = self.base_url + "services/{0}".format(service_name)
        payload = ServiceMaintenanceModePayloadTemplate(action,service_name).get()
        self.put(url, payload)
        i = 0
        k = False
        while i<5:
            state = self.get_service_maintenance_state(service_name)
            i = i+1
            print(state)
            if state == action:
                k=True
                break
            sleep(5)
        if not k:
            raise AmbariError("Turning {0} maintenance mode for {1} service on {2} cluster was not successful".format(action, service_name, self.cluster_name))
        return

    def get_maintenance_mode_state_for_service(self, service_name):
        try:
            return self.get_service_info(service_name)["ServiceInfo"]["maintenance_state"]
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")

    def turn_on_maintenance_mode_for_service(self, service_name):
        self.turn_maintenance_mode_for_service("ON", service_name)

    def turn_off_maintenance_mode_for_service(self, service_name):
        self.turn_maintenance_mode_for_service("OFF", service_name)

    def switch_service_state(self, action, service_name):
        if service_name not in self.service_list:
            raise AmbariError("{0} is not available on this cluster".format(service_name))
        if action == self.get_service_state(service_name):
            raise AmbariError("{0} is already in {1} state on this cluster".format(service_name,action))
        url = "{0}services/{1}".format(self.base_url,service_name)
        payload = ServiceStartStopPayloadTemplate(action,self.cluster_name,service_name).get()
        resp = self.put_json(url, payload)
        try:
            reqid = resp["Requests"]["id"]
        except KeyError as e:
            raise AmbariError("Server response is not valid or empty, please check")
        i = 0
        k = False
        while i<50:
            state = self.get_request_state(reqid)
            i = i+1
            print(state)
            if state == "COMPLETED":
                k=True
                break
            sleep(6)
        if not k:
            raise AmbariError("Changing {0} service state to {1} on {2} cluster was not successful".format(service_name,action, self.cluster_name))
        return

    def stop_service(self, service_name):
        self.switch_service_state("INSTALLED",service_name)

    def start_service(self, service_name):
        self.switch_service_state("STARTED",service_name)
