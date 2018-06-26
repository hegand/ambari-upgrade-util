import json

class ServiceStartStopPayloadTemplate(object):
    def __init__(self, action, cluster_name, service_name):
        self.payload = '' + \
        '{' + \
        '  "RequestInfo":{' + \
        '      "context":"Changing {0} service state to {1}",'.format(service_name,action) + \
        '      "operation_level":{' + \
        '         "level":"SERVICE",' + \
        '         "cluster_name":"{0}",'.format(cluster_name) + \
        '         "service_name":"{0}"'.format(service_name) + \
        '       }' + \
        '   },' + \
        '  "Body":{' + \
        '      "ServiceInfo":{' + \
        '          "state": "{0}"'.format(action) + \
        '       }' + \
        '   }' + \
        '}'

    def get(self):
        return self.payload

    def get_json(self):
        return json.loads(self.payload)

class ServiceMaintenanceModePayloadTemplate(object):
    def __init__(self, action, service_name):
        self.payload = '' + \
        '{' + \
        '  "RequestInfo":{' + \
        '      "context":"Turning {0} maintenance mode for service {1}"'.format(action,service_name) + \
        '   },' + \
        '  "Body":{' + \
        '      "ServiceInfo":{' + \
        '          "maintenance_state": "{0}"'.format(action) + \
        '       }' + \
        '   }' + \
        '}'

    def get(self):
        return self.payload

    def get_json(self):
        return json.loads(self.payload)
