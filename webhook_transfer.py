import requests
import json
import random
import string
import time
import urllib3

url='https://kube-auditing-webhook-svc.kubesphere-logging-system.svc:6443/audit/webhook/event'
#url = 'https://10.203.1.11:30635/audit/webhook/event'
#url = 'https://10.203.1.210:30399/audit/webhook/event'

def wh_interface(Time,Workspace,Reason,AuditResType,ResName,SourceIPs,LogLevel):
    #print('webhokk info from bmcprogram',Time,Workspace,Reason,AuditResType,ResName,SourceIPs,LogLevel)

    audit_id_audit=''.join(random.sample(string.ascii_lowercase + string.digits,8)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 4)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 4)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 4)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 12))
    #print(audit_id_audit)
    headers = {
        'content-type': 'application/json'
    }
    data = {

        "Items": [

            {

                "Devops": "",

                "Workspace": Workspace,

                "Cluster": "",

                "Message": "",

                "Level": "Metadata",

                "AuditID": audit_id_audit,

                "Stage": "ResponseComplete",

                "RequestURI": "",

                "Verb": "",

                "User": {

                    "username": "system",

                    "groups": [

                        "system:authenticated"

                    ]

                },
                "ImpersonatedUser": None,

                "SourceIPs": [

                    SourceIPs

                ],

                "UserAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",

                "ObjectRef": {

                    "Resource": ResName,

                    "Namespace": "",

                    "Name": AuditResType,

                    "UID": "",

                    "APIGroup": "",

                    "APIVersion": "",

                    "ResourceVersion": Workspace,

                    "Subresource": ""

                },

                "ResponseStatus": {
                    "code":0,
                    "metadata": {},
                    "status": LogLevel,
                    "reason": Reason
                },
                "RequestObject": None,

                "ResponseObject": None,

                "RequestReceivedTimestamp": Time,

                "StageTimestamp": Time,

                "Annotations": None

            }

        ]

     }
    data = json.dumps(data)
    #print(data)
    err_signal = True
    for i in range(10):
      try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.post(url,headers=headers, verify=False ,data=data)
        #print(response)
        #print('Data was written successfully,AuditID:',audit_id_audit)
        err_signal = False
        break
      except:
        time.sleep(2)
    if err_signal:
      print('Tried to rewrite the data 10 times, but the data write failed!')




def wh_interface_many(events_list):
    #print('webhokk info from bmcprogram',Time,Workspace,Reason,AuditResType,ResName,SourceIPs,LogLevel)
    #print(audit_id_audit)
    headers = {
        'content-type': 'application/json'
    }

    data = {"Items": []}

    for event in  events_list:

        audit_id_audit=''.join(random.sample(string.ascii_lowercase + string.digits, 8)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 4)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 4)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 4)) + '-' + ''.join(random.sample(string.ascii_lowercase + string.digits, 12))

        item = {

            "Devops": "",

            "Workspace": event[1],

            "Cluster": "",

            "Message": "",

            "Level": "Metadata",

            "AuditID": audit_id_audit,

            "Stage": "",

            "RequestURI": "",

            "Verb": "",

            "User": {

                "username": "system",

                "groups": [

                    ""

                ]

            },
            "ImpersonatedUser": None,

            "SourceIPs": [

                event[5]

            ],

            "ObjectRef": {

                "Resource": event[4],

                "Namespace": "",

                "Name": event[3],

                "UID": "",

                "APIGroup": "",

                "APIVersion": "",

                "ResourceVersion": event[1],

                "Subresource": ""

            },

            "ResponseStatus": {
                "code":0,
                "metadata": {},
                "status": event[6],
                "reason": event[2]
            },
            "RequestObject": None,

            "ResponseObject": None,

            "RequestReceivedTimestamp": event[0],

            "Annotations": None

        }

        data['Items'].append(item)


    data = json.dumps(data)
    #print(data)
    err_signal = True
    for i in range(10):
      try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.post(url,headers=headers, verify=False ,data=data)
        #print(response)
        #print('Data was written successfully,AuditID:',audit_id_audit)
        err_signal = False
        break
      except:
        time.sleep(2)
    if err_signal:
      print('Tried to rewrite the data 10 times, but the data write failed!')


