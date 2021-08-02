import requests
from azure.mgmt.batch import BatchManagementClient


urllocal = "http://localhost:7071/api/BatchHttpTrigger"
urlremote = "https://azure-batch.azurewebsites.net/api/BatchHttpTrigger"
#urlremote = "https://azure-batch-function.azurewebsites.net/api/BatchHttpTrigger"



data_file = 'person.csv'
mapping_file = 'person_map.json'
try:
	targeturl = urlremote
	# url
	data = {"data_file":"{}".format(data_file), "mapping_file":"{}".format(mapping_file)}
	response = requests.post(url=targeturl, json=data, verify=False, headers={"content-type": "text/json",},)
	print("Results for {} are {}:".format(targeturl, response.text))
	# url2
	#targeturl = urllocal
	data = {"data_file":"{}".format(data_file), "mapping_file":"{}".format(mapping_file)}
	response = requests.post(url=targeturl, json=data, verify=False, headers={"content-type": "text/json",},)
	print("Results for {} are {}:".format(targeturl, response.text))
except Exception as ex:
	print(ex)
	pass

#data = "{'data_file': '{}', 'mapping_file': '{}'".format(data_file, mapping_file)


