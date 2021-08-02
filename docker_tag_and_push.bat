call az acr login --name registryetl
call docker tag brunoterkaly/python-etl:latest registryetl.azurecr.io/images/python-etl:latest
call docker push registryetl.azurecr.io/images/python-etl:latest
