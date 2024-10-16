# Oracle Database@Azure: Create an Autonomous Database
The steps below show how to create an Autonomous Database using the Azure CLI. You will need to [install the Azure CLI](https://learn.microsoft.com/en-us/cli/azure/) prior to running thru this example.

Run the examples below at a command prompt.

> [!NOTE]
> See the [details for onboarding Autonomous Database](https://learn.microsoft.com/en-us/azure/oracle/oracle-db/onboard-oracle-database)

## Deploy your Autonomous Database
Start by logging into Azure:
```bash
az login 
```

Assign variables that will be used by the Azure CLI:

```bash
LOCATION="eastus"
RESOURCE_GROUP="resource-group-name-goes-here"
VNET_ID="vnet-resource-name-goes-here"
SUBNET_ID="subnet-resource-name-goes-here"
ADB_NAME="adb-name-goes-here"
ADMIN_PASSWORD="your-adb-admin-password"
```

Create a new database:
```bash
az oracle-database autonomous-database create \
--location $LOCATION \
--autonomousdatabasename $ADB_NAME \
--resource-group $RESOURCE_GROUP \
--subnet-id $SUBNET_ID \
--display-name $ADB_NAME \
--compute-model ECPU \
--compute-count 2 \
--cpu-auto-scaling true \
--data-storage-size-in-gbs 500 \
--license-model BringYourOwnLicense \
--db-workload OLTP \
--db-version 19c \
--character-set AL32UTF8 \
--ncharacter-set AL16UTF16 \
--vnet-id $VNET_ID \
--regular \
--admin-password $ADMIN_PASSWORD
```

After a few minutes, review your newly created database:
```bash
az oracle-database autonomous-database show \
--autonomousdatabasename $ADB_NAME \
--resource-group $RESOURCE_GROUP
```

Drop your Autonomous Database
```bash
az oracle-database autonomous-database delete \
--autonomousdatabasename $ADB_NAME \
--resource-group $RESOURCE_GROUP \
--no-wait false
```



<hr>
Copyright (c) 2024 Oracle and/or its affiliates.<br>
  Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
