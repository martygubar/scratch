## Usage analysis on OCI - using the API

**Install the oci_cli_rc file**

This file lets you save query definitions and then refer to them.

Setup:

```bash
oci setup oci-cli-rc
```

Add this query to the rc file:

```
get_usage=data.items[*].{"tenant-name":"tenant-name", "region":"region", "compartment-name":"compartment-name",service: service, "sku-name": "sku-name", "computed-quantity": "computed-quantity","unit":"unit","time-usage-ended":"time-usage-ended","time-usage-started":"time-usage-started" }

```

Endpoint: `https://usageapi.us-ashburn-1.oci.oraclecloud.com/20200107/usage`

**Create an input file for the API parameters**

File: usage.json

```json
{
  "granularity": "MONTHLY",
  "groupBy": [
    "skuName",
    "region",
    "tenantName",
    "compartmentName"
  ],
  "compartmentDepth":4,
  "queryType": "USAGE",
  "tenantId": "ocid1.tenancy.oc1..aaaaaaaafcue47pqmrf4vigneebgbcmmoy5r7xvoypicjqqge32ewnrcyx2a",
  "timeUsageEnded": "2022-08-22",
  "timeUsageStarted": "2022-01-01"
}
```

**Get the usage summary**

```bash
oci usage-api usage-summary request-summarized-usages --from-json file:///Users/mgubar/Code/scratch/scratch/tips/usage.json --query query://get_usage 
```

**Request Body**

```json
{
  "granularity": "MONTHLY",
  "groupBy": [
    "skuName",
    "region",
    "tenantName",
    "compartmentName"
  ],
  "compartmentDepth":4,
  "queryType": "USAGE",
  "tenantId": "ocid1.tenancy.oc1..aaaaaaaafcue47pqmrf4vigneebgbcmmoy5r7xvoypicjqqge32ewnrcyx2a",
  "timeUsageEnded": "2022-08-22",
  "timeUsageStarted": "2022-08-01"
}
```