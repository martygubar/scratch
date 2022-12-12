# SQL Tips

## sqlcl
### Connect
```sql
sql /nolog
set cloudconfig /Users/mgubar/Code/wallets/Wallet_phx.zip
connect ADMIN@test_high
```
connect MOVIESTREAM@test_high
watchS0meMovies#


## Database details

Tenancy/cloud details

`SELECT cloud_identity FROM v$pdbs;`

Patch level:

`SELECT * FROM DBA_CLOUD_PATCH_INFO`


Export data using datapump:
sql /nolog
set cloudconfig /home/martin_gub/Wallet_phx.zip  # cloudshell
set cloudconfig /Users/mgubar/code/wallets/Wallet_phx.zip
connect MOVIESTREAM@test_high
Pwd: watchS0meMovies#

-- OCI setup for database access to Oracle Object Store
oci profile us-phoenix-1
cs https://objectstorage.us-phoenix-1.oraclecloud.com/n/adwc4pm/b/spatial-debug/o/spatial-debug/

-- Export the current schema into the DATA_PUMP_DIR with automatic copy using OCI setup

dp export -credential DEFAULT -copycloud

-- Import from Oracle Object Store using credential
SQL> set cloudconfig <wallet>
SQL> connect <cloud-connect-string>
SQL> dp import -dumpuri /o/my_dump.dmp -c SWIFTCRED