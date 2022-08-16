# SQL Tips

## sqlcl
### Connect
```sql
sql /nolog
set cloudconfig /Users/ctuzla/Downloads/Wallet_adwfinance_old.zip
connect ADMIN@adwfinance_low
```

## Database details

Tenancy/cloud details

`SELECT cloud_identity FROM v$pdbs;`

Patch level:

`SELECT * FROM DBA_CLOUD_PATCH_INFO`