# Swingbench
http://www.dominicgiles.com/blog/files/c84a63640d52961fc28f750570888cdc-169.html


## TPCDS
## TPCDS Challenge - workload is not cpu bound
### create the schema
export WALLET="/home/opc/wallet/Wallet_autoscale_on.zip"
export SB_USER=TPCDS
export SB_PASS=bigdataPM2019#
export SB_SERVICE=autoscaleon_low

cd swingbench/bin
./tpcdswizard -cf $WALLET \
              -cs $SB_SERVICE \
              -ts DATA \
              -its DATA \
              -dbap $SB_PASS \
              -dba admin \
              -u $SB_USER \
              -p $SB_PASS \
              -async_off \
              -scale 10 \
              -create \
              -cl \
              -v
   

### Gather status
./sbutil -soe -cf $WALLET -cs $SB_SERVICE -u $SB_USER -p $SB_PASS -stats     

### Table rowcounts
./sbutil -soe -cf $WALLET  -cs $SB_SERVICE -u $SB_USER -p $SB_PASS -tables

### Run a workload - TPCDS
./charbench -c ../configs/TPCDS_Like_Workload.xml \
            -cf $WALLET \
            -cs $SB_SERVICE \
            -u $SB_USER \
            -p $SB_PASS \
            -v users,tpm,tps,vresp,errs \
            -intermin 0 \
            -intermax 0 \
            -min 0 \
            -max 0 \
            -uc 128 \
            -di SQ,WQ,WA \
            -rt 0:10.30       

## JSON
### create the schema
export WALLET="/home/opc/wallet/Wallet_autoscale_on.zip"
export SB_USER=JSON
export SB_PASS=bigdataPM2019#
export SB_SERVICE=autoscaleon_high

cd swingbench/bin
./jsonwizard -cf $WALLET \
              -cs $SB_SERVICE \
              -ts DATA \
              -its DATA \
              -dbap $SB_PASS \
              -dba admin \
              -u $SB_USER \
              -p $SB_PASS \
              -async_off \
              -scale 10 \
              -create \
              -cl \
              -v
   

### Gather status
./sbutil -$SB_USER -cf $WALLET -cs $SB_SERVICE -u $SB_USER -p $SB_PASS -stats     

### Run a workload - JSON
export WALLET="/home/opc/wallet/Wallet_autoscale_on.zip"
export SB_USER=JSON
export SB_PASS=bigdataPM2019#
export SB_SERVICE=autoscaleon_low

./charbench -c ../configs/JSON_Workload.xml \
            -cf $WALLET \
            -cs $SB_SERVICE \
            -u $SB_USER \
            -p $SB_PASS \
            -v users,tpm,tps,vresp,errs \
            -intermin 0 \
            -intermax 0 \
            -min 0 \
            -max 0 \
            -uc 128 \
            -di SQ,WQ,WA \
            -rt 0:10                 

# ADB Workload Generator - Hot CPUs

export WALLET="/home/opc/wallet/Wallet_autoscale_on.zip"
export DBUSER=admin
export DBPASS=bigdataPM2019#
export DBSERVICE=autoscaleon_low

