-- will capture daily usage
-- rolling 90 days
drop table oci_daily_usage_raw;
drop table oci_monthly_usage_raw;
create table oci_daily_usage_raw (
    acct_name varchar2(100),
    oci_usage clob constraint ocu_ensure_json check (oci_usage is json),
    loaded_date date
);

-- historical
create table oci_monthly_usage_raw (
    acct_name varchar2(100),
    oci_usage clob constraint omu_ensure_json check (oci_usage is json),
    loaded_date date
);

-- 
create table load_log
(	execution_time timestamp (6),
    message varchar2(32000 byte),
    session_id number,
    username   varchar2(30)
);
   
select json_transform(x, remove '$.items[*].*?(@ == null)') 
from res;

-- For each tenancy do....
-- Daily (run every day):
--   1. Fetch from the latest run to today - 1. If empty, get the latest 90 days (max)
--   2. Fetch latest 90 days for each tenancy
-- Monthly (run every day):
--   1. Fetch from latest date. If empty, get 365 days (max)
--   2. Daily, delete records for this month
--   3. Fetch data for this month for each tenancy


declare
    response    clob default null;
    trimmed_response    clob default null;
    cred varchar2(15) := 'OCI_ADWC4PM';
    u varchar2(100)   := 'https://usageapi.us-ashburn-1.oci.oraclecloud.com/20200107/usage';
    resp                    DBMS_CLOUD_TYPES.resp;
    req_body blob;
begin
--    dbms_output.put_line('Send Request - '||rsrc);
    req_body := utl_raw. cast_to_raw(
        '{
          "granularity": "DAILY",
          "groupBy": [
            "skuName",
            "region",
            "tenantName",
            "compartmentName"
          ],
          "compartmentDepth":4,
          "queryType": "USAGE",
          "tenantId": "ocid1.tenancy.oc1..aaaaaaaafcue47pqmrf4vigneebgbcmmoy5r7xvoypicjqqge32ewnrcyx2a",
          "timeUsageEnded":   "2022-08-02T00:00:00+00:00",
          "timeUsageStarted": "2022-08-01T00:00:00+00:00"
        }');
        
         
    resp := DBMS_CLOUD.send_request(
            credential_name => 'OCI_ADWC4PM',
            uri => u,
            method => DBMS_CLOUD.METHOD_POST,
            headers => JSON_OBJECT('opc-request-id' value 'list-usage'),
            body => req_body
          );

    response := dbms_cloud.get_response_text(resp);
    select json_transform(response, remove '$.items[*].*?(@ == null)')
    into trimmed_response
    from dual;
    
    insert into res (date_id, x) values (sysdate, trimmed_response); 
    commit;
    begin
        dbms_output.put_line(response);
    exception
    when others then
        dbms_output.put_line('ERROR \n' || substr(response,1,200));
    end;
end;    
/


select * from res order by 1 desc;
select systimestamp from dual;
select * from oci_tenancies;

select * from oci_rsrc_info;

declare
    response    clob default null;
    cred varchar2(15) := 'OCI_ADWC4PM';
    u varchar2(100)   := 'https://usageapi.us-ashburn-1.oci.oraclecloud.com/20200107/usage';
    resp                    DBMS_CLOUD_TYPES.resp;
    req_body blob;
begin
--    dbms_output.put_line('Send Request - '||rsrc);
    req_body := utl_raw. cast_to_raw(
        '{
          "region": "eu-paris-1",
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
          "timeUsageEnded":   "2022-08-24T00:00:00+00:00",
          "timeUsageStarted": "2022-08-01T00:00:00+00:00"
        }');
        
         
    resp := DBMS_CLOUD.send_request(
            credential_name => 'OCI_ADWC4PM',
            uri => u,
            method => DBMS_CLOUD.METHOD_POST,
            headers => JSON_OBJECT('opc-request-id' value 'list-usage'),
            body => req_body
          );
--    dbms_output.put_line('Headers: ' || CHR(10) || '------------' || CHR(10) || DBMS_CLOUD.get_response_headers(resp).to_clob || CHR(10));
--    dbms_output.put_line('Body: ' || '------------' || CHR(10) || substr(json_obj,100) || CHR(10));


    response := DBMS_CLOUD.get_response_text(resp);
    insert into res (date_id, x) values (sysdate, response); 
    commit;
    begin
        dbms_output.put_line(response);
    exception
    when others then
        dbms_output.put_line('ERROR \n' || substr(response,1,200));
    end;
end;    
/


--
-- WORKING
--
declare
    response    clob default null;
    cred varchar2(15) := 'OCI_ADWC4PM';
    u varchar2(100)   := 'https://usageapi.us-ashburn-1.oci.oraclecloud.com/20200107/usage';
    resp                    DBMS_CLOUD_TYPES.resp;
    req_body blob;
begin
--    dbms_output.put_line('Send Request - '||rsrc);
    req_body := utl_raw. cast_to_raw(
        '{          
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
          "timeUsageEnded":   "2022-08-24T00:00:00+00:00",
          "timeUsageStarted": "2022-08-01T00:00:00+00:00",
          "filter": {
              "operator": "AND",
              "dimensions": [
                {
                  "key": "region",
                  "value": "compute"
                },
                {
                  "key": "compartmentPath",
                  "value": "abc/cde"
                }
              ],
              "tags": [
                {
                  "namespace": "compute",
                  "key": "created",
                  "value": "string"
                }
              ],
              "filters": null
            }
        }'
        );
        
         
    resp := DBMS_CLOUD.send_request(
            credential_name => 'OCI_ADWC4PM',
            uri => u,
            method => DBMS_CLOUD.METHOD_POST,
            headers => JSON_OBJECT('opc-request-id' value 'list-usage'),
            body => req_body
          );
--    dbms_output.put_line('Headers: ' || CHR(10) || '------------' || CHR(10) || DBMS_CLOUD.get_response_headers(resp).to_clob || CHR(10));
--    dbms_output.put_line('Body: ' || '------------' || CHR(10) || substr(json_obj,100) || CHR(10));


    response := DBMS_CLOUD.get_response_text(resp);
    insert into res (date_id, x) values (sysdate, response); 
    commit;
    begin
        dbms_output.put_line(response);
    exception
    when others then
        dbms_output.put_line('ERROR \n' || substr(response,1,200));
    end;
end;    
/


select *
from oci_rsrc_info;
select *
from res
order by 1;


--"query":"data.items[*].{\"tenant-name\":\"tenant-name\", \"region\":\"region\", \"compartment-name\":\"compartment-name\",service: service, \"sku-name\": \"sku-name\", \"computed-quantity\": \"computed-quantity\",\"unit\":\"unit\",\"time-usage-ended\":\"time-usage-ended\",\"time-usage-started\":\"time-usage-started\" }"

select * from res;
delete res where x not like '%group%';
select json_query(x, '$.groupBy')
from res;


create or replace view oci_monthly_usage as
with all_metrics as (
select
    acct_name,
    loaded_date,
    tenant,
    compartment,
    region,
    sku,
    unit,
    time_usage_started,
    time_usage_ended,
    quantity,
    case when upper(unit) = 'OCPU HOURS' 
        then round(quantity, 0)
        else null end as ocpu_hours,
    case 
        when upper(unit) = 'GIGABYTE PER MONTH' 
            then round(quantity /1000 , 2)
        when upper(unit) = 'TB' 
            then round(quantity, 2)
        else null end as TB        
from oci_monthly_usage_raw m, 
     json_table (
        oci_usage, '$.items[*]'
        columns (
            tenant varchar2(100) path '$.tenantName',
            compartment varchar2(100) path '$.compartmentName',
            region varchar2(100) path '$.region',
            sku varchar2(100) path '$.skuName',
            unit varchar2(100) path '$.unit',
            time_usage_started date path '$.timeUsageStarted',
            time_usage_ended date path '$.timeUsageEnded',
            quantity number path '$.computedQuantity'
        )     
     )
where loaded_date in (select max(loaded_date) from oci_monthly_usage_raw)
)
select
    acct_name,
    loaded_date,
    tenant,
    compartment,
    region,
    sku,
    unit,    
    time_usage_ended,
    ocpu_hours,
    tb
from all_metrics
where ocpu_hours is not null
or tb is not null;
;

select * from oci_monthly_usage
where rownum < 10;

select * from oci_monthly_usage_raw;


create or replace   procedure write
        /* write message to the log
       use style to format output
       -1 = error       #! Error !# message
        1 = header(h1)  { message }
        2 = header (h2) -> message
        3 = header (h3) --> message
        4 = list item       -
        null = plan     message
    */
    (
      message in varchar2 default null,
      style   in number default null

    ) as
        l_message varchar2(32000);

    begin

        if message is null then
            return;
        end if;

        dbms_output.put_line(to_char(systimestamp, 'DD-MON-YY HH:MI:SS') || ' - ' || message);

        -- Add style to the message
        l_message := case style
                        when null then
                            message
                        when -1 then
                            '#! Error !#' || message
                        when 1 then
                            '[- ' || message || ' -]'
                        when 4 then
                            '    - ' || message
                        else
                           substr('------------------------->', (style * -2)) || ' ' || message
                      end;

        execute immediate 'insert into load_log(execution_time, session_id, username, message) values(:t1, :sid, :u, :msg)'
                using systimestamp, sys_context('USERENV', 'SID'), sys_context('USERENV', 'SESSION_USER'),  l_message;
        commit;


    end write;
/    