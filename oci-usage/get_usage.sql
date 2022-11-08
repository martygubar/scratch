
drop table oci_monthly_usage_raw;
create table oci_monthly_usage_raw (
    acct_name varchar2(100),
    oci_usage clob constraint omu_ensure_json check (oci_usage is json),
    month_end_date date
);


 create or replace view OCI_MONTHLY_USAGE AS 
  with all_metrics as (
select
    acct_name,
    month_end_date,    
    compartment,
    region,
    sku,
    resource_id,
    unit,
    time_usage_started,
    time_usage_ended,
    time_usage_ended - 1 as usage_month,
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
            resource_id varchar2(200) path '$.resourceId',
            compartment varchar2(100) path '$.compartmentName',
            region varchar2(100) path '$.region',
            sku varchar2(200) path '$.skuName',
            unit varchar2(100) path '$.unit',
            time_usage_started date path '$.timeUsageStarted',
            time_usage_ended date path '$.timeUsageEnded',
            quantity number path '$.computedQuantity'
        )     
     )
)
select
    acct_name,
    month_end_date,
    usage_month,
    compartment,
    region,
    sku,
    resource_id,
    unit,    
    time_usage_ended,
    ocpu_hours,
    tb
from all_metrics
where ocpu_hours is not null
or tb is not null;



create or replace procedure write
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

create or replace procedure get_usage (
    end_date in date default sysdate -- the last day to collect data for. Defaults to now.
)

as 
    begin_date      date;
    cred            varchar2(50) := 'OCI_ADWC4PM';
    endpoint        varchar2(100);
    err_msg         varchar2(4000);
    granularity     varchar2(20) := 'MONTHLY';       
    num_del_recs    int;
    region          varchar2(100):= 'us-ashburn-1';
    request_body    blob;    
    response_rec    DBMS_CLOUD_TYPES.resp;
    start_time      date;
    status_code     integer;
    tenant          varchar2(100) := 'adwc4pm';
    tenant_ocid     varchar2(200);
    usage_not_null  clob default null;
    usage_all       clob default null;

begin

    -- Timing
    start_time := sysdate;
        
    write('Begin',1);
    write('processing day: ' || to_char(end_date, 'YYYY-MON-DD'), 2);
    
    -- Remove records that have a partial month
    -- The last day for a month is the first day of the following month
    -- i.e. month_end_date = 2022-09-01 will contain data for all of august    
    select count(*)
    into num_del_recs
    from oci_monthly_usage_raw
    where (to_char(month_end_date, 'YYYY-MM') = to_char(end_date-1, 'YYYY-MM')
      and to_char(trunc(month_end_date), 'DD') != '01')  -- don't drop the first day of the month
      or  to_char(month_end_date, 'YYYY-MON-DD') = to_char(end_date, 'YYYY-MON-DD'); --processing the same day again;
    
    
    if num_del_recs > 0 then
        write('deleting rows for end date ' || to_char(end_date, 'YYYY-MM'), 2);
        
        delete oci_monthly_usage_raw
        where (to_char(month_end_date, 'YYYY-MM') = to_char(end_date-1, 'YYYY-MM')
          and to_char(trunc(month_end_date), 'DD') != '01')
          or  to_char(month_end_date, 'YYYY-MON-DD') = to_char(end_date, 'YYYY-MON-DD');
        
        commit;
        
        write('# rows deleted : ' || num_del_recs, 3);

    end if;    
    
    -- Usage API will be able to retrieve all data across the tenancy
    -- Loop over each tenancy and retrieve the usage stats
    
    for rec_oci_tenant in (
        select acct_name, ocid             
        from oci_tenancies 
        )
    loop
        -- Save off the tenancy ocid
        tenant_ocid := rec_oci_tenant.ocid;
        tenant      := rec_oci_tenant.acct_name;        
        cred        := 'OCI_' || upper(tenant);
        
        endpoint     := 'https://usageapi.' || region || '.oci.oraclecloud.com/20200107/usage';        
        write('tenancy    :  ' || tenant, 2);
        write('region     :  ' || region, 3);
        write('endpoint   :  ' || endpoint, 3);
        write('credential :  ' || cred, 3);
        write('granularity:  ' || granularity, 3);     
        write('end date   :  ' || to_char(end_date, 'YYYY-MM-DD'), 3);

        -- Begin date is always the first day of the given month
        -- Note, it must be the day before
        -- If end_date is 2022-SEP-01, then it is collecting data from the entire previous month
        -- So, the begin_date must be 2022-AUG-01
        begin_date := trunc(end_date-1, 'MM');
        
        
        write('begin date :  ' || to_char(begin_date, 'YYYY-MM-DD'), 3);     
        
        -- Check if the data is already up to date. We're collecting up to yesterday, so
        -- if the latest data contains yesterday, then we're  up to date.
        -- Make the request                
        request_body := utl_raw.cast_to_raw(
            '{
              "granularity": "' || granularity || '",
              "groupBy": [
                "resourceId",
                "skuName",
                "region",
                "compartmentName"
              ],
              "compartmentDepth":4,
              "queryType": "USAGE",
              "tenantId": "' || tenant_ocid || '",
              "timeUsageEnded": "' || to_char(end_date, 'YYYY-MM-DD') || 'T00:00:00.000Z",
              "timeUsageStarted": "' || to_char(begin_date, 'YYYY-MM-DD') || 'T00:00:00.000Z"
            }');
            
        response_rec := 
            DBMS_CLOUD.send_request (
                credential_name => cred,
                uri => endpoint,
                method => DBMS_CLOUD.METHOD_POST,
                headers => JSON_OBJECT('opc-request-id' value 'list-usage'),
                body => request_body,
                cache   => true
              );
        
        status_code := dbms_cloud.get_response_status_code(response_rec);      
        write('response code:  ' || status_code, 3);          
                                   
        -- Check the response code
        if status_code = 200 then            
            -- Save the response - or usage information
            usage_all := DBMS_CLOUD.get_response_text(response_rec);
            
            select json_transform(usage_all, remove '$.items[*].*?(@ == null)') 
            into usage_not_null
            from dual;
               
            insert into oci_monthly_usage_raw (acct_name, oci_usage, month_end_date)
            values (tenant, usage_not_null, end_date);
                
            commit;
        else
            write('invalid response code.', -1);
            
        end if;
        
            
        write('Completed tenancy    :  ' || tenant, 2);
    
    end loop;
    
    write('Complete. Elapsed(s): ' || round((sysdate - start_time) * 24 * 60 * 60, 0), 1);
    

    
exception
when others then    
    err_msg := substr(sqlerrm, 1, 3800);
    write(err_msg, -1);
    
end get_usage;
/