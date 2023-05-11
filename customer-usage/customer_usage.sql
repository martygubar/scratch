create table customer_usage as
select
    collection_ts as collection_time,
    fk_region_key as region,
    tenant_id,
    tnt_ocid,
    pdb_name,
    pdb_service_type as service,
    lifecycle_state,
    pdb_cpu_count_used as max_available_cpu_count,
    PDB_CURRENT_DB_CPU as used_cpu_count,
    pdb_rsrc_cpu_count as base_cpu_count,
    pdb_current_db_cpu_utilization cpu_used_percent,
    pdb_max_size_bytes as max_size_bytes,
    base_storage_tb as storage_allocated_terabytes,
    pdb_current_db_storage_utilization as storage_used_percentage,
    pdb_current_size_bytes/1000000000 as storage_used_terabytes,
    autoscalable as is_autoscalable,
    autoscalable_storage,
    autoscaled_cpu_count,
    autoscaled_cpu_updated,
    free_tier as is_free_tier,
    disabled as is_database_disabled,
    pdb_last_activity as last_activity,
    created as date_created,
    deleted as date_deleted,
    closed as date_closed
from
    adbprod.fact_database_granular
where internal = 'F'
  and to_char(collection_ts, 'YYYY-MON') in ('2022-OCT')
  ;