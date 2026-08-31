-- Kafka metric tables for IoTDB 2.x table dialect.
CREATE DATABASE IF NOT EXISTS eventmesh_dashboard WITH (TTL=31536000000);

USE eventmesh_dashboard;

CREATE TABLE IF NOT EXISTS kafka_cluster_metrics (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    cluster_name STRING ATTRIBUTE,
    health_state INT64 FIELD,
    health_check_passed INT64 FIELD,
    health_check_total INT64 FIELD,
    health_state_topics INT64 FIELD,
    health_check_passed_topics INT64 FIELD,
    health_check_total_topics INT64 FIELD,
    health_state_brokers INT64 FIELD,
    health_check_passed_brokers INT64 FIELD,
    health_check_total_brokers INT64 FIELD,
    health_state_groups INT64 FIELD,
    health_check_passed_groups INT64 FIELD,
    health_check_total_groups INT64 FIELD,
    health_state_cluster INT64 FIELD,
    health_check_passed_cluster INT64 FIELD,
    health_check_total_cluster INT64 FIELD,
    total_request_queue_size INT64 FIELD,
    total_response_queue_size INT64 FIELD,
    event_queue_size INT64 FIELD,
    active_controller_count INT64 FIELD,
    total_produce_requests DOUBLE FIELD,
    connections_count INT64 FIELD,
    partition_no_leader INT64 FIELD,
    partition_min_isr_s INT64 FIELD,
    partition_min_isr_e INT64 FIELD,
    partition_urp INT64 FIELD,
    messages_in DOUBLE FIELD,
    leader_messages INT64 FIELD,
    total_log_size INT64 FIELD,
    bytes_in DOUBLE FIELD,
    bytes_in_min_5 DOUBLE FIELD,
    bytes_in_min_15 DOUBLE FIELD,
    bytes_out DOUBLE FIELD,
    bytes_out_min_5 DOUBLE FIELD,
    bytes_out_min_15 DOUBLE FIELD,
    group_actives INT64 FIELD,
    group_emptys INT64 FIELD,
    group_rebalances INT64 FIELD,
    group_deads INT64 FIELD,
    alive INT64 FIELD,
    load_re_balance_enable INT64 FIELD,
    load_re_balance_nw_in INT64 FIELD,
    load_re_balance_nw_out INT64 FIELD,
    load_re_balance_disk INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_broker_metrics (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    broker_id STRING TAG,
    cluster_name STRING ATTRIBUTE,
    health_state INT64 FIELD,
    health_check_passed INT64 FIELD,
    health_check_total INT64 FIELD,
    total_request_queue_size INT64 FIELD,
    total_response_queue_size INT64 FIELD,
    messages_in DOUBLE FIELD,
    total_produce_requests DOUBLE FIELD,
    network_processor_avg_idle DOUBLE FIELD,
    request_handler_avg_idle DOUBLE FIELD,
    connections_count INT64 FIELD,
    partition_urp INT64 FIELD,
    partition_min_isr_s INT64 FIELD,
    partition_min_isr_e INT64 FIELD,
    partitions INT64 FIELD,
    partitions_skew DOUBLE FIELD,
    leaders INT64 FIELD,
    leaders_skew DOUBLE FIELD,
    active_controller_count INT64 FIELD,
    event_queue_size INT64 FIELD,
    bytes_in DOUBLE FIELD,
    bytes_in_min_5 DOUBLE FIELD,
    bytes_in_min_15 DOUBLE FIELD,
    bytes_out DOUBLE FIELD,
    bytes_out_min_5 DOUBLE FIELD,
    bytes_out_min_15 DOUBLE FIELD,
    replication_bytes_in DOUBLE FIELD,
    replication_bytes_out DOUBLE FIELD,
    reassignment_bytes_in DOUBLE FIELD,
    reassignment_bytes_out DOUBLE FIELD,
    log_size INT64 FIELD,
    alive INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_topic_metrics (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    topic STRING TAG,
    cluster_name STRING ATTRIBUTE,
    health_state INT64 FIELD,
    health_check_passed INT64 FIELD,
    health_check_total INT64 FIELD,
    total_produce_requests DOUBLE FIELD,
    bytes_rejected DOUBLE FIELD,
    failed_fetch_requests DOUBLE FIELD,
    failed_produce_requests DOUBLE FIELD,
    messages INT64 FIELD,
    messages_in DOUBLE FIELD,
    bytes_in DOUBLE FIELD,
    bytes_in_min_5 DOUBLE FIELD,
    bytes_in_min_15 DOUBLE FIELD,
    bytes_out DOUBLE FIELD,
    bytes_out_min_5 DOUBLE FIELD,
    bytes_out_min_15 DOUBLE FIELD,
    log_size INT64 FIELD,
    partition_urp INT64 FIELD,
    mirror_fetch_lag INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_partition_metrics (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    topic STRING TAG,
    partition_id STRING TAG,
    cluster_name STRING ATTRIBUTE,
    log_start_offset INT64 FIELD,
    log_end_offset INT64 FIELD,
    messages INT64 FIELD,
    bytes_in DOUBLE FIELD,
    bytes_out DOUBLE FIELD,
    log_size INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_group_metrics (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    group_id STRING TAG,
    cluster_name STRING ATTRIBUTE,
    health_state INT64 FIELD,
    health_check_passed INT64 FIELD,
    health_check_total INT64 FIELD,
    state INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_group_partition_metrics (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    group_id STRING TAG,
    topic STRING TAG,
    partition_id STRING TAG,
    cluster_name STRING ATTRIBUTE,
    offset_consumed INT64 FIELD,
    log_end_offset INT64 FIELD,
    lag INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_replica_metrics (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    broker_id STRING TAG,
    topic STRING TAG,
    partition_id STRING TAG,
    cluster_name STRING ATTRIBUTE,
    log_start_offset INT64 FIELD,
    log_end_offset INT64 FIELD,
    messages INT64 FIELD,
    log_size INT64 FIELD,
    in_sync INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_collection_runs (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    cluster_name STRING ATTRIBUTE,
    status STRING FIELD,
    sample_count INT64 FIELD,
    failure_count INT64 FIELD,
    duration_ms INT64 FIELD
);

CREATE TABLE IF NOT EXISTS kafka_collection_failures (
    time TIMESTAMP TIME,
    organization_id STRING TAG,
    "cluster_id" STRING TAG,
    failure_id STRING TAG,
    cluster_name STRING ATTRIBUTE,
    metric_name STRING FIELD,
    object_name STRING FIELD,
    reason STRING FIELD
);
