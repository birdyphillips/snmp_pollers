-- =============================================================================
-- Latency_Management Upstream Poller — Table Creation
-- Database : latency_management  (24.179.103.29)
-- DDL user : db_admin / lldaateam
-- Run user : aarch_user / arch123
--
-- Usage:
--   CREATE DATABASE latency_management;
--   \c latency_management
--   CREATE EXTENSION IF NOT EXISTS timescaledb;
--   \i create_tables.sql
--
-- NOTE: If recreating lm_docs_qos_service_flow_metrics, always use
-- DROP TABLE lm_docs_qos_service_flow_metrics CASCADE;
--   Plain DROP TABLE leaves orphaned TimescaleDB chunks with the old schema,
--   causing InvalidTextRepresentation errors on INSERT.
-- =============================================================================

-- CM device registry
CREATE TABLE IF NOT EXISTS lm_cm_devices (
    cm_mac_addr TEXT PRIMARY KEY,
    last_seen   TIMESTAMPTZ
);

-- Wide upstream service flow metrics (one row per poll per SF)
CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_metrics (
    timestamp                    TIMESTAMPTZ NOT NULL,
    cm_mac_addr                  TEXT        NOT NULL,
    sf_index                     TEXT        NOT NULL,
    service_class_name           TEXT,
    max_traffic_rate             BIGINT,
    max_traffic_burst            BIGINT,
    aqm_latency_target           BIGINT,
    minimum_buffer               BIGINT,
    target_buffer                BIGINT,
    maximum_buffer               BIGINT,
    buffer_size                  BIGINT,
    total_pkts                   BIGINT,
    total_octets                 BIGINT,
    policed_drop_pkts            BIGINT,
    policed_delay_pkts           BIGINT,
    aqm_dropped_pkts             BIGINT,
    congestion_aqm_dropped_pkts  BIGINT,
    congestion_sanctioned_pkts   BIGINT,
    latency_max_usec             BIGINT,
    latency_num_hist_updates     BIGINT,
    latency_bin1_pkts  BIGINT, latency_bin2_pkts  BIGINT,
    latency_bin3_pkts  BIGINT, latency_bin4_pkts  BIGINT,
    latency_bin5_pkts  BIGINT, latency_bin6_pkts  BIGINT,
    latency_bin7_pkts  BIGINT, latency_bin8_pkts  BIGINT,
    latency_bin9_pkts  BIGINT, latency_bin10_pkts BIGINT,
    latency_bin11_pkts BIGINT, latency_bin12_pkts BIGINT,
    latency_bin13_pkts BIGINT, latency_bin14_pkts BIGINT,
    latency_bin15_pkts BIGINT, latency_bin16_pkts BIGINT
);
SELECT create_hypertable('lm_docs_qos_service_flow_metrics', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_pkts (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    pkts        BIGINT
);
SELECT create_hypertable('lm_docs_qos_service_flow_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_octets (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    octets      BIGINT
);
SELECT create_hypertable('lm_docs_qos_service_flow_octets', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_params (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    param_name  TEXT,
    param_value TEXT
);
SELECT create_hypertable('lm_docs_qos_params', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_aqm_dropped_pkts (
    timestamp        TIMESTAMPTZ NOT NULL,
    cm_mac_addr      TEXT        NOT NULL,
    sf_index         TEXT        NOT NULL,
    aqm_dropped_pkts BIGINT
);
SELECT create_hypertable('lm_docs_qos_service_flow_aqm_dropped_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_congestion_sanctioned_pkts (
    timestamp        TIMESTAMPTZ NOT NULL,
    cm_mac_addr      TEXT        NOT NULL,
    sf_index         TEXT        NOT NULL,
    sanctioned_pkts  BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_congestion_sanctioned_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_latency_max_latency (
    timestamp        TIMESTAMPTZ NOT NULL,
    cm_mac_addr      TEXT        NOT NULL,
    sf_index         TEXT        NOT NULL,
    latency_max_usec BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_latency_max_latency', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_latency_num_hist_updates (
    timestamp         TIMESTAMPTZ NOT NULL,
    cm_mac_addr       TEXT        NOT NULL,
    sf_index          TEXT        NOT NULL,
    num_hist_updates  BIGINT,
    buffer_size       BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_latency_num_hist_updates', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_latency_bin_pkts (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    bin_number  INT,
    pkt_count   BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_latency_bin_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_latency_histogram (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    bin1_pkts   BIGINT, bin2_pkts  BIGINT, bin3_pkts  BIGINT, bin4_pkts  BIGINT,
    bin5_pkts   BIGINT, bin6_pkts  BIGINT, bin7_pkts  BIGINT, bin8_pkts  BIGINT,
    bin9_pkts   BIGINT, bin10_pkts BIGINT, bin11_pkts BIGINT, bin12_pkts BIGINT,
    bin13_pkts  BIGINT, bin14_pkts BIGINT, bin15_pkts BIGINT, bin16_pkts BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_latency_histogram', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_sfid (
    timestamp          TIMESTAMPTZ NOT NULL,
    cm_mac_addr        TEXT        NOT NULL,
    sf_index           TEXT        NOT NULL,
    service_class_name TEXT
);
SELECT create_hypertable('lm_docs_qos_service_flow_sfid', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_sfid_delta (
    timestamp          TIMESTAMPTZ NOT NULL,
    cm_mac_addr        TEXT        NOT NULL,
    sf_index           TEXT        NOT NULL,
    service_class_name TEXT
);
SELECT create_hypertable('lm_docs_qos_service_flow_sfid_delta', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_pkts_delta (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    pkts        BIGINT,
    raw_prev    BIGINT,
    raw_curr    BIGINT,
    delta       BIGINT
);
SELECT create_hypertable('lm_docs_qos_service_flow_pkts_delta', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_octets_delta (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    octets      BIGINT,
    raw_prev    BIGINT,
    raw_curr    BIGINT,
    delta       BIGINT
);
SELECT create_hypertable('lm_docs_qos_service_flow_octets_delta', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_dropped_delta (
    timestamp         TIMESTAMPTZ NOT NULL,
    cm_mac_addr       TEXT        NOT NULL,
    sf_index          TEXT        NOT NULL,
    aqm_dropped_pkts  BIGINT,
    policed_drop_pkts BIGINT,
    raw_prev_aqm      BIGINT,
    raw_curr_aqm      BIGINT,
    raw_prev_policed  BIGINT,
    raw_curr_policed  BIGINT,
    delta_aqm         BIGINT,
    delta_policed     BIGINT
);
SELECT create_hypertable('lm_docs_qos_service_flow_dropped_delta', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_service_flow_time_created (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    sf_index    TEXT        NOT NULL,
    created_at  TIMESTAMPTZ
);
SELECT create_hypertable('lm_docs_qos_service_flow_time_created', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_congestion_total_ect0_pkts (
    timestamp       TIMESTAMPTZ NOT NULL,
    cm_mac_addr     TEXT        NOT NULL,
    sf_index        TEXT        NOT NULL,
    total_ect0_pkts BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_congestion_total_ect0_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_congestion_total_ect1_pkts (
    timestamp       TIMESTAMPTZ NOT NULL,
    cm_mac_addr     TEXT        NOT NULL,
    sf_index        TEXT        NOT NULL,
    total_ect1_pkts BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_congestion_total_ect1_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_congestion_ce_marked_ect1_pkts (
    timestamp           TIMESTAMPTZ NOT NULL,
    cm_mac_addr         TEXT        NOT NULL,
    sf_index            TEXT        NOT NULL,
    ce_marked_ect1_pkts BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_congestion_ce_marked_ect1_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_congestion_arrived_ce_pkts (
    timestamp       TIMESTAMPTZ NOT NULL,
    cm_mac_addr     TEXT        NOT NULL,
    sf_index        TEXT        NOT NULL,
    arrived_ce_pkts BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_congestion_arrived_ce_pkts', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_docs_qos_sf_latency_bin_edge (
    timestamp        TIMESTAMPTZ NOT NULL,
    cm_mac_addr      TEXT        NOT NULL,
    sf_index         TEXT        NOT NULL,
    latency_bin_edge BIGINT
);
SELECT create_hypertable('lm_docs_qos_sf_latency_bin_edge', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS lm_qos_aggregate_service_flow_map (
    timestamp       TIMESTAMPTZ NOT NULL,
    cm_mac_addr     TEXT        NOT NULL,
    sf_index        TEXT        NOT NULL,
    aggregate_sf_id BIGINT
);
SELECT create_hypertable('lm_qos_aggregate_service_flow_map', 'timestamp', if_not_exists => TRUE);

-- docsDevEventTable (1.3.6.1.2.1.69.1.5.8)
CREATE TABLE IF NOT EXISTS lm_docs_dev_event_table (
    timestamp   TIMESTAMPTZ NOT NULL,
    cm_mac_addr TEXT        NOT NULL,
    event_index TEXT        NOT NULL,
    first_time  TEXT,
    last_time   TEXT,
    counts      BIGINT,
    level       INT,
    event_id    BIGINT,
    event_text  TEXT
);
SELECT create_hypertable('lm_docs_dev_event_table', 'timestamp', if_not_exists => TRUE);

-- Grant permissions to poller runtime user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO aarch_user;
