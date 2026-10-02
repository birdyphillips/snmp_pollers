# Latency Management Upstream — Table Verification Queries

Run these against `latency_management` (`24.179.103.29:5432`) as `aarch_user` after the poller
has completed at least one poll cycle.

---

## Quick Row Count — All Tables at Once

```sql
SELECT 'lm_cm_devices'                                  AS table_name, COUNT(*) FROM lm_cm_devices
UNION ALL
SELECT 'lm_docs_qos_service_flow_metrics',               COUNT(*) FROM lm_docs_qos_service_flow_metrics
UNION ALL
SELECT 'lm_docs_qos_service_flow_pkts',                  COUNT(*) FROM lm_docs_qos_service_flow_pkts
UNION ALL
SELECT 'lm_docs_qos_service_flow_octets',                COUNT(*) FROM lm_docs_qos_service_flow_octets
UNION ALL
SELECT 'lm_docs_qos_params',                             COUNT(*) FROM lm_docs_qos_params
UNION ALL
SELECT 'lm_docs_qos_service_flow_aqm_dropped_pkts',      COUNT(*) FROM lm_docs_qos_service_flow_aqm_dropped_pkts
UNION ALL
SELECT 'lm_docs_qos_sf_congestion_sanctioned_pkts',      COUNT(*) FROM lm_docs_qos_sf_congestion_sanctioned_pkts
UNION ALL
SELECT 'lm_docs_qos_sf_latency_max_latency',             COUNT(*) FROM lm_docs_qos_sf_latency_max_latency
UNION ALL
SELECT 'lm_docs_qos_sf_latency_num_hist_updates',        COUNT(*) FROM lm_docs_qos_sf_latency_num_hist_updates
UNION ALL
SELECT 'lm_docs_qos_sf_latency_bin_pkts',                COUNT(*) FROM lm_docs_qos_sf_latency_bin_pkts
UNION ALL
SELECT 'lm_docs_qos_sf_latency_histogram',               COUNT(*) FROM lm_docs_qos_sf_latency_histogram
UNION ALL
SELECT 'lm_docs_qos_service_flow_sfid',                  COUNT(*) FROM lm_docs_qos_service_flow_sfid
UNION ALL
SELECT 'lm_docs_qos_service_flow_sfid_delta',            COUNT(*) FROM lm_docs_qos_service_flow_sfid_delta
UNION ALL
SELECT 'lm_docs_qos_service_flow_pkts_delta',            COUNT(*) FROM lm_docs_qos_service_flow_pkts_delta
UNION ALL
SELECT 'lm_docs_qos_service_flow_octets_delta',          COUNT(*) FROM lm_docs_qos_service_flow_octets_delta
UNION ALL
SELECT 'lm_docs_qos_service_flow_dropped_delta',         COUNT(*) FROM lm_docs_qos_service_flow_dropped_delta
UNION ALL
SELECT 'lm_docs_qos_service_flow_time_created',          COUNT(*) FROM lm_docs_qos_service_flow_time_created
UNION ALL
SELECT 'lm_docs_qos_sf_congestion_total_ect0_pkts',      COUNT(*) FROM lm_docs_qos_sf_congestion_total_ect0_pkts
UNION ALL
SELECT 'lm_docs_qos_sf_congestion_total_ect1_pkts',      COUNT(*) FROM lm_docs_qos_sf_congestion_total_ect1_pkts
UNION ALL
SELECT 'lm_docs_qos_sf_congestion_ce_marked_ect1_pkts',  COUNT(*) FROM lm_docs_qos_sf_congestion_ce_marked_ect1_pkts
UNION ALL
SELECT 'lm_docs_qos_sf_congestion_arrived_ce_pkts',      COUNT(*) FROM lm_docs_qos_sf_congestion_arrived_ce_pkts
UNION ALL
SELECT 'lm_docs_qos_sf_latency_bin_edge',                COUNT(*) FROM lm_docs_qos_sf_latency_bin_edge
UNION ALL
SELECT 'lm_qos_aggregate_service_flow_map',              COUNT(*) FROM lm_qos_aggregate_service_flow_map
UNION ALL
SELECT 'lm_docs_dev_event_table',                        COUNT(*) FROM lm_docs_dev_event_table
ORDER BY table_name;
```

---

## Most Recent Poll Timestamp

```sql
SELECT MAX(timestamp) AS last_poll FROM lm_docs_qos_service_flow_metrics;
```

---

## Modems Seen (last 5 minutes)

```sql
SELECT DISTINCT cm_mac_addr
FROM lm_docs_qos_service_flow_metrics
WHERE timestamp >= NOW() - INTERVAL '5 minutes'
ORDER BY cm_mac_addr;
```

---

## Sample Rows Per Table

```sql
-- CM devices
SELECT * FROM lm_cm_devices ORDER BY last_seen DESC LIMIT 5;

-- Wide upstream metrics
SELECT timestamp, cm_mac_addr, sf_index, service_class_name, total_pkts, latency_max_usec
FROM lm_docs_qos_service_flow_metrics
ORDER BY timestamp DESC LIMIT 5;

-- Histogram bins
SELECT timestamp, cm_mac_addr, sf_index, bin1_pkts, bin2_pkts, bin3_pkts
FROM lm_docs_qos_sf_latency_histogram
ORDER BY timestamp DESC LIMIT 5;

-- Delta packets
SELECT timestamp, cm_mac_addr, sf_index, pkts, delta
FROM lm_docs_qos_service_flow_pkts_delta
ORDER BY timestamp DESC LIMIT 5;

-- Delta octets
SELECT timestamp, cm_mac_addr, sf_index, octets, delta
FROM lm_docs_qos_service_flow_octets_delta
ORDER BY timestamp DESC LIMIT 5;

-- Delta drops
SELECT timestamp, cm_mac_addr, sf_index, aqm_dropped_pkts, policed_drop_pkts, delta_aqm, delta_policed
FROM lm_docs_qos_service_flow_dropped_delta
ORDER BY timestamp DESC LIMIT 5;

-- Latency max
SELECT timestamp, cm_mac_addr, sf_index, latency_max_usec
FROM lm_docs_qos_sf_latency_max_latency
ORDER BY timestamp DESC LIMIT 5;

-- Congestion ECT0
SELECT timestamp, cm_mac_addr, sf_index, total_ect0_pkts
FROM lm_docs_qos_sf_congestion_total_ect0_pkts
ORDER BY timestamp DESC LIMIT 5;

-- Aggregate SF map
SELECT timestamp, cm_mac_addr, sf_index, aggregate_sf_id
FROM lm_qos_aggregate_service_flow_map
ORDER BY timestamp DESC LIMIT 5;

-- DOCSIS events
SELECT timestamp, cm_mac_addr, event_id, level, event_text
FROM lm_docs_dev_event_table
ORDER BY timestamp DESC LIMIT 5;
```

---

## Verify Source Modem List (ipdr_data_for_histograms)

Run against `ipdr_data_for_histograms` as `db_admin`:

```sql
-- Total enabled modems
SELECT COUNT(DISTINCT mac_address) AS unique_modems FROM public.ds_histograms_enabled_modems;

-- Sample
SELECT DISTINCT mac_address FROM public.ds_histograms_enabled_modems LIMIT 10;
```
