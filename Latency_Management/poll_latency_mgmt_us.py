"""
Latency Management Upstream Poller
====================================
Polls cable modems via Elements API SNMP walk every 60s.
Modem list is pulled live from latency_on_vcmts.public.ds_histograms_enabled_modems.
Results are written to the output DB configured in .env (DB_HOST / DB_NAME).

Usage:
    python poll_latency_mgmt_us.py
    python poll_latency_mgmt_us.py 08a7c0885eff   # single modem override
"""
import sys
import os
import time
from datetime import datetime, timezone
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor

import logging
import logging.handlers

import requests
import psycopg2
import psycopg2.extras
import psycopg2.pool
from dotenv import load_dotenv
import urllib3
import requests.packages.urllib3.util.connection as urllib3_cn
urllib3_cn.HAS_IPV6 = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'poll_lm.log')
    fmt = '%(asctime)s [%(levelname)s] %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    logger = logging.getLogger()
    if logger.handlers:
        return
    handler_file = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=5
    )
    handler_file.setFormatter(logging.Formatter(fmt, datefmt))
    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setFormatter(logging.Formatter(fmt, datefmt))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler_file)
    logger.addHandler(handler_console)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.getenv('ELEMENTS_API_URL', 'https://api.elements.charter.com')
API_USER = os.getenv('ELEMENTS_API_USER')
API_PASS = os.getenv('ELEMENTS_API_PASS')

PG_HOST = os.getenv('DB_HOST')
PG_PORT = int(os.getenv('DB_PORT', '5432'))
PG_USER = os.getenv('DB_USER')
PG_PASS = os.getenv('DB_PASS')
PG_DB   = os.getenv('DB_NAME')

# Source DB — MACs come from here
SRC_HOST = os.getenv('DB_HOST_MANAGED')
SRC_PORT = int(os.getenv('DB_PORT_MANAGED', '5432'))
SRC_DB   = os.getenv('DB_NAME_MANAGED')
SRC_USER = os.getenv('DB_USER_MANAGED')
SRC_PASS = os.getenv('DB_PASS_MANAGED')

POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '300'))
API_TIMEOUT   = int(os.getenv('API_TIMEOUT', '25'))
CONCURRENCY   = int(os.getenv('CONCURRENCY', '10'))

# ---------------------------------------------------------------------------
# OIDs
# ---------------------------------------------------------------------------

AQM_OIDS = [
    {"oid_name": "docsQosSfLatencyMaxLatency",              "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.1"},
    {"oid_name": "docsQosSfLatencyNumHistUpdates",          "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.2"},
    {"oid_name": "docsQosSfLatencyBin1Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.3"},
    {"oid_name": "docsQosSfLatencyBin2Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.4"},
    {"oid_name": "docsQosSfLatencyBin3Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.5"},
    {"oid_name": "docsQosSfLatencyBin4Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.6"},
    {"oid_name": "docsQosSfLatencyBin5Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.7"},
    {"oid_name": "docsQosSfLatencyBin6Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.8"},
    {"oid_name": "docsQosSfLatencyBin7Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.9"},
    {"oid_name": "docsQosSfLatencyBin8Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.10"},
    {"oid_name": "docsQosSfLatencyBin9Pkts",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.11"},
    {"oid_name": "docsQosSfLatencyBin10Pkts",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.12"},
    {"oid_name": "docsQosSfLatencyBin11Pkts",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.13"},
    {"oid_name": "docsQosSfLatencyBin12Pkts",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.14"},
    {"oid_name": "docsQosSfLatencyBin13Pkts",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.15"},
    {"oid_name": "docsQosSfLatencyBin14Pkts",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.16"},
    {"oid_name": "docsQosSfLatencyBin15Pkts",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.17"},
    {"oid_name": "docsQosSfLatencyBin16Pkts",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.18"},
    {"oid_name": "docsQosServiceFlowPkts",                  "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.1"},
    {"oid_name": "docsQosServiceFlowOctets",                "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.2"},
    {"oid_name": "docsQosServiceFlowPolicedDropPkts",       "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.6"},
    {"oid_name": "docsQosServiceFlowPolicedDelayPkts",      "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.7"},
    {"oid_name": "docsQosServiceFlowAQMDroppedPkts",        "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.8"},
    {"oid_name": "docsQosServiceFlowBufferSize",            "oid": "1.3.6.1.4.1.4491.2.1.21.1.3.1.17"},
    {"oid_name": "docsQosSfCongestionSanctionedPkts",       "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.1"},
    {"oid_name": "docsQosSfCongestionTotalEct0Pkts",        "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.2"},
    {"oid_name": "docsQosSfCongestionTotalEct1Pkts",        "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.3"},
    {"oid_name": "docsQosSfCongestionCeMarkedEct1Pkts",     "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.4"},
    {"oid_name": "docsQosSfCongestionArrivedCePkts",        "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.5"},
    {"oid_name": "docsQosParamSetMaxTrafficRate",           "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.6.2.1"},
    {"oid_name": "docsQosParamSetMaxTrafficBurst",          "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.7.2.1"},
    {"oid_name": "docsQosParamSetMaxConcatBurst",           "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.8.2.1"},
    {"oid_name": "docsQosParamSetAqmLatencyTarget",         "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.43.2.1"},
    {"oid_name": "docsQosParamSetMinimumBuffer",            "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.39.2.1"},
    {"oid_name": "docsQosParamSetTargetBuffer",             "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.40.2.1"},
    {"oid_name": "docsQosParamSetMaximumBuffer",            "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.41.2.1"},
    {"oid_name": "docsQosParamSetServiceClassName",         "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.4.2.2"},
    {"oid_name": "docsQosParamSetServiceClassNameActive",   "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.4.1.1"},
    {"oid_name": "docsQosParamSetServiceClassNameAdmitted", "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.4.3.1"},
    {"oid_name": "docsQosParamSetPriority",                 "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.5.2.1"},
    {"oid_name": "docsQosSfLatencyBinEdge",                 "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.1.1.2"},
    {"oid_name": "docsQosServiceFlowDirection",             "oid": "1.3.6.1.4.1.4491.2.1.21.1.3.1.2"},
    {"oid_name": "docsQosServiceFlowPrimary",               "oid": "1.3.6.1.4.1.4491.2.1.21.1.3.1.3"},
    {"oid_name": "docsQosServiceFlowSID",                   "oid": "1.3.6.1.4.1.4491.2.1.21.1.3.1.5"},
    {"oid_name": "docsQosServiceFlowTimeCreated",           "oid": "1.3.6.1.4.1.4491.2.1.21.1.3.1.4"},
    {"oid_name": "docsQosServiceClassName",                 "oid": "1.3.6.1.4.1.4491.2.1.21.1.8.1.2"},
    {"oid_name": "qos_aggregate_service_flow_id",           "oid": "1.3.6.1.4.1.4491.2.1.21.1.3.1.19"},
]

# docsDevEventTable — DOCS event log (1.3.6.1.2.1.69.1.5.8)
DOCS_EVENT_OIDS = [
    {"oid_name": "docsDevEvFirstTime",  "oid": "1.3.6.1.2.1.69.1.5.8.1.2"},
    {"oid_name": "docsDevEvLastTime",   "oid": "1.3.6.1.2.1.69.1.5.8.1.3"},
    {"oid_name": "docsDevEvCounts",     "oid": "1.3.6.1.2.1.69.1.5.8.1.4"},
    {"oid_name": "docsDevEvLevel",      "oid": "1.3.6.1.2.1.69.1.5.8.1.5"},
    {"oid_name": "docsDevEvId",         "oid": "1.3.6.1.2.1.69.1.5.8.1.6"},
    {"oid_name": "docsDevEvText",       "oid": "1.3.6.1.2.1.69.1.5.8.1.7"},
]

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_US_COLS = [
    'timestamp', 'cm_mac_addr', 'sf_index', 'service_class_name',
    'max_traffic_rate', 'max_traffic_burst', 'aqm_latency_target',
    'minimum_buffer', 'target_buffer', 'maximum_buffer', 'buffer_size',
    'total_pkts', 'total_octets', 'policed_drop_pkts', 'policed_delay_pkts',
    'aqm_dropped_pkts', 'congestion_aqm_dropped_pkts', 'congestion_sanctioned_pkts',
    'latency_max_usec', 'latency_num_hist_updates',
    *[f'latency_bin{i}_pkts' for i in range(1, 17)]
]
_US_SQL = (
    f"INSERT INTO lm_docs_qos_service_flow_metrics ({', '.join(_US_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_US_COLS))})"
)
_CM_SQL = """
    INSERT INTO lm_cm_devices (cm_mac_addr, last_seen)
    VALUES (%s, %s)
    ON CONFLICT (cm_mac_addr) DO UPDATE SET last_seen = EXCLUDED.last_seen
"""
_SQL_QOS_SF_PKTS    = "INSERT INTO lm_docs_qos_service_flow_pkts (timestamp, cm_mac_addr, sf_index, pkts) VALUES (%s, %s, %s, %s)"
_SQL_QOS_SF_OCTETS  = "INSERT INTO lm_docs_qos_service_flow_octets (timestamp, cm_mac_addr, sf_index, octets) VALUES (%s, %s, %s, %s)"
_SQL_DOCS_QOS_PARAMS= "INSERT INTO lm_docs_qos_params (timestamp, cm_mac_addr, sf_index, param_name, param_value) VALUES (%s, %s, %s, %s, %s)"
_SQL_DP_AQM_DROPPED = "INSERT INTO lm_docs_qos_service_flow_aqm_dropped_pkts (timestamp, cm_mac_addr, sf_index, aqm_dropped_pkts) VALUES (%s, %s, %s, %s)"
_SQL_DP_SANCTIONED  = "INSERT INTO lm_docs_qos_sf_congestion_sanctioned_pkts (timestamp, cm_mac_addr, sf_index, sanctioned_pkts) VALUES (%s, %s, %s, %s)"
_SQL_DP_LAT_MAX     = "INSERT INTO lm_docs_qos_sf_latency_max_latency (timestamp, cm_mac_addr, sf_index, latency_max_usec) VALUES (%s, %s, %s, %s)"
_SQL_DP_LAT_AVG     = "INSERT INTO lm_docs_qos_sf_latency_num_hist_updates (timestamp, cm_mac_addr, sf_index, num_hist_updates, buffer_size) VALUES (%s, %s, %s, %s, %s)"
_SQL_DP_LAT_BIN     = "INSERT INTO lm_docs_qos_sf_latency_bin_pkts (timestamp, cm_mac_addr, sf_index, bin_number, pkt_count) VALUES (%s, %s, %s, %s, %s)"
_SQL_HISTOGRAM      = (
    "INSERT INTO lm_docs_qos_sf_latency_histogram (timestamp, cm_mac_addr, sf_index, "
    + ", ".join([f'bin{i}_pkts' for i in range(1, 17)])
    + ") VALUES (%s, %s, %s, " + ", ".join(['%s'] * 16) + ")"
)
_SQL_SCHED_FLOW         = "INSERT INTO lm_docs_qos_service_flow_sfid (timestamp, cm_mac_addr, sf_index, service_class_name) VALUES (%s, %s, %s, %s)"
_SQL_KSAMIS1_SFID       = "INSERT INTO lm_docs_qos_service_flow_sfid_delta (timestamp, cm_mac_addr, sf_index, service_class_name) VALUES (%s, %s, %s, %s)"
_SQL_KSAMIS1_PKTS       = "INSERT INTO lm_docs_qos_service_flow_pkts_delta (timestamp, cm_mac_addr, sf_index, pkts, raw_prev, raw_curr, delta) VALUES (%s, %s, %s, %s, %s, %s, %s)"
_SQL_KSAMIS1_OCTETS     = "INSERT INTO lm_docs_qos_service_flow_octets_delta (timestamp, cm_mac_addr, sf_index, octets, raw_prev, raw_curr, delta) VALUES (%s, %s, %s, %s, %s, %s, %s)"
_SQL_KSAMIS1_DROPPED    = "INSERT INTO lm_docs_qos_service_flow_dropped_delta (timestamp, cm_mac_addr, sf_index, aqm_dropped_pkts, policed_drop_pkts, raw_prev_aqm, raw_curr_aqm, raw_prev_policed, raw_curr_policed, delta_aqm, delta_policed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
_SQL_KSAMIS1_TIME       = "INSERT INTO lm_docs_qos_service_flow_time_created (timestamp, cm_mac_addr, sf_index, created_at) VALUES (%s, %s, %s, %s)"
_SQL_CONG_ECT0          = "INSERT INTO lm_docs_qos_sf_congestion_total_ect0_pkts (timestamp, cm_mac_addr, sf_index, total_ect0_pkts) VALUES (%s, %s, %s, %s)"
_SQL_CONG_ECT1          = "INSERT INTO lm_docs_qos_sf_congestion_total_ect1_pkts (timestamp, cm_mac_addr, sf_index, total_ect1_pkts) VALUES (%s, %s, %s, %s)"
_SQL_CONG_CE_MARKED     = "INSERT INTO lm_docs_qos_sf_congestion_ce_marked_ect1_pkts (timestamp, cm_mac_addr, sf_index, ce_marked_ect1_pkts) VALUES (%s, %s, %s, %s)"
_SQL_CONG_ARRIVED_CE    = "INSERT INTO lm_docs_qos_sf_congestion_arrived_ce_pkts (timestamp, cm_mac_addr, sf_index, arrived_ce_pkts) VALUES (%s, %s, %s, %s)"
_SQL_LAT_BIN_EDGE       = "INSERT INTO lm_docs_qos_sf_latency_bin_edge (timestamp, cm_mac_addr, sf_index, latency_bin_edge) VALUES (%s, %s, %s, %s)"
_SQL_ASF_MAP            = "INSERT INTO lm_qos_aggregate_service_flow_map (timestamp, cm_mac_addr, sf_index, aggregate_sf_id) VALUES (%s, %s, %s, %s)"
_SQL_DOCS_DEV_EVENT     = "INSERT INTO lm_docs_dev_event_table (timestamp, cm_mac_addr, event_index, first_time, last_time, counts, level, event_id, event_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

# ---------------------------------------------------------------------------
# Delta state
# ---------------------------------------------------------------------------

_prev_counters = {}

# ---------------------------------------------------------------------------
# DB connections
# ---------------------------------------------------------------------------

def connect_output_db():
    return psycopg2.pool.ThreadedConnectionPool(
        2, CONCURRENCY + 2,
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS, dbname=PG_DB
    )


def fetch_modems_from_db():
    """Pull MAC addresses from latency_on_vcmts.ds_histograms_enabled_modems."""
    conn = psycopg2.connect(
        host=SRC_HOST, port=SRC_PORT,
        dbname=SRC_DB, user=SRC_USER, password=SRC_PASS
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT dhem.* FROM public.ds_histograms_enabled_modems AS dhem")
        rows = cur.fetchall()
        macs = list({normalize_mac(str(row[3])) for row in rows if row[3]})  # col 3 = mac_address
        log.info(f"Fetched {len(macs)} modems from {SRC_DB}.ds_histograms_enabled_modems")
        return macs
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Elements API
# ---------------------------------------------------------------------------

def normalize_mac(mac):
    return mac.strip().replace(":", "").replace("-", "").replace(".", "").lower()


def mac_with_colons(mac):
    return ':'.join(mac[i:i+2] for i in range(0, 12, 2))


def api_login():
    creds = b64encode(f"{API_USER}:{API_PASS}".encode()).decode()
    try:
        resp = requests.post(
            f"{BASE_URL}/api/login/",
            headers={
                'Content-Type': 'application/json;charset=UTF-8',
                'Authorization': f'Basic {creds}'
            },
            json={}, verify=False, timeout=API_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        raise RuntimeError(f"API login timed out after {API_TIMEOUT}s")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"API login connection error: {e}") from e
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"API login HTTP error {e.response.status_code}") from e
    except ValueError as e:
        raise RuntimeError(f"API login returned invalid JSON: {e}") from e

    if 'token' in data:
        return data['token']
    elif data.get('success') and 'response' in data:
        return data['response']['token']
    else:
        raise RuntimeError(f"Login failed: {data}")


def snmp_walk(token, device_id):
    resp = requests.post(
        f"{BASE_URL}/api/snmp/walk/{device_id}?ensure_cm=true",
        headers={
            'Content-Type': 'application/json;charset=UTF-8',
            'Authorization': f'Token {token}'
        },
        json={"oids": AQM_OIDS},
        verify=False, timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def snmp_walk_docs_events(token, device_id):
    """Walk docsDevEventTable (1.3.6.1.2.1.69.1.5.8) for a CM."""
    resp = requests.post(
        f"{BASE_URL}/api/snmp/walk/{device_id}?ensure_cm=true",
        headers={
            'Content-Type': 'application/json;charset=UTF-8',
            'Authorization': f'Token {token}'
        },
        json={"oids": DOCS_EVENT_OIDS},
        verify=False, timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def _decode_date_and_time(val):
    """Convert SNMP DateAndTime hex string to ISO timestamp string.
    DateAndTime is 8 or 11 bytes: year(2) month day hour min sec deci-sec [tz-dir tz-hr tz-min]
    Elements API may return it as a hex string like '80 00' or as an int/string.
    """
    if val is None:
        return None
    try:
        if isinstance(val, str) and ' ' in val:
            b = bytes(int(x, 16) for x in val.strip().split())
        elif isinstance(val, (bytes, bytearray)):
            b = bytes(val)
        else:
            return str(val)
        if len(b) < 8:
            return val if isinstance(val, str) else str(val)
        year  = (b[0] << 8) | b[1]
        month, day, hour, minute, sec, deci = b[2], b[3], b[4], b[5], b[6], b[7]
        ts = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{sec:02d}.{deci}"
        if len(b) == 11:
            tz_dir = '+' if chr(b[8]) == '+' else '-'
            ts += f" {tz_dir}{b[9]:02d}:{b[10]:02d}"
        return ts
    except Exception:
        return str(val)


def _clean_event_text(val):
    if isinstance(val, str):
        return val.replace('&quot;', '"').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').strip()
    return val


def _parse_event_rows(cm_mac_fmt, data, poll_ts, first_key, last_key, counts_key, level_key, id_key, text_key):
    rows = []
    for entry in data['data']:
        walked_data = entry.get('response', {}).get('walked_data', {})
        for event_index, m in walked_data.items():
            rows.append((
                poll_ts, cm_mac_fmt, event_index,
                _decode_date_and_time(m.get(first_key)),
                _decode_date_and_time(m.get(last_key)),
                m.get(counts_key), m.get(level_key), m.get(id_key),
                _clean_event_text(m.get(text_key)),
            ))
    return rows


def insert_docs_dev_events(conn, cm_mac, data, poll_ts):
    """Parse docsDevEventTable walk response and insert into lm_docs_dev_event_table."""
    if not data.get('success') or not data.get('data'):
        return 0
    rows = _parse_event_rows(
        mac_with_colons(cm_mac), data, poll_ts,
        'docsDevEvFirstTime', 'docsDevEvLastTime', 'docsDevEvCounts',
        'docsDevEvLevel', 'docsDevEvId', 'docsDevEvText'
    )
    if not rows:
        return 0
    try:
        cur = conn.cursor()
        cur.executemany(_SQL_DOCS_DEV_EVENT, rows)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        log.error(f"{cm_mac} -> DOCS DEV EVENT INSERT FAILED: {type(e).__name__}: {e}")
        return 0
    finally:
        try:
            cur.close()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

def insert_upstream_metrics(conn, cm_mac, data, poll_ts):
    if not data.get('success') or not data.get('data'):
        log.warning(f"{cm_mac} -> API returned no data")
        return 0

    now = poll_ts
    cm_mac_fmt = mac_with_colons(cm_mac)

    us_rows, cm_rows = [], []
    qos_sf_pkts_rows, qos_sf_octets_rows, docs_qos_rows = [], [], []
    dp_aqm_rows, dp_sanc_rows = [], []
    dp_lat_max_rows, dp_lat_avg_rows, dp_lat_bin_rows = [], [], []
    hist_rows, sched_rows, ksamis1_sfid_rows = [], [], []
    ksamis1_pkts_rows, ksamis1_octets_rows, ksamis1_dropped_rows, ksamis1_time_rows = [], [], [], []
    cong_ect0_rows, cong_ect1_rows, cong_ce_marked_rows, cong_arrived_ce_rows = [], [], [], []
    lat_bin_edge_rows, asf_map_rows = [], []

    for entry in data['data']:
        walked_data = entry.get('response', {}).get('walked_data', {})

        asf_scn_map = {}
        for sf_id, m in walked_data.items():
            scn = m.get('docsQosParamSetServiceClassName',
                        m.get('docsQosParamSetServiceClassNameActive',
                        m.get('docsQosParamSetServiceClassNameAdmitted', '')))
            if scn:
                asf_scn_map[sf_id] = scn

        for sf_id, m in walked_data.items():
            scn = m.get('docsQosParamSetServiceClassName',
                        m.get('docsQosParamSetServiceClassNameActive',
                        m.get('docsQosParamSetServiceClassNameAdmitted', '')))
            if not scn:
                asf_id = str(m.get('qos_aggregate_service_flow_id', 0))
                if asf_id and asf_id != '0':
                    scn = asf_scn_map.get(asf_id, '')

            direction = m.get('docsQosServiceFlowDirection', m.get('qos_service_flow_direction', 0))
            is_downstream = (str(direction) == '1') or (scn and scn.lower().startswith('ds'))
            if is_downstream:
                continue

            if not scn:
                scn = 'unknown_us'

            max_rate      = m.get('docsQosParamSetMaxTrafficRate', 0)
            max_burst     = m.get('docsQosParamSetMaxTrafficBurst', 0)
            max_concat    = m.get('docsQosParamSetMaxConcatBurst', 0)
            aqm_target    = m.get('docsQosParamSetAqmLatencyTarget', 0)
            min_buf       = m.get('docsQosParamSetMinimumBuffer', 0)
            tgt_buf       = m.get('docsQosParamSetTargetBuffer', 0)
            max_buf       = m.get('docsQosParamSetMaximumBuffer', 0)
            buf_size      = m.get('docsQosServiceFlowBufferSize', 0)
            pkts          = m.get('docsQosServiceFlowPkts', 0)
            octets        = m.get('docsQosServiceFlowOctets', 0)
            policed_drop  = m.get('docsQosServiceFlowPolicedDropPkts', 0)
            policed_delay = m.get('docsQosServiceFlowPolicedDelayPkts', 0)
            aqm_dropped   = m.get('docsQosServiceFlowAQMDroppedPkts', 0)
            cong_sanc        = m.get('docsQosSfCongestionSanctionedPkts', 0)
            cong_ect0        = m.get('docsQosSfCongestionTotalEct0Pkts', 0)
            cong_ect1        = m.get('docsQosSfCongestionTotalEct1Pkts', 0)
            cong_ce_marked   = m.get('docsQosSfCongestionCeMarkedEct1Pkts', 0)
            cong_arrived_ce  = m.get('docsQosSfCongestionArrivedCePkts', 0)
            _lbe_raw      = m.get('docsQosSfLatencyBinEdge', 0)
            try:
                lat_bin_edge = int(_lbe_raw)
            except (ValueError, TypeError):
                lat_bin_edge = None
            lat_max       = m.get('docsQosSfLatencyMaxLatency', 0)
            lat_hist      = m.get('docsQosSfLatencyNumHistUpdates', 0)
            bins = [m.get(f'docsQosSfLatencyBin{i}Pkts', 0) for i in range(1, 17)]

            cm_rows.append((cm_mac_fmt, now))
            us_rows.append((
                now, cm_mac_fmt, sf_id, scn,
                max_rate, max_burst, aqm_target,
                min_buf, tgt_buf, max_buf, buf_size,
                pkts, octets, policed_drop, policed_delay,
                aqm_dropped, cong_ce_marked, cong_sanc,
                lat_max, lat_hist, *bins
            ))
            qos_sf_pkts_rows.append((now, cm_mac_fmt, sf_id, pkts))
            qos_sf_octets_rows.append((now, cm_mac_fmt, sf_id, octets))
            for pname, pval in {
                'max_traffic_rate': max_rate, 'max_traffic_burst': max_burst,
                'aqm_latency_target': aqm_target, 'minimum_buffer': min_buf,
                'target_buffer': tgt_buf, 'maximum_buffer': max_buf,
            }.items():
                docs_qos_rows.append((now, cm_mac_fmt, sf_id, pname, str(pval)))

            dp_aqm_rows.append((now, cm_mac_fmt, sf_id, aqm_dropped))
            dp_sanc_rows.append((now, cm_mac_fmt, sf_id, cong_sanc))
            dp_lat_max_rows.append((now, cm_mac_fmt, sf_id, lat_max))
            dp_lat_avg_rows.append((now, cm_mac_fmt, sf_id, lat_hist, buf_size))
            for bin_num in range(1, 17):
                dp_lat_bin_rows.append((now, cm_mac_fmt, sf_id, bin_num, bins[bin_num - 1]))

            hist_rows.append((now, cm_mac_fmt, sf_id, *bins))
            sched_rows.append((now, cm_mac_fmt, sf_id, scn))
            ksamis1_sfid_rows.append((now, cm_mac_fmt, sf_id, scn))

            key = (cm_mac_fmt, sf_id)
            current = {'pkts': pkts, 'octets': octets, 'aqm_dropped': aqm_dropped, 'policed_drop': policed_drop}
            prev = _prev_counters.get(key)
            if prev is not None:
                delta_pkts    = pkts - prev['pkts']
                delta_octets  = octets - prev['octets']
                delta_aqm     = aqm_dropped - prev['aqm_dropped']
                delta_policed = policed_drop - prev['policed_drop']
                if delta_pkts >= 0:
                    ksamis1_pkts_rows.append((now, cm_mac_fmt, sf_id, delta_pkts, prev['pkts'], pkts, delta_pkts))
                if delta_octets >= 0:
                    ksamis1_octets_rows.append((now, cm_mac_fmt, sf_id, delta_octets, prev['octets'], octets, delta_octets))
                if delta_aqm >= 0 and delta_policed >= 0:
                    ksamis1_dropped_rows.append((now, cm_mac_fmt, sf_id, delta_aqm, delta_policed,
                                                 prev['aqm_dropped'], aqm_dropped,
                                                 prev['policed_drop'], policed_drop,
                                                 delta_aqm, delta_policed))
            _prev_counters[key] = current

            ksamis1_time_rows.append((now, cm_mac_fmt, sf_id, now))
            cong_ect0_rows.append((now, cm_mac_fmt, sf_id, cong_ect0))
            cong_ect1_rows.append((now, cm_mac_fmt, sf_id, cong_ect1))
            cong_ce_marked_rows.append((now, cm_mac_fmt, sf_id, cong_ce_marked))
            cong_arrived_ce_rows.append((now, cm_mac_fmt, sf_id, cong_arrived_ce))
            lat_bin_edge_rows.append((now, cm_mac_fmt, sf_id, lat_bin_edge))
            asf_map_rows.append((now, cm_mac_fmt, sf_id, int(m.get('qos_aggregate_service_flow_id', 0))))

    if not us_rows:
        log.warning(f"{cm_mac} -> no upstream service flows found")
        return 0

    try:
        cur = conn.cursor()
        cur.executemany(_CM_SQL, cm_rows)
        cur.executemany(_US_SQL, us_rows)
        cur.executemany(_SQL_QOS_SF_PKTS, qos_sf_pkts_rows)
        cur.executemany(_SQL_QOS_SF_OCTETS, qos_sf_octets_rows)
        cur.executemany(_SQL_DOCS_QOS_PARAMS, docs_qos_rows)
        cur.executemany(_SQL_DP_AQM_DROPPED, dp_aqm_rows)
        cur.executemany(_SQL_DP_SANCTIONED, dp_sanc_rows)
        cur.executemany(_SQL_DP_LAT_MAX, dp_lat_max_rows)
        cur.executemany(_SQL_DP_LAT_AVG, dp_lat_avg_rows)
        cur.executemany(_SQL_DP_LAT_BIN, dp_lat_bin_rows)
        cur.executemany(_SQL_HISTOGRAM, hist_rows)
        cur.executemany(_SQL_SCHED_FLOW, sched_rows)
        cur.executemany(_SQL_KSAMIS1_SFID, ksamis1_sfid_rows)
        cur.executemany(_SQL_KSAMIS1_PKTS, ksamis1_pkts_rows)
        cur.executemany(_SQL_KSAMIS1_OCTETS, ksamis1_octets_rows)
        cur.executemany(_SQL_KSAMIS1_DROPPED, ksamis1_dropped_rows)
        cur.executemany(_SQL_KSAMIS1_TIME, ksamis1_time_rows)
        cur.executemany(_SQL_CONG_ECT0, cong_ect0_rows)
        cur.executemany(_SQL_CONG_ECT1, cong_ect1_rows)
        cur.executemany(_SQL_CONG_CE_MARKED, cong_ce_marked_rows)
        cur.executemany(_SQL_CONG_ARRIVED_CE, cong_arrived_ce_rows)
        cur.executemany(_SQL_LAT_BIN_EDGE, lat_bin_edge_rows)
        cur.executemany(_SQL_ASF_MAP, asf_map_rows)
        conn.commit()
        return len(us_rows)
    except Exception as e:
        conn.rollback()
        log.error(f"{cm_mac} -> DB INSERT FAILED: {type(e).__name__}: {e}")
        log.error(f"{cm_mac} -> Failed SQL: {cur.query if cur else 'unknown'}")
        return 0
    finally:
        try:
            cur.close()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Poll helpers
# ---------------------------------------------------------------------------

def poll_modem(token, pool, mac):
    conn = pool.getconn()
    try:
        t0 = time.perf_counter()
        data = snmp_walk(token, mac)
        docs_event_data = snmp_walk_docs_events(token, mac)
        t_api = time.perf_counter() - t0
        poll_ts = datetime.now(timezone.utc)
        t1 = time.perf_counter()
        count = insert_upstream_metrics(conn, mac, data, poll_ts)
        docs_event_count = insert_docs_dev_events(conn, mac, docs_event_data, poll_ts)
        t_db = time.perf_counter() - t1
        log.info(f"{mac} -> {count} flows | {docs_event_count} docs_events | api={t_api:.2f}s db={t_db:.2f}s")
        return count, None
    except requests.exceptions.Timeout:
        log.warning(f"{mac} -> TIMEOUT after {API_TIMEOUT}s")
        return 0, None
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status == 401:
            log.warning(f"{mac} -> 401 token expired")
            return 0, '401'
        log.error(f"{mac} -> HTTP ERROR {status}: {e}")
        return 0, None
    except requests.exceptions.ConnectionError as e:
        log.error(f"{mac} -> CONNECTION ERROR: {e}")
        return 0, None
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error(f"{mac} -> ERROR: {e}")
        return 0, None
    finally:
        pool.putconn(conn)


def poll_once(token, pool, modems):
    t_poll = time.perf_counter()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(poll_modem, token, pool, mac) for mac in modems]
        results = []
        for mac, f in zip(modems, futures):
            try:
                results.append(f.result())
            except Exception as e:
                log.error(f"{mac} -> unhandled thread exception: {type(e).__name__}: {e}")
                results.append((0, None))
    total_rows = sum(r[0] for r in results)
    token_expired = any(r[1] == '401' for r in results)
    elapsed = time.perf_counter() - t_poll
    log.info(f"Poll complete | modems={len(modems)} rows={total_rows} duration={elapsed:.1f}s")
    return total_rows, token_expired

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Latency Management Upstream Poller')
    parser.add_argument('mac', nargs='?', help='Poll a single modem by MAC address')
    parser.add_argument('--quiet', action='store_true')
    args = parser.parse_args()

    setup_logging()
    log.info("=== POLLER STARTED ===")
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    missing = []
    if not API_USER: missing.append("ELEMENTS_API_USER")
    if not API_PASS: missing.append("ELEMENTS_API_PASS")
    if missing:
        for var in missing:
            log.error(f"Missing required .env variable: {var}")
        sys.exit(1)

    if args.mac:
        modems = [normalize_mac(args.mac)]
    else:
        try:
            modems = fetch_modems_from_db()
        except Exception as e:
            log.critical(f"Failed to fetch modems from source DB: {e}")
            sys.exit(1)

    if not modems:
        log.critical("No modems found in ds_histograms_enabled_modems — exiting.")
        sys.exit(1)

    log.info(f"Modems to poll : {len(modems)}")
    log.info(f"Poll interval  : {POLL_INTERVAL}s")
    log.info(f"Modem refresh  : every 24h")
    log.info(f"API timeout    : {API_TIMEOUT}s")
    log.info(f"Concurrency    : {CONCURRENCY}")

    try:
        pool = connect_output_db()
    except Exception as e:
        log.critical(f"Output DB connection failed: {e}")
        sys.exit(1)
    log.info("Output DB connected")

    try:
        token = api_login()
    except RuntimeError as e:
        log.critical(str(e))
        log.info("Retrying login in 30s...")
        time.sleep(30)
        try:
            token = api_login()
        except RuntimeError as e:
            log.critical(f"Login retry failed: {e}")
            pool.closeall()
            sys.exit(1)
    log.info("API authenticated — POLLING STARTED (Ctrl+C to stop)")

    last_modem_refresh = time.monotonic()
    MODEM_REFRESH_INTERVAL = 86400  # 24 hours

    poll_count = 1
    try:
        while True:
            cycle_start = time.perf_counter()
            log.info(f"--- Poll #{poll_count} ---")

            if not args.mac and (time.monotonic() - last_modem_refresh >= MODEM_REFRESH_INTERVAL):
                try:
                    modems = fetch_modems_from_db()
                    last_modem_refresh = time.monotonic()
                except Exception as e:
                    log.warning(f"Could not refresh modem list: {e} — using previous list")

            try:
                while True:
                    total_rows, token_expired = poll_once(token, pool, modems)
                    if token_expired:
                        log.warning("Token expired, re-authenticating...")
                        try:
                            token = api_login()
                        except RuntimeError as e:
                            log.error(f"Re-auth failed: {e} — skipping poll #{poll_count}")
                            break
                        log.info(f"--- Retrying Poll #{poll_count} ---")
                        continue
                    break
            except psycopg2.OperationalError:
                log.warning("Output DB connection lost, reconnecting...")
                pool.closeall()
                pool = connect_output_db()
                log.info("Output DB reconnected")

            elapsed = time.perf_counter() - cycle_start
            sleep_time = max(0, POLL_INTERVAL - elapsed)
            log.info(f"Sleeping {sleep_time:.1f}s until next poll")
            time.sleep(sleep_time)
            poll_count += 1

    except KeyboardInterrupt:
        log.info("Stopping poller (KeyboardInterrupt)")
    finally:
        pool.closeall()
        log.info(f"Done. {poll_count} polls completed.")


if __name__ == "__main__":
    main()
