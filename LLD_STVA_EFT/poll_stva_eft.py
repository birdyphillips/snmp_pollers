"""
LLD STVA EFT Poller
====================
Polls C4509C40FED8 or C44FD5AB85B0 (or both) via Elements API every 60s.
Writes raw API response to JSON and flattened upstream flows to CSV.

Output files (created in same directory as this script):
    stva_eft_<MAC>_<YYYYMMDD>.json
    stva_eft_<MAC>_<YYYYMMDD>.csv
"""
import os
import sys
import csv
import json
import time
import logging
from datetime import datetime, timezone
from base64 import b64encode

import requests
import urllib3
import requests.packages.urllib3.util.connection as urllib3_cn
from dotenv import load_dotenv

urllib3_cn.HAS_IPV6 = False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL    = os.getenv('ELEMENTS_API_URL', 'https://api.elements.charter.com')
API_USER    = os.getenv('ELEMENTS_API_USER')
API_PASS    = os.getenv('ELEMENTS_API_PASS')
API_TIMEOUT = int(os.getenv('API_TIMEOUT', '25'))
POLL_INTERVAL = 60

KNOWN_MACS = {
    '1': 'c4509c40fed8',
    '2': 'c44fd5ab85b0',
}

AQM_OIDS = [
    {"oid_name": "docsQosSfLatencyMaxLatency",         "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.1"},
    {"oid_name": "docsQosSfLatencyNumHistUpdates",     "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.2.1.2"},
    *[{"oid_name": f"docsQosSfLatencyBin{i}Pkts",     "oid": f"1.3.6.1.4.1.4491.2.1.21.1.29.2.1.{i+2}"} for i in range(1, 17)],
    {"oid_name": "qos_service_flow_buffer_size",       "oid": "1.3.6.1.4.1.4491.2.1.21.1.3.1.17"},
    {"oid_name": "docsQosSfCongestionAqmDroppedPkts",  "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.1"},
    {"oid_name": "docsQosSfCongestionSanctionedPkts",  "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.4"},
    {"oid_name": "docsQosSfCongestionTotalEct0Pkts",   "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.5"},
    {"oid_name": "docsQosSfCongestionTotalEct1Pkts",   "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.6"},
    {"oid_name": "docsQosSfCongestionCeMarkedEct1Pkts","oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.7"},
    {"oid_name": "docsQosSfCongestionArrivedCePkts",   "oid": "1.3.6.1.4.1.4491.2.1.21.1.30.1.8"},
    {"oid_name": "docsQosSfLatencyBinEdge",            "oid": "1.3.6.1.4.1.4491.2.1.21.1.29.1.1.2"},
    {"oid_name": "qos_param_set_max_traffic_rate",     "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.6.2.1"},
    {"oid_name": "qos_param_set_max_traffic_burst",    "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.7.2.1"},
    {"oid_name": "qos_param_set_service_class_name",   "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.4.2.2"},
    {"oid_name": "docsQosParamSetAqmLatencyTarget",    "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.43.2.1"},
    {"oid_name": "docsQosParamSetMinimumBuffer",       "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.39.2.1"},
    {"oid_name": "docsQosParamSetTargetBuffer",        "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.40.2.1"},
    {"oid_name": "docsQosParamSetMaximumBuffer",       "oid": "1.3.6.1.4.1.4491.2.1.21.1.2.1.41.2.1"},
    {"oid_name": "qos_service_flow_pkts",              "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.1"},
    {"oid_name": "qos_service_flow_octets",            "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.2"},
    {"oid_name": "qos_service_flow_policed_drop_pkts", "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.6"},
    {"oid_name": "qos_service_flow_policed_delay_pkts","oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.7"},
    {"oid_name": "qos_service_flow_aqm_dropped_pkts",  "oid": "1.3.6.1.4.1.4491.2.1.21.1.4.1.8"},
]

CSV_FIELDS = [
    'timestamp', 'cm_mac', 'sf_index', 'service_class_name',
    'max_traffic_rate', 'max_traffic_burst', 'aqm_latency_target',
    'minimum_buffer', 'target_buffer', 'maximum_buffer', 'buffer_size',
    'total_pkts', 'total_octets', 'policed_drop_pkts', 'policed_delay_pkts',
    'aqm_dropped_pkts', 'congestion_aqm_dropped_pkts', 'congestion_sanctioned_pkts',
    'congestion_ect0_pkts', 'congestion_ect1_pkts', 'congestion_ce_marked_pkts',
    'congestion_arrived_ce_pkts', 'latency_bin_edge',
    'latency_max_usec', 'latency_num_hist_updates',
    *[f'latency_bin{i}_pkts' for i in range(1, 17)],
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def normalize_mac(mac):
    return mac.strip().replace(':', '').replace('-', '').replace('.', '').lower()

def mac_with_colons(mac):
    return ':'.join(mac[i:i+2] for i in range(0, 12, 2))

def api_login():
    creds = b64encode(f"{API_USER}:{API_PASS}".encode()).decode()
    resp = requests.post(
        f"{BASE_URL}/api/login/",
        headers={'Content-Type': 'application/json;charset=UTF-8', 'Authorization': f'Basic {creds}'},
        json={}, verify=False, timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    data = resp.json()
    if 'token' in data:
        return data['token']
    elif data.get('success') and 'response' in data:
        return data['response']['token']
    raise RuntimeError(f"Login failed: {data}")

def snmp_walk(token, mac):
    resp = requests.post(
        f"{BASE_URL}/api/snmp/walk/{mac}?ensure_cm=true",
        headers={'Content-Type': 'application/json;charset=UTF-8', 'Authorization': f'Token {token}'},
        json={"oids": AQM_OIDS}, verify=False, timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))

def mac_dir(mac):
    path = os.path.join(HERE, mac.upper())
    os.makedirs(path, exist_ok=True)
    return path

def csv_path(mac):
    return os.path.join(mac_dir(mac), f"stva_eft_{mac}.csv")

def write_json(mac, poll_num, timestamp, data):
    path = os.path.join(mac_dir(mac), f"poll_{poll_num}.json")
    record = {"poll": poll_num, "timestamp": timestamp.isoformat(), "mac": mac, "response": data}
    with open(path, 'w') as f:
        json.dump(record, f, indent=2)

def append_csv(mac, timestamp, data):
    path = csv_path(mac)
    write_header = not os.path.exists(path)
    cm_mac_fmt = mac_with_colons(mac)

    rows = []
    if data.get('success') and data.get('data'):
        for entry in data['data']:
            walked = entry.get('response', {}).get('walked_data', {})
            for sf_id, m in walked.items():
                scn = m.get('docsQosParamSetServiceClassName', m.get('qos_param_set_service_class_name', ''))
                if not scn:
                    continue
                bins = [m.get(f'docsQosSfLatencyBin{i}Pkts', 0) for i in range(1, 17)]
                row = {
                    'timestamp':                   timestamp.isoformat(),
                    'cm_mac':                      cm_mac_fmt,
                    'sf_index':                    sf_id,
                    'service_class_name':          scn,
                    'max_traffic_rate':            m.get('docsQosParamSetMaxTrafficRate',    m.get('qos_param_set_max_traffic_rate', 0)),
                    'max_traffic_burst':           m.get('docsQosParamSetMaxTrafficBurst',   m.get('qos_param_set_max_traffic_burst', 0)),
                    'aqm_latency_target':          m.get('docsQosParamSetAqmLatencyTarget',  0),
                    'minimum_buffer':              m.get('docsQosParamSetMinimumBuffer',     0),
                    'target_buffer':               m.get('docsQosParamSetTargetBuffer',      0),
                    'maximum_buffer':              m.get('docsQosParamSetMaximumBuffer',     0),
                    'buffer_size':                 m.get('docsQosServiceFlowBufferSize',     m.get('qos_service_flow_buffer_size', 0)),
                    'total_pkts':                  m.get('docsQosServiceFlowPkts',           m.get('qos_service_flow_pkts', 0)),
                    'total_octets':                m.get('docsQosServiceFlowOctets',         m.get('qos_service_flow_octets', 0)),
                    'policed_drop_pkts':           m.get('docsQosServiceFlowPolicedDropPkts',  m.get('qos_service_flow_policed_drop_pkts', 0)),
                    'policed_delay_pkts':          m.get('docsQosServiceFlowPolicedDelayPkts', m.get('qos_service_flow_policed_delay_pkts', 0)),
                    'aqm_dropped_pkts':            m.get('docsQosServiceFlowAQMDroppedPkts',   m.get('qos_service_flow_aqm_dropped_pkts', 0)),
                    'congestion_aqm_dropped_pkts': m.get('docsQosSfCongestionAqmDroppedPkts', 0),
                    'congestion_sanctioned_pkts':  m.get('docsQosSfCongestionSanctionedPkts', 0),
                    'congestion_ect0_pkts':        m.get('docsQosSfCongestionTotalEct0Pkts', 0),
                    'congestion_ect1_pkts':        m.get('docsQosSfCongestionTotalEct1Pkts', 0),
                    'congestion_ce_marked_pkts':   m.get('docsQosSfCongestionCeMarkedEct1Pkts', 0),
                    'congestion_arrived_ce_pkts':  m.get('docsQosSfCongestionArrivedCePkts', 0),
                    'latency_bin_edge':            m.get('docsQosSfLatencyBinEdge', 0),
                    'latency_max_usec':            m.get('docsQosSfLatencyMaxLatency', 0),
                    'latency_num_hist_updates':    m.get('docsQosSfLatencyNumHistUpdates', 0),
                    **{f'latency_bin{i}_pkts': bins[i-1] for i in range(1, 17)},
                }
                rows.append(row)

    with open(path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    return len(rows)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def prompt_macs():
    print("\nSelect modem(s) to poll:")
    print("  1 - C4509C40FED8")
    print("  2 - C44FD5AB85B0")
    print("  3 - Both")
    choice = input("Enter choice [1/2/3]: ").strip()
    if choice == '1':
        return [KNOWN_MACS['1']]
    elif choice == '2':
        return [KNOWN_MACS['2']]
    elif choice == '3':
        return [KNOWN_MACS['1'], KNOWN_MACS['2']]
    else:
        print("Invalid choice, defaulting to both.")
        return [KNOWN_MACS['1'], KNOWN_MACS['2']]

def main():
    if not API_USER or not API_PASS:
        log.error("ELEMENTS_API_USER and ELEMENTS_API_PASS must be set in .env")
        sys.exit(1)

    modems = prompt_macs()
    log.info(f"Polling: {[mac_with_colons(m) for m in modems]} every {POLL_INTERVAL}s")

    log.info("Authenticating...")
    token = api_login()
    log.info("Authenticated. Starting poll loop (Ctrl+C to stop)")

    poll_count = 0
    try:
        while True:
            poll_count += 1
            cycle_start = time.perf_counter()
            now = datetime.now(timezone.utc)
            log.info(f"--- Poll #{poll_count} @ {now.strftime('%Y-%m-%d %H:%M:%S UTC')} ---")

            for mac in modems:
                try:
                    data = snmp_walk(token, mac)
                    write_json(mac, poll_count, now, data)
                    rows = append_csv(mac, now, data)
                    log.info(f"{mac_with_colons(mac)} -> {rows} US flows written")
                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 401:
                        log.warning("Token expired, re-authenticating...")
                        token = api_login()
                    else:
                        log.error(f"{mac} -> HTTP {e}")
                except requests.exceptions.Timeout:
                    log.warning(f"{mac} -> timeout")
                except Exception as e:
                    log.error(f"{mac} -> {e}")

            elapsed = time.perf_counter() - cycle_start
            sleep_time = max(0, POLL_INTERVAL - elapsed)
            log.info(f"Sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info(f"Stopped after {poll_count} polls.")

if __name__ == '__main__':
    main()
