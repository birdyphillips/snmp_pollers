"""
STVA EFT CSV Plotter
Usage:
    python plot_stva.py                  # uses default C4509C40FED8 folder
    python plot_stva.py C44FD5AB85B0     # specify MAC folder name
    python plot_stva.py path/to/file.csv # specify CSV directly
"""
import os
import sys
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

BASE = os.path.dirname(os.path.abspath(__file__))

if len(sys.argv) > 1:
    arg = ' '.join(sys.argv[1:])
    if arg.endswith('.csv'):
        CSV = arg
    else:
        folder = os.path.join(BASE, arg)
        matches = glob.glob(os.path.join(folder, '*.csv'))
        if not matches:
            print(f"No CSV found in {folder}")
            sys.exit(1)
        CSV = matches[0]
else:
    CSV = os.path.join(BASE, 'C4509C40FED8', 'stva_eft_c4509c40fed8.csv')

print(f"Loading: {CSV}")
df = pd.read_csv(CSV, parse_dates=['timestamp'])
df = df.sort_values('timestamp')

# Compute per-poll deltas for cumulative counters
df['delta_pkts']   = df.groupby('service_class_name')['total_pkts'].diff().clip(lower=0)
df['delta_octets'] = df.groupby('service_class_name')['total_octets'].diff().clip(lower=0)
df['delta_mbps']   = (df['delta_octets'] * 8) / 1_000_000  # Mbps per 60s interval

flows = df['service_class_name'].unique()
print(f"Flows found: {list(flows)}")

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def fmt_ax(ax):
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3)

# ---------------------------------------------------------------------------
# Plot 1: Delta Mbps per flow (throughput per poll)
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(12, 5))
for scn in flows:
    d = df[df['service_class_name'] == scn]
    ax.plot(d['timestamp'], d['delta_mbps'], marker='o', markersize=3, label=scn)
ax.set_title('Throughput per Poll (Mbps)')
ax.set_ylabel('Mbps')
ax.set_xlabel('Time (UTC)')
ax.legend(fontsize=8)
fmt_ax(ax)
plt.tight_layout()
plt.savefig(os.path.join(os.path.dirname(CSV), 'plot_throughput_mbps.png'), dpi=150)
plt.show()

# ---------------------------------------------------------------------------
# Plot 2: Cumulative total_pkts per flow
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(12, 5))
for scn in flows:
    d = df[df['service_class_name'] == scn]
    ax.plot(d['timestamp'], d['total_pkts'], marker='o', markersize=3, label=scn)
ax.set_title('Cumulative Total Packets')
ax.set_ylabel('Packets')
ax.set_xlabel('Time (UTC)')
ax.legend(fontsize=8)
fmt_ax(ax)
plt.tight_layout()
plt.savefig(os.path.join(os.path.dirname(CSV), 'plot_total_pkts.png'), dpi=150)
plt.show()

# ---------------------------------------------------------------------------
# Plot 3: Latency max usec — upstream flows only
# ---------------------------------------------------------------------------
us_flows = df[df['service_class_name'].str.startswith('us')]
if not us_flows.empty:
    fig, ax = plt.subplots(figsize=(12, 5))
    for scn in us_flows['service_class_name'].unique():
        d = us_flows[us_flows['service_class_name'] == scn]
        ax.plot(d['timestamp'], d['latency_max_usec'], marker='o', markersize=3, label=scn)
    ax.set_title('Upstream Latency Max (µs)')
    ax.set_ylabel('Latency (µs)')
    ax.set_xlabel('Time (UTC)')
    ax.legend(fontsize=8)
    fmt_ax(ax)
    plt.tight_layout()
    plt.savefig(os.path.join(os.path.dirname(CSV), 'plot_latency_max.png'), dpi=150)
    plt.show()

# ---------------------------------------------------------------------------
# Plot 4: Latency histogram bins — primary upstream HSI flow (last poll)
# ---------------------------------------------------------------------------
us_hsi_scns = [s for s in df['service_class_name'].unique()
               if s.lower().startswith('us') and 'hsi' in s.lower() and '-ll' not in s.lower()]
hsi_scn = us_hsi_scns[0] if us_hsi_scns else None
hsi = df[df['service_class_name'] == hsi_scn] if hsi_scn else pd.DataFrame()
if not hsi.empty:
    last = hsi.iloc[-1]
    bins = [last[f'latency_bin{i}_pkts'] for i in range(1, 17)]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(range(1, 17), bins, color='steelblue')
    ax.set_title(f'{hsi_scn} Latency Histogram (last poll @ {last["timestamp"]})')
    ax.set_xlabel('Bin')
    ax.set_ylabel('Packets')
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(os.path.join(os.path.dirname(CSV), 'plot_latency_histogram.png'), dpi=150)
    plt.show()

# ---------------------------------------------------------------------------
# Plot 5: Policed delay pkts — primary upstream HSI flow
# ---------------------------------------------------------------------------
if not hsi.empty:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(hsi['timestamp'], hsi['policed_delay_pkts'], color='orange', marker='o', markersize=3)
    ax.set_title(f'{hsi_scn} Policed Delay Packets (cumulative)')
    ax.set_ylabel('Packets')
    ax.set_xlabel('Time (UTC)')
    fmt_ax(ax)
    plt.tight_layout()
    plt.savefig(os.path.join(os.path.dirname(CSV), 'plot_policed_delay.png'), dpi=150)
    plt.show()

print("Plots saved to", os.path.dirname(CSV))
