"""
plot_stva_excel.py
==================
Reads stva_eft_<mac>.csv and generates an Excel workbook with matplotlib charts.

Usage:
    python plot_stva_excel.py                        # auto-finds CSV in subfolders
    python plot_stva_excel.py C44FD5AB85B0           # specific MAC folder
"""
import os
import sys
import glob
import io
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from openpyxl import Workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.utils.dataframe import dataframe_to_rows

HERE = os.path.dirname(os.path.abspath(__file__))


def find_csv(mac=None):
    if mac:
        pattern = os.path.join(HERE, mac.upper(), f"stva_eft_{mac.lower()}.csv")
    else:
        pattern = os.path.join(HERE, '*', 'stva_eft_*.csv')
    files = glob.glob(pattern)
    if not files:
        print(f"No CSV found matching: {pattern}")
        sys.exit(1)
    return files[0]


def make_png(x, ys, labels, title, ylabel):
    fig, ax = plt.subplots(figsize=(10, 4))
    for y, label in zip(ys, labels):
        ax.plot(x, y, marker='o', markersize=3, label=label)
    ax.set_title(title, fontsize=11)
    ax.set_ylabel(ylabel)
    ax.set_xlabel('Time (UTC)')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=130)
    plt.close(fig)
    return io.BytesIO(buf.getvalue())


def main():
    mac = sys.argv[1] if len(sys.argv) > 1 else None
    csv_path = find_csv(mac)
    print(f"Reading: {csv_path}")

    COLUMNS = [
        'timestamp', 'cm_mac', 'sf_index', 'service_class_name',
        'max_traffic_rate', 'max_traffic_burst', 'aqm_latency_target',
        'minimum_buffer', 'target_buffer', 'maximum_buffer', 'buffer_size',
        'total_pkts', 'total_octets', 'policed_drop_pkts', 'policed_delay_pkts',
        'aqm_dropped_pkts', 'congestion_aqm_dropped_pkts', 'congestion_sanctioned_pkts',
        'congestion_ect0_pkts', 'congestion_ect1_pkts', 'congestion_ce_marked_pkts',
        'congestion_arrived_ce_pkts', 'latency_bin_edge', 'latency_max_usec',
        'latency_num_hist_updates',
        *[f'latency_bin{i}_pkts' for i in range(1, 17)],
    ]

    # Detect whether the CSV has a header row
    with open(csv_path) as f:
        first = f.readline()
    has_header = first.startswith('timestamp')

    if has_header:
        df = pd.read_csv(csv_path, parse_dates=['timestamp'])
    else:
        df = pd.read_csv(csv_path, header=None, names=COLUMNS,
                         parse_dates=['timestamp'], on_bad_lines='skip')
        df = df[df['timestamp'].notna()]

    df = df.sort_values('timestamp').reset_index(drop=True)

    # Strip timezone for openpyxl data sheets
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)

    df['delta_pkts']   = df.groupby('service_class_name')['total_pkts'].diff().clip(lower=0)
    df['delta_octets'] = df.groupby('service_class_name')['total_octets'].diff().clip(lower=0)
    df['delta_mbps']   = (df['delta_octets'] * 8) / 1_000_000

    wb = Workbook()
    wb.remove(wb.active)

    for scn in df['service_class_name'].unique():
        sub = df[df['service_class_name'] == scn].copy().reset_index(drop=True)
        ts  = sub['timestamp']
        has_traffic = sub['total_pkts'].sum() > 0

        ws_data = wb.create_sheet(title=scn[:28])
        for r in dataframe_to_rows(sub, index=False, header=True):
            ws_data.append(r)

        ws_chart = wb.create_sheet(title=f"{scn[:25]}_ch")
        row = 1
        charts = []

        if has_traffic:
            charts.append(make_png(ts, [sub['delta_mbps']], ['Mbps'],
                                   f'{scn} — Throughput per Poll', 'Mbps'))
            charts.append(make_png(ts, [sub['delta_pkts']], ['Packets'],
                                   f'{scn} — Packets per Poll (delta)', 'Packets'))

        if sub['latency_max_usec'].sum() > 0:
            charts.append(make_png(ts, [sub['latency_max_usec']], ['latency_max_usec'],
                                   f'{scn} — Latency Max', 'µs'))

        bin_cols  = [f'latency_bin{i}_pkts' for i in range(1, 17)]
        bin_data  = [sub[c] for c in bin_cols if sub[c].sum() > 0]
        bin_names = [c for c in bin_cols if sub[c].sum() > 0]
        if bin_data:
            charts.append(make_png(ts, bin_data, bin_names,
                                   f'{scn} — Latency Bins', 'Packets'))

        delay_cols = ['policed_delay_pkts', 'policed_drop_pkts', 'aqm_dropped_pkts']
        delay_data = [sub[c] for c in delay_cols if c in sub.columns]
        if delay_data:
            charts.append(make_png(ts, delay_data, delay_cols[:len(delay_data)],
                                   f'{scn} — Policed Delay & Drops', 'Packets'))

        for buf in charts:
            img = XLImage(buf)
            img.anchor = f'A{row}'
            ws_chart.add_image(img)
            row += 23

        print(f"  {scn}: {len(charts)} charts")

    # --- Summary sheet: timestamp + total_pkts per service class ---
    active_scns = [s for s in df['service_class_name'].unique()
                   if df[df['service_class_name'] == s]['total_pkts'].sum() > 0]
    if active_scns:
        ws_sum = wb.create_sheet(title='total_pkts_summary', index=0)
        summary = df[df['service_class_name'].isin(active_scns)][['timestamp', 'service_class_name', 'total_pkts']]
        pivot = summary.pivot_table(index='timestamp', columns='service_class_name',
                                    values='total_pkts', aggfunc='first').reset_index()
        for r in dataframe_to_rows(pivot, index=False, header=True):
            ws_sum.append(r)

        ws_sum_ch = wb.create_sheet(title='total_pkts_chart', index=1)
        ts = pivot['timestamp']
        ys  = [pivot[s] for s in active_scns if s in pivot.columns]
        buf = make_png(ts, ys, active_scns,
                       'Total Packets by Service Class', 'Packets (cumulative)')
        img = XLImage(buf)
        img.anchor = 'A1'
        ws_sum_ch.add_image(img)
        print(f"  Summary: total_pkts chart for {active_scns}")

    out_path = os.path.join(HERE, 'stva_eft_charts.xlsx')
    wb.save(out_path)
    print(f"Saved: {out_path}")


if __name__ == '__main__':
    main()
