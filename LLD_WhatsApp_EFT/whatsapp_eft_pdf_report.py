"""
whatsapp_eft_pdf_report.py
==========================
Reads stva_eft_<mac>.csv and generates a PDF report with charts.

Usage:
    python whatsapp_eft_pdf_report.py                  # auto-finds CSV in subfolders
    python whatsapp_eft_pdf_report.py BC96E5358F9A     # specific MAC folder
    python whatsapp_eft_pdf_report.py path/to/file.csv # specific CSV file

Requirements:
    pip install pandas matplotlib
"""
import os
import sys
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
CHART_COLORS = ['#1a73e8','#34a853','#fa7b17','#ea4335','#a142f4','#24c1e0','#f538a0']
BG_DARK      = '#0d1b2a'
BG_PANEL     = '#112240'
GRID_COLOR   = '#1e3a5f'
TEXT_COLOR   = '#e8eaed'
SUBTEXT      = '#e8eaed'
ACCENT       = '#1a73e8'

MODEM_NAMES  = {
    'bc96e5358f9a': "Nuzhat's CM",
    '147d0517e08c': "Rickie's CM",
}

# ---------------------------------------------------------------------------
# Find CSV
# ---------------------------------------------------------------------------
def find_csv(arg=None):
    if arg:
        if arg.endswith('.csv'):
            return arg
        folder = os.path.join(HERE, arg.upper())
        matches = glob.glob(os.path.join(folder, '*.csv'))
        if not matches:
            print(f"No CSV found in {folder}")
            sys.exit(1)
        return matches[0]
    matches = glob.glob(os.path.join(HERE, '*', 'stva_eft_*.csv'))
    if not matches:
        print("No CSV found in subfolders.")
        sys.exit(1)
    return matches[0]

# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------
def fmt_ax(ax):
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', color=TEXT_COLOR)
    ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8)
    ax.set_axisbelow(True)

def style_ax(ax):
    ax.set_facecolor(BG_PANEL)
    ax.tick_params(colors=TEXT_COLOR, labelsize=9)
    ax.xaxis.label.set_color(SUBTEXT)
    ax.yaxis.label.set_color(SUBTEXT)
    for spine in ax.spines.values():
        spine.set_edgecolor(GRID_COLOR)

def add_header(fig, title, subtitle=None):
    hax = fig.add_axes([0, 0.91, 1, 0.09])
    hax.set_facecolor(ACCENT)
    hax.axis('off')
    hax.text(0.5, 0.62, title,
             transform=hax.transAxes, fontsize=16, fontweight='bold',
             color='white', ha='center', va='center',
             fontfamily='DejaVu Sans')
    if subtitle:
        hax.text(0.5, 0.15, subtitle,
                 transform=hax.transAxes, fontsize=8,
                 color=TEXT_COLOR, ha='center', va='center', fontstyle='italic')

def add_footer(fig, mac, modem_name, session_start, session_end):
    fax = fig.add_axes([0, 0, 1, 0.04])
    fax.set_facecolor('#0a1628')
    fax.axis('off')
    fax.text(0.5, 0.5,
             f'LLD WhatsApp EFT  |  {modem_name} ({mac})  |  {session_start} — {session_end}  |  aphillips — Charter Access Engineering',
             transform=fax.transAxes, fontsize=7, color='#445566',
             ha='center', va='center')

def make_fig():
    fig, ax = plt.subplots(figsize=(11, 5.8), subplot_kw={'facecolor': BG_PANEL})
    fig.patch.set_facecolor(BG_DARK)
    fig.subplots_adjust(top=0.88, bottom=0.18, left=0.09, right=0.97)
    style_ax(ax)
    return fig, ax

def save_page(pdf, fig, ax, header_title, subtitle, mac, modem_name, session_start, session_end):
    add_header(fig, header_title, subtitle)
    add_footer(fig, mac, modem_name, session_start, session_end)
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)

def plot_line(ax, df, y_col, group_col, ylabel):
    for i, (name, grp) in enumerate(df.groupby(group_col)):
        color = CHART_COLORS[i % len(CHART_COLORS)]
        ax.plot(grp['timestamp'], grp[y_col],
                marker='o', markersize=4, linewidth=2, color=color, label=name,
                markerfacecolor='white', markeredgecolor=color, markeredgewidth=1.5)
        ax.fill_between(grp['timestamp'], grp[y_col], alpha=0.08, color=color)
    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold')
    ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
    ax.legend(fontsize=8, facecolor=BG_DARK, edgecolor=GRID_COLOR,
              labelcolor=TEXT_COLOR, framealpha=0.9)
    fmt_ax(ax)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    csv_file = find_csv(arg)
    print(f"Reading: {csv_file}")

    df = pd.read_csv(csv_file, parse_dates=['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)

    mac           = df['cm_mac'].iloc[0].replace(':', '').lower() if 'cm_mac' in df.columns else 'unknown'
    mac_fmt       = df['cm_mac'].iloc[0] if 'cm_mac' in df.columns else 'Unknown'
    modem_name    = MODEM_NAMES.get(mac, mac_fmt)
    session_start = df['timestamp'].min().strftime('%Y-%m-%d %H:%M UTC')
    session_end   = df['timestamp'].max().strftime('%Y-%m-%d %H:%M UTC')
    total_polls   = df['timestamp'].nunique()
    flows         = df['service_class_name'].unique()
    us_flows      = df[df['service_class_name'].str.startswith('us', na=False)]

    session_duration = df['timestamp'].max() - df['timestamp'].min()
    hours, rem = divmod(int(session_duration.total_seconds()), 3600)
    minutes = rem // 60

    df['delta_octets'] = df.groupby('service_class_name')['total_octets'].diff().clip(lower=0)
    df['delta_mbps']   = (df['delta_octets'] * 8) / 1_000_000

    out_path = os.path.join(os.path.dirname(csv_file), f"whatsapp_eft_report_{mac}.pdf")

    with PdfPages(out_path) as pdf:

        # ----------------------------------------------------------------
        # Page 1 — Cover
        # ----------------------------------------------------------------
        fig = plt.figure(figsize=(11, 8.5))
        fig.patch.set_facecolor(BG_PANEL)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor(BG_PANEL)
        ax.axis('off')
        ax.text(0.5, 0.935, 'LLD WHATSAPP EFT SESSION REPORT',
                transform=ax.transAxes, fontsize=26, fontweight='bold',
                color='white', ha='center', va='center', fontfamily='DejaVu Sans')
        ax.text(0.5, 0.83, 'Charter Communications  •  Access Engineering  •  LLD EFT',
                transform=ax.transAxes, fontsize=12, color=TEXT_COLOR,
                ha='center', va='center', fontfamily='DejaVu Sans', fontstyle='italic')
        ax.axhline(y=0.79, xmin=0.05, xmax=0.95, color=ACCENT, linewidth=1.5)

        labels = [
            ('Modem',        modem_name),
            ('Modem MAC',    mac_fmt),
            ('Session Start', session_start),
            ('Session End',   session_end),
            ('Duration',      f'{hours}h {minutes}m'),
            ('Total Polls',   str(total_polls)),
            ('Poll Interval', '60 seconds'),
            ('Service Flows', ', '.join(flows)),
        ]
        y = 0.72
        for label, value in labels:
            ax.text(0.12, y, f'{label}:', transform=ax.transAxes,
                    fontsize=11, color=SUBTEXT, fontweight='bold', va='center')
            ax.text(0.38, y, value, transform=ax.transAxes,
                    fontsize=11, color='white', va='center', fontfamily='monospace')
            y -= 0.07

        ax.text(0.5, 0.02,
                f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}   |   aphillips — Charter Access Engineering',
                transform=ax.transAxes, fontsize=8, color='#445566', ha='center', va='center')

        pdf.savefig(fig, facecolor=fig.get_facecolor())
        plt.close(fig)

        # ----------------------------------------------------------------
        # Page 2 — Table of Contents
        # ----------------------------------------------------------------
        fig = plt.figure(figsize=(11, 8.5))
        fig.patch.set_facecolor(BG_PANEL)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_facecolor(BG_PANEL)
        ax.axis('off')
        ax.text(0.5, 0.935, 'TABLE OF CONTENTS',
                transform=ax.transAxes, fontsize=22, fontweight='bold',
                color='white', ha='center', va='center', fontfamily='DejaVu Sans')
        ax.axhline(y=0.88, xmin=0.05, xmax=0.95, color=ACCENT, linewidth=1)

        contents = [
            ('3',  'Throughput per Poll',           'Mbps per 60s interval across all service flows'),
            ('4',  'Upstream Latency Max',           'Peak latency (µs) per upstream flow over time'),
            ('5',  'AQM Drops & CE Marked Packets',  'Congestion AQM drops and CE marked packet counts'),
            ('6',  'Policed Delay & Drop Packets',   'Policed delay and drop packet counts per flow'),
            ('7',  'Latency Histogram',              'Bin distribution of upstream HSI latency (last poll)'),
            ('8',  'Total Octets',                   'Cumulative octets transferred per service flow'),
            ('9',  'AQM Dropped Packets',            'Service flow level AQM drop counters'),
            ('10', 'ECT(0) & ECT(1) Packets',        'ECN capable transport packet counts per flow'),
        ]

        y = 0.78
        for i, (page, title, desc) in enumerate(contents):
            row_color = '#112240' if i % 2 == 0 else '#0d1b2a'
            ax.axhspan(y - 0.03, y + 0.04, facecolor=row_color, alpha=1.0)
            ax.text(0.07, y + 0.005, f'pg {page}',
                    transform=ax.transAxes, fontsize=10, fontweight='bold',
                    color=ACCENT, va='center', ha='center',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='#0d2a4a',
                              edgecolor=ACCENT, linewidth=1))
            ax.text(0.15, y + 0.005, title,
                    transform=ax.transAxes, fontsize=11, fontweight='bold',
                    color=TEXT_COLOR, va='center', fontfamily='DejaVu Sans')
            ax.text(0.15, y - 0.022, desc,
                    transform=ax.transAxes, fontsize=9,
                    color=SUBTEXT, va='center', fontstyle='italic')
            ax.axhline(y=y - 0.03, xmin=0.05, xmax=0.95,
                       color=GRID_COLOR, linewidth=0.5, linestyle=':')
            y -= 0.08

        ax.text(0.5, 0.02, f'LLD WhatsApp EFT Report  |  {modem_name} ({mac_fmt})  |  {session_start} — {session_end}',
                transform=ax.transAxes, fontsize=8, color='#445566', ha='center', va='center')

        pdf.savefig(fig, facecolor=fig.get_facecolor())
        plt.close(fig)

        # ----------------------------------------------------------------
        # Page 3 — Throughput
        # ----------------------------------------------------------------
        fig, ax = make_fig()
        plot_line(ax, df, 'delta_mbps', 'service_class_name', 'Mbps')
        save_page(pdf, fig, ax, 'THROUGHPUT PER POLL (Mbps)',
                  f'{modem_name} ({mac_fmt})  |  All Service Flows  |  60s intervals',
                  mac_fmt, modem_name, session_start, session_end)

        # ----------------------------------------------------------------
        # Page 4 — Upstream Latency Max
        # ----------------------------------------------------------------
        if not us_flows.empty:
            fig, ax = make_fig()
            plot_line(ax, us_flows, 'latency_max_usec', 'service_class_name', 'Latency (µs)')
            save_page(pdf, fig, ax, 'UPSTREAM LATENCY MAX (µs)',
                      f'{modem_name} ({mac_fmt})  |  Upstream Flows Only',
                      mac_fmt, modem_name, session_start, session_end)

        # ----------------------------------------------------------------
        # Page 5 — AQM Drops & CE Marked
        # ----------------------------------------------------------------
        fig, ax = make_fig()
        for i, (name, grp) in enumerate(df.groupby('service_class_name')):
            c = CHART_COLORS[i % len(CHART_COLORS)]
            ax.plot(grp['timestamp'], grp['congestion_aqm_dropped_pkts'],
                    marker='o', markersize=4, linewidth=2, color=c, label=f'{name} AQM drops')
            ax.plot(grp['timestamp'], grp['congestion_ce_marked_pkts'],
                    marker='x', markersize=4, linewidth=1.5, linestyle='--',
                    color=c, alpha=0.7, label=f'{name} CE marked')
        ax.set_ylabel('Packets', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=7, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9, ncol=2)
        fmt_ax(ax)
        save_page(pdf, fig, ax, 'AQM DROPS & CE MARKED PACKETS',
                  f'{modem_name} ({mac_fmt})', mac_fmt, modem_name, session_start, session_end)

        # ----------------------------------------------------------------
        # Page 6 — Policed Delay & Drop
        # ----------------------------------------------------------------
        fig, ax = make_fig()
        for i, (name, grp) in enumerate(df.groupby('service_class_name')):
            c = CHART_COLORS[i % len(CHART_COLORS)]
            ax.plot(grp['timestamp'], grp['policed_delay_pkts'],
                    marker='o', markersize=4, linewidth=2, color=c, label=f'{name} delay')
            ax.plot(grp['timestamp'], grp['policed_drop_pkts'],
                    marker='x', markersize=4, linewidth=1.5, linestyle='--',
                    color=c, alpha=0.7, label=f'{name} drop')
        ax.set_ylabel('Packets', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=7, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9, ncol=2)
        fmt_ax(ax)
        save_page(pdf, fig, ax, 'POLICED DELAY & DROP PACKETS',
                  f'{modem_name} ({mac_fmt})', mac_fmt, modem_name, session_start, session_end)

        # ----------------------------------------------------------------
        # Page 7 — Latency Histogram
        # ----------------------------------------------------------------
        us_hsi = [s for s in flows if s.lower().startswith('us')
                  and 'hsi' in s.lower() and '-ll' not in s.lower()]
        hsi_scn = us_hsi[0] if us_hsi else None
        hsi = df[df['service_class_name'] == hsi_scn] if hsi_scn else pd.DataFrame()
        if not hsi.empty:
            last = hsi.iloc[-1]
            bins = [last[f'latency_bin{i}_pkts'] for i in range(1, 17)]
            fig, ax = make_fig()
            bars = ax.bar(range(1, 17), bins, color=ACCENT,
                          edgecolor=BG_DARK, linewidth=0.5, width=0.7)
            for bar, val in zip(bars, bins):
                if val > 0:
                    ax.text(bar.get_x() + bar.get_width()/2,
                            bar.get_height() + max(bins) * 0.01,
                            str(int(val)), ha='center', va='bottom',
                            fontsize=7, color=TEXT_COLOR)
            ax.set_xlabel('Bin', color=SUBTEXT, fontsize=10)
            ax.set_ylabel('Packets', color=SUBTEXT, fontsize=10, fontweight='bold')
            ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8, axis='y')
            ax.tick_params(colors=TEXT_COLOR)
            for spine in ax.spines.values():
                spine.set_edgecolor(GRID_COLOR)
            save_page(pdf, fig, ax, 'LATENCY HISTOGRAM — LAST POLL',
                      f'{hsi_scn}  |  Poll @ {last["timestamp"]}',
                      mac_fmt, modem_name, session_start, session_end)

        # ----------------------------------------------------------------
        # Page 8 — Total Octets
        # ----------------------------------------------------------------
        fig, ax = make_fig()
        plot_line(ax, df, 'total_octets', 'service_class_name', 'Octets')
        save_page(pdf, fig, ax, 'TOTAL OCTETS (CUMULATIVE)',
                  f'{modem_name} ({mac_fmt})  |  All Service Flows',
                  mac_fmt, modem_name, session_start, session_end)

        # ----------------------------------------------------------------
        # Page 9 — AQM Dropped Packets
        # ----------------------------------------------------------------
        fig, ax = make_fig()
        plot_line(ax, df, 'aqm_dropped_pkts', 'service_class_name', 'Packets')
        save_page(pdf, fig, ax, 'AQM DROPPED PACKETS',
                  f'{modem_name} ({mac_fmt})  |  Service Flow Level',
                  mac_fmt, modem_name, session_start, session_end)

        # ----------------------------------------------------------------
        # Page 10 — ECT(0) & ECT(1)
        # ----------------------------------------------------------------
        fig, ax = make_fig()
        for i, (name, grp) in enumerate(df.groupby('service_class_name')):
            c = CHART_COLORS[i % len(CHART_COLORS)]
            ax.plot(grp['timestamp'], grp['congestion_ect0_pkts'],
                    marker='o', markersize=4, linewidth=2, color=c, label=f'{name} ECT(0)')
            ax.plot(grp['timestamp'], grp['congestion_ect1_pkts'],
                    marker='x', markersize=4, linewidth=1.5, linestyle='--',
                    color=c, alpha=0.7, label=f'{name} ECT(1)')
        ax.set_ylabel('Packets', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=7, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9, ncol=2)
        fmt_ax(ax)
        save_page(pdf, fig, ax, 'ECT(0) & ECT(1) PACKETS',
                  f'{modem_name} ({mac_fmt})', mac_fmt, modem_name, session_start, session_end)

        d = pdf.infodict()
        d['Title']   = f'LLD WhatsApp EFT Report — {modem_name} ({mac_fmt})'
        d['Author']  = 'aphillips — Charter Access Engineering'
        d['Subject'] = 'LLD EFT Telemetry Report'

    print(f"PDF saved: {out_path}")

if __name__ == '__main__':
    main()
