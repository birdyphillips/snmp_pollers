"""
CMTS SNMP Poller — pysnmp 7.x (Python 3.13 compatible)
Usage: python cmts_snmp_poller.py
"""

import asyncio
from pysnmp.hlapi.v3arch.asyncio import (
    SnmpEngine, CommunityData, UdpTransportTarget,
    ContextData, ObjectType, ObjectIdentity, walk_cmd
)

# ── Config ────────────────────────────────────────────────────────────────────
TARGETS = [
    # Add your CMTS targets here
    # {"name": "<cmts-name>", "ip": "<cmts-ip>", "community": "<snmp-community>"},
]

MIBS = {
    "sf_direction":    "1.3.6.1.4.1.4491.2.1.21.1.11.1.3",
    "sf_stats":        "1.3.6.1.4.1.4491.2.1.21.1.4",
    "agg_flow_stats":  "1.3.6.1.4.1.4491.2.1.21.1.27",
    "latency_stats":   "1.3.6.1.4.1.4491.2.1.21.1.29",
    "congestion_stats":"1.3.6.1.4.1.4491.2.1.21.1.30",
}

TIMEOUT = 5
RETRIES = 2
PORT    = 161
# ─────────────────────────────────────────────────────────────────────────────


async def snmp_walk(ip, community, oid):
    engine   = SnmpEngine()
    transport = await UdpTransportTarget.create((ip, PORT), timeout=TIMEOUT, retries=RETRIES)
    results  = []
    async for err, errStatus, errIdx, varBinds in walk_cmd(
        engine,
        CommunityData(community, mpModel=1),
        transport,
        ContextData(),
        ObjectType(ObjectIdentity(oid)),
        lexicographicMode=False,
    ):
        if err:
            print(f"  ERROR: {err}")
            break
        if errStatus:
            print(f"  ERROR: {errStatus.prettyPrint()} at {errIdx}")
            break
        for varBind in varBinds:
            results.append((str(varBind[0]), varBind[1].prettyPrint()))
    return results


async def poll_target(target):
    print(f"\n{'='*60}")
    print(f"Target : {target['name']}  ({target['ip']})")
    print(f"{'='*60}")
    for mib_name, oid in MIBS.items():
        print(f"\n--- {mib_name} ({oid}) ---")
        rows = await snmp_walk(target["ip"], target["community"], oid)
        if not rows:
            print("  (no data)")
        for oid_str, val in rows:
            print(f"  {oid_str} = {val}")


async def main():
    for target in TARGETS:
        await poll_target(target)


if __name__ == "__main__":
    asyncio.run(main())
