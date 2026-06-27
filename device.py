import random
import re
import os
from dotenv import load_dotenv
from jumpssh import SSHSession
from datetime import datetime, timezone

load_dotenv()

class Device:
    """A single Wi-Fi device that emits telemetry readings.

    __init__ holds the STATIC identity/config of the device.
    generate_reading() produces ONE dynamic telemetry event (a dict matching
    schema.json): fresh metrics + timestamp, plus a buffer_event label that is
    CORRELATED with the metrics — so the ML model later has real signal to learn,
    not random noise.
    """

    def __init__(self, device_id, device_mac, freq_band, channel_num):
        self.device_id = device_id
        self.device_mac = device_mac
        self.freq_band = freq_band
        self.channel_num = channel_num

    def generate_reading(self):
        # --- dynamic per-reading metrics (realistic-ish ranges; tune to taste) ---
        rtt_ms = round(random.uniform(20, 300), 1)
        link_budget_db = round(random.uniform(2.0, 15.0), 1)
        avg_link_utilization_pct = round(random.uniform(10, 100), 1)
        cwnd_bytes = random.randint(10_000, 300_000)
        rwnd_bytes = random.randint(65_536, 262_144)

        # --- derive the label: buffering gets MORE likely as the link degrades ---
        # Count how many signals look "bad", then convert that into a probability.
        # (Probabilistic, not a hard rule, so the data isn't trivially separable.)
        bad_signals = (
            (rtt_ms > 150)
            + (link_budget_db < 6.0)
            + (avg_link_utilization_pct > 80)
            + (cwnd_bytes < 50_000)
        )
        buffer_prob = min(0.95, 0.05 + 0.22 * bad_signals)  # 5% floor, +22% per bad signal
        buffer_event = random.random() < buffer_prob

        return {
            "device_id": self.device_id,
            "device_mac": self.device_mac,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "freq_band": self.freq_band,
            "channel_num": self.channel_num,
            "rtt_ms": rtt_ms,
            "link_budget_db": link_budget_db,
            "avg_link_utilization_pct": avg_link_utilization_pct,
            "cwnd_bytes": cwnd_bytes,
            "rwnd_bytes": rwnd_bytes,
            "buffer_event": buffer_event,
        }

def get_device_data(ip):
    session = SSHSession(host=ip,username=os.getenv("PI_USERNAME"),password=os.getenv("PI_PASSWORD")).open()

    def device_id(session):
        hostname = session.get_cmd_output('hostname')
        return hostname
    
    def device_mac(session):
        mac = session.get_cmd_output('cat /sys/class/net/wlan0/address')
        return mac
    
    def device_wifi_data(session):
        wifi_data = session.get_cmd_output('/usr/sbin/iw dev wlan0 info')
        m = re.search(r'channel (\d+) \((\d+) MHz\)', wifi_data)
        channel_num = int(m.group(1))
        freq_mhz = int(m.group(2))
        freq_band = "5GHz" if freq_mhz >= 5000 else "2.4GHz"
        
        return channel_num, freq_band
    
    channel, band = device_wifi_data(session)

    return {
        "device_id": device_id(session),
        "device_mac": device_mac(session),
        "freq_band": band,
        "channel_num": channel
    }