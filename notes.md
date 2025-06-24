# 🧠 Rogue ONT Detection - Backend Notes

## 🔧 Environment

- CMS Version: `15.01.772`
- Target platform: Calix E7-20 (GPON)
- CLI Access via: SSH
- Python Framework: FastAPI
- SSH Library: **Netmiko** (instead of Paramiko)

---

## 🎯 Goals

- Identify rogue ONTs on overloaded PONs.
- Automate collection of:
  - `missedBursts`
  - `bipErrors`
  - `crcErrors`
  - Optical levels (Tx/Rx)
- Flag outliers to identify misbehaving ONTs.
- Eventually export provision data for potential SMx use.

---

## 🖥️ SSH Command Structure

```bash
ssh admin@<e7-20-mgmt-ip>
```

🔍 Core Diagnostic Commands for GPON & ONT Monitoring
📈 1. Show PON Port Errors
bash
Copy
Edit
show gpon pon-errors gpon-1/1/1
Displays: bipErrors, missedBursts, sdber, laser-on time, etc.

Use this to get overall health of the PON — useful for rogue detection.

🧠 2. Show ONT Statistics
bash
Copy
Edit
show interface gpon-1/1/1 ont 4 statistics
Displays ONT-specific counters:

CRC errors

Burst miss count

BIP errors

Optical receive/transmit levels

🔌 3. Show ONT Provisioning Info
bash
Copy
Edit
show gpon ont-provision gpon-1/1/1 4
Returns:

ONT model

Serial number

Admin status (enabled/disabled)

Provisioned services

🔎 4. Show ONT Alarm Status
bash
Copy
Edit
show alarm gpon-1/1/1 ont 4
View real-time or historical alarms for a given ONT.

💡 5. Show Optical Power on PON
bash
Copy
Edit
show gpon ont-optical gpon-1/1/1
Lists receive/transmit optical levels for all ONTs on a PON.

🧾 6. Show System Version
bash
Copy
Edit
show version
Useful for logging and tracking which firmware you’re hitting with each query.

🚨 Bonus Commands for Deeper Debugging (Optional)
Command Purpose
show interface gpon-1/1/1 Status summary of all ONTs
show gpon serial-number Lists serial numbers of active ONTs
show system clock Time drift detection for time-sensitive logs
show user Useful if you need to audit who is logged in or accessing the CLI

🛠️ Suggested FastAPI Route Mapping
Route Command
GET /e7/{ip}/pon-errors/{slot}/{port} show gpon pon-errors gpon-{slot}/{port}
GET /e7/{ip}/ont-stats/{slot}/{port}/{ont_id} show interface gpon-{slot}/{port} ont {ont_id} statistics
GET /e7/{ip}/ont-provision/{slot}/{port}/{ont_id} show gpon ont-provision gpon-{slot}/{port} {ont_id}
GET /e7/{ip}/ont-optical/{slot}/{port} show gpon ont-optical gpon-{slot}/{port}
