# Qumulo Custom Storage Report

A Python service that polls the Qumulo REST API, writes storage metrics to InfluxDB, and emails a formatted Excel report on a monthly schedule. Designed to run continuously as a background service.

---

## Overview

The service performs two distinct functions on a single continuous loop:

1. **Metric collection** — Queries the Qumulo cluster at a configurable interval, then writes quota usage, SMB share counts, NFS export counts, and per-share detail records to InfluxDB. These metrics can be visualised in Grafana.

2. **Monthly report** — On a configured day and hour each month, assembles an Excel spreadsheet from the same Qumulo data and emails it to one or more recipients via SMTP.

All configuration lives in `config.toml`. There are no databases, containers, or external schedulers required.

---

## Requirements

- Python 3.11 or later (uses the built-in `tomllib` module)
- Access to a Qumulo cluster REST API (bearer token auth)
- An InfluxDB 2.x instance
- An SMTP relay or mail server

### Install dependencies

```bash
pip install -r requirements.txt
```

Dependencies: `requests`, `influxdb-client`, `openpyxl`.

---

## Configuration

All settings are in `config.toml`, located in the same directory as the script.

### `[[clusters]]`

Repeat this block once per Qumulo cluster. Each block must have a unique `cluster_name`.

| Key | Description |
|---|---|
| `host` | Management IP or hostname of the Qumulo cluster (used for REST API calls) |
| `port` | API port (default: `8000`) |
| `token` | Qumulo bearer token |
| `cluster_name` | Friendly name used as a tag in InfluxDB and as the sheet name in the Excel report |

### `[clusters.tenant_hosts]`

Nested inside each `[[clusters]]` block. Maps each tenant name (as it appears in the Qumulo API) to the FQDN used in share paths. The service resolves tenant names at runtime via `GET /v1/multitenancy/tenants/`.

Add one entry per tenant. The key must match the tenant name exactly as returned by the API.

```toml
[[clusters]]
host         = "10.0.0.1"
port         = 8000
token        = "access-v1:..."
cluster_name = "prod-cluster"

[clusters.tenant_hosts]
"Default"        = "nas01.yourdomain.com"
"Finance_Tenant" = "nas02.yourdomain.com"

# Add more clusters by repeating the [[clusters]] block:
# [[clusters]]
# host         = "10.0.0.2"
# token        = "access-v1:..."
# cluster_name = "dr-cluster"
# [clusters.tenant_hosts]
# "Default" = "nas03.yourdomain.com"
```

SMB share paths are formatted as `\\FQDN\share_name`. NFS export paths are formatted as `FQDN:/export_path`. If a tenant name has no matching entry, the path uses `unknown-tenant`.

### `[influxdb]`

| Key | Description |
|---|---|
| `url` | InfluxDB URL including port, e.g. `http://10.0.0.1:8086` |
| `token` | InfluxDB API token |
| `org` | InfluxDB organisation ID (not name) |
| `bucket` | Destination bucket name, e.g. `Qumulo storage reporting` |

To find your InfluxDB organisation ID: in the InfluxDB UI go to **Organisation Settings** and copy the ID shown there (not the display name).

### `[smtp]`

| Key | Description |
|---|---|
| `host` | SMTP server hostname |
| `port` | SMTP port |
| `use_tls` | `true` to upgrade the connection with STARTTLS; `false` for plain (e.g. port 25 internal relay) |
| `from_addr` | Sender address shown in the email |
| `to_addrs` | List of recipient addresses, e.g. `["user1@example.com", "user2@example.com"]` |
| `username` | Optional — omit for unauthenticated relays |
| `password` | Optional — omit for unauthenticated relays |

### `[report]`

| Key | Description |
|---|---|
| `collection_interval_minutes` | How often to poll the Qumulo API and write to InfluxDB |
| `schedule_day` | Day of the month (1–28) to send the monthly email report |
| `schedule_hour` | Hour of the day (0–23, 24-hour clock) to send the report |
| `title` | Display name used in the email subject and body |

---

## Running the service

```bash
# Start the service (runs continuously)
python storage_report.py

# Use a specific config file
python storage_report.py --config /etc/qumulo/config.toml
```

The service logs to stdout in the format `YYYY-MM-DD HH:MM:SS  LEVEL  message`. It handles `SIGTERM` and `SIGINT` for graceful shutdown.

---

## CLI options

### `--dry-run`

Skips all InfluxDB writes. When combined with `--send-report`, saves the Excel file to disk next to the script instead of emailing it. Useful for verifying output before enabling live writes.

```bash
# Run the service without writing to InfluxDB
python storage_report.py --dry-run

# Generate the Excel file locally without sending email
python storage_report.py --send-report --dry-run
```

### `--send-report`

Builds and sends the monthly Excel report immediately, then exits. Does not start the service loop. Combine with `--dry-run` to save the file locally instead of emailing.

```bash
python storage_report.py --send-report
```

### `--test`

Runs the embedded unit test suite without requiring a cluster, InfluxDB, or config file. Exits with code 0 on success, 1 on failure.

```bash
python storage_report.py --test
```

### `--test-influx`

Runs an end-to-end integration test using the real cluster and InfluxDB credentials from `config.toml`. Writes data to a separate test bucket (default name: `test-qumulo-reporting`) so that production data is not affected.

```bash
python storage_report.py --test-influx

# Use a custom test bucket name
python storage_report.py --test-influx --test-bucket "my-test-bucket"
```

### `--test-influx-cleanup`

Deletes the test bucket created by `--test-influx`.

```bash
python storage_report.py --test-influx-cleanup
python storage_report.py --test-influx-cleanup --test-bucket "my-test-bucket"
```

---

## Excel report format

The report is a workbook named `storage_report_YYYY-MM.xlsx` with one sheet per configured cluster, each sheet named after its `cluster_name`. If all clusters fail to return data the workbook contains a single "No Data" sheet.

- **Row 1** — Column headers with blue fill and bold white text.
- **Row 2 onward** — One row per SMB share, followed by one row per NFS export.

**Columns:**

| Column | Description |
|---|---|
| Share Path / NFS Export | Full UNC path (SMB) or `FQDN:/path` (NFS) |
| SMB/NFS | Protocol type |
| Filesystem Path | Underlying filesystem path on the cluster |
| Quota Allocated | Quota limit in human-readable units (TB/GB/MB/B). Empty if no quota exists; `0B` if quota is set to zero |
| Quota Used | Current usage in the same format |
| Owner | From file user-metadata key `Owner` |
| Team | From file user-metadata key `Team` |
| SNOW Request | From file user-metadata key `SNOW Request` |
| Speedtype | From file user-metadata key `Speedtype` |

Quota data is joined to each row by matching the share's filesystem path against the quota path. File metadata is fetched per filesystem path from the Qumulo user-metadata API (`/v1/files/{path}/user-metadata/generic/`). Metadata values are base64-decoded from the API response.

---

## InfluxDB measurements

Four measurements are written on each collection cycle:

| Measurement | Tags | Fields |
|---|---|---|
| `storage_quota` | `cluster`, `path` | `limit_bytes`, `used_bytes`, `used_pct` |
| `storage_smb` | `cluster`, `tenant` | `share_count` |
| `storage_nfs` | `cluster`, `tenant` | `export_count` |
| `storage_share_detail` | `cluster`, `tenant`, `share_path` | All Excel columns as string fields |

All points use second-precision timestamps (`WritePrecision.S`).

`storage_quota`, `storage_smb`, and `storage_nfs` are suitable for time-series panels in Grafana. `storage_share_detail` holds the full per-share data for table visualisation.

### Grafana dashboard

A ready-to-import dashboard is included at `grafana-dashboard.json`. It provides a table panel matching the Excel report layout, with a cluster dropdown populated from your InfluxDB data.

**To import:**

1. In Grafana, go to **Dashboards → Import**.
2. Click **Upload dashboard JSON file** and select `grafana-dashboard.json`.
3. When prompted, select your InfluxDB data source (must be configured with **Flux** query language).
4. Click **Import**.

The cluster dropdown is populated automatically from the `cluster` tag in InfluxDB, which matches the `cluster_name` values in your `config.toml`.

> **Note:** The dashboard queries `range(start: -2h)` and takes the `last()` point per share. Set the dashboard refresh interval to match your `collection_interval_minutes` so the table stays current.

---

## Code structure

Everything is in a single file, `storage_report.py`.

| Class / function | Responsibility |
|---|---|
| `QumuloClient` | Wraps all Qumulo REST API calls. Uses bearer token auth and `verify=False` for self-signed certificates. |
| `InfluxWriter` | Writes `Point` records to InfluxDB synchronously. |
| `assemble_report_rows()` | Joins SMB/NFS entries with quota data and file metadata into a flat list of row dicts. |
| `build_excel_report()` | Builds the `.xlsx` workbook using `openpyxl`. |
| `ReportMailer` | Constructs and sends the `EmailMessage` with the spreadsheet attached. |
| `AppConfig` (and sub-dataclasses) | Typed configuration loaded from `config.toml` via `tomllib`. |
| `run_service()` | Main loop — collects metrics on interval, sends report on schedule. |
| `_run_unit_tests()` | Runs all embedded `_Test*` test cases. |
| `_run_influx_integration_test()` | End-to-end test against real infrastructure. |

### Data flow

```
Qumulo API  →  QumuloClient  ──┬──  InfluxWriter  →  InfluxDB
                                └──  assemble_report_rows()
                                            │
                                    build_excel_report()  →  ReportMailer  →  SMTP
```

### Tenant resolution

The Qumulo API returns a numeric `tenant_id` on every share and export. At runtime, the service resolves these to names via `GET /v1/multitenancy/tenants/`, then looks up the FQDN from `[tenant_hosts]` in config. This means the config maps human-readable tenant names to FQDNs, and the API mapping from IDs to names is fetched dynamically.

### Quota matching

Quota data is indexed by filesystem path (trailing slashes stripped). Each share and export row looks up its `fs_path` in this index. If no quota exists, the quota columns are left empty. If a quota exists with a limit of zero, the columns display `0B` rather than being blank.

---

## Running as a system service (Ubuntu 22.04+)

### 1. Create a service account

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin qumulo-report
```

### 2. Install the service files

```bash
sudo mkdir -p /opt/qumulo/qumulo-report
sudo cp storage_report.py config.toml requirements.txt /opt/qumulo/qumulo-report/
sudo chown -R root:qumulo-report /opt/qumulo/qumulo-report
```

### 3. Install Python dependencies

```bash
sudo apt install -y python3-pip
sudo pip3 install -r /opt/qumulo/qumulo-report/requirements.txt
```

### 4. Restrict config file permissions

`config.toml` contains bearer tokens in plaintext. Set it so only root (owner) and the service account (group) can read it — no other users:

```bash
sudo chmod 640 /opt/qumulo/qumulo-report/config.toml
```

### 5. Create the systemd unit file

```bash
sudo tee /etc/systemd/system/qumulo-report.service > /dev/null <<'EOF'
[Unit]
Description=Qumulo Storage Report Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/qumulo/qumulo-report/storage_report.py
WorkingDirectory=/opt/qumulo/qumulo-report
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
```

### 6. Enable and start the service

```bash
sudo systemctl daemon-reload
sudo systemctl enable qumulo-report
sudo systemctl start qumulo-report
```

### 7. Verify it is running

```bash
sudo systemctl status qumulo-report
sudo journalctl -u qumulo-report -f
```

### Applying config or code changes

Because the service reads `config.toml` and `storage_report.py` from disk at startup, any change takes effect with a simple restart — no reinstall or redeployment needed:

```bash
# Edit config or replace the script file, then:
sudo systemctl restart qumulo-report
```

For config-only changes you can also do a reload-aware restart that waits for the current collection cycle to finish:

```bash
sudo systemctl kill --kill-who=main --signal=SIGTERM qumulo-report
# systemd will automatically restart it via Restart=on-failure
```

### Uninstall

```bash
sudo systemctl stop qumulo-report
sudo systemctl disable qumulo-report
sudo rm /etc/systemd/system/qumulo-report.service
sudo systemctl daemon-reload
sudo rm -rf /opt/qumulo/qumulo-report
```

---

### Windows

On Windows, the script can be wrapped with NSSM or run as a scheduled task. Note that on Windows, SIGTERM cannot be delivered by the OS — only Ctrl+C (SIGINT) or a hard kill will stop the process. NSSM's stop command sends SIGTERM, which will not be handled gracefully; configure NSSM with a suitable shutdown timeout so it falls back to a hard terminate.

### Config file permissions

`config.toml` contains bearer tokens for both Qumulo and InfluxDB in plaintext. On Linux/macOS, restrict it so only root and the service group can read it:

```bash
sudo chown root:qumulo-report config.toml
sudo chmod 640 config.toml
```

The script will log a warning at startup if the file is readable by others outside the owner/group.

---

## Notes

- The Qumulo API uses self-signed TLS certificates. The `urllib3` InsecureRequestWarning is suppressed only within `QumuloClient` API calls using `warnings.catch_warnings()`, not process-wide.
- The InfluxDB `org` field in config must be the organisation ID (a hex string), not the display name.
- SMTP authentication (`username` / `password`) is optional. Omit both keys from the config for unauthenticated relays (e.g. an internal mail relay on port 25). Set `use_tls = true` to issue a STARTTLS upgrade after connecting; leave it `false` for plain connections.
- The monthly report fires once per calendar month. After sending, the service writes a `.report_state` file next to the script recording the year and month (`YYYYMM`). If the service is restarted on the same day it already sent the report, it reads this file on startup and will not send again until the next scheduled month. If the state file is deleted or unreadable, the service treats no report as having been sent and will re-send on the next scheduled window.
