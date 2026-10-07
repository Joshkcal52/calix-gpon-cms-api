# GPON Provisioning API for Calix CMS

A REST and Kafka front end for legacy Calix GPON: what the SMx API is for XGS, this is for CMS. A billing system such as Sonar calls it (or publishes an event) when a customer signs up, and the configuration is pushed to the legacy ONT through the CMS northbound interface, with no screen-scraping.

It configures each ONT piece by piece (the ONT, each Ethernet port, each service, on a plain port or on the built-in router of ONTs such as the 844G), runs the rest of the lifecycle as repeat-safe jobs, exports a network for a GPON-to-XGS cutover in SMx's terms, plans the SMx requests as a dry run, compares saved snapshots with CMS, and has a read-only check for a real CMS.

**Start with [HANDOFF.md](HANDOFF.md)**: what this is, a five-minute tour, what is proven and what is not. The design is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); a multi-process demo is in [deploy/demo/README.md](deploy/demo/README.md).

The repository is named `rogue-api` after its first feature, rogue ONT detection over SSH. That lives on in `/api/v1` and is described further down.

## 🚀 Quick Start

### 1. Activate Virtual Environment

```bash
# On macOS/Linux
source venv/bin/activate

# On Windows
venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the example environment file and update it with your settings:

```bash
cp env_example.txt .env
```

Edit `.env` with your actual SSH credentials and settings.

No hardware? Set `MOCK_MODE=true` in `.env` and skip the credentials. See [Mock Mode](#-mock-mode-no-hardware-needed).

### 4. Run the Application

```bash
# Method 1: Using Python directly
python main.py

# Method 2: Using uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 5. Access the API

- **API Documentation**: http://localhost:8000/docs
- **Alternative Docs**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health
- **API Status**: http://localhost:8000/api/v1/status

## 📁 Project Structure

```
rogue-api/
├── app/                          # Main application package
│   ├── __init__.py
│   ├── models/                   # Pydantic data models
│   │   ├── ont_model.py         # ONT data models
│   │   ├── ssh_model.py         # SSH connection models
│   │   └── hp_model.py          # HP switch interface models
│   ├── routes/                   # API route handlers
│   │   ├── ssh_routes.py        # SSH management endpoints
│   │   ├── ont_routes.py        # ONT management endpoints
│   │   └── hp_routes.py         # HP switch endpoints
│   ├── controller/               # Business logic
│   │   ├── ssh_controller.py    # SSH connection service (real or mock)
│   │   ├── mock_e7.py           # Simulated E7-20 used by MOCK_MODE
│   │   ├── ont_controller.py    # ONT data service
│   │   ├── hp_controller.py     # HP switch service
│   │   ├── provisioning_controller.py  # v2: ONT activation workflow (ONT, ports, services)
│   │   ├── lifecycle_controller.py     # v2: changes to existing ONTs, ports and services
│   │   └── job_controller.py    # v2: job status and event log
│   ├── nbi/                      # v2: CMS gateway, cms-nbi-client backend, mock CMS, mock NBI server,
│   │                             #     router_side.py (the unverified router-side addressing)
│   ├── legacy/                   # v2: signup and cutover events, export, SMx plan, snapshots and compare
│   ├── tools/cms_check.py        # v2: read-only check against a real CMS
│   ├── messaging/                # v2: command queue (in-memory or Kafka)
│   ├── worker/                   # v2: background worker (python -m app.worker)
│   └── utils/                    # Utility functions
│       └── helpers.py           # Helper functions
├── deploy/                      # Dockerfile, demo (Compose), local Kafka, Kubernetes manifests
├── docs/ARCHITECTURE.md         # v2 design
├── tests/                       # Pytest tests
├── main.py                      # FastAPI application entry point
├── config.py                    # Configuration settings
├── conftest.py                  # Pytest fixtures
├── requirements.txt             # Python dependencies
├── requirements-kafka.txt       # Extra: Kafka queue
├── requirements-cms.txt         # Extra: real CMS via cms-nbi-client
├── requirements-test.txt        # Everything the full test suite uses
├── env_example.txt              # Environment variables template
├── HANDOFF.md                   # Start here
├── README.md                    # This file
├── notes.md                     # Project notes
└── venv/                        # Virtual environment
```

## 🔧 Configuration

The application uses Pydantic settings for configuration management. Key settings include:

- **Mock Mode**: `MOCK_MODE=true` simulates the E7-20 instead of opening SSH
- **SSH Connection**: Host, username, password, port, timeout
- **API Settings**: Host, port, reload mode
- **Logging**: Log level configuration

## 🧪 Mock Mode (no hardware needed)

Set `MOCK_MODE=true` in `.env` (or run `MOCK_MODE=true python main.py`) to develop without an E7-20.

- `POST /api/v1/ssh/connect` accepts any host, username and password and opens no network connection.
- Commands are answered by a simulated chassis in `app/controller/mock_e7.py`: 14 ONTs on two PONs, with a rogue ONT (`1/1/1:4`) on PON `1/1/1`.
- `/api/v1/status` and `/api/v1/ssh/status` report `"mock_mode": true`, and `show version` starts with a `MOCK DEVICE` banner, so simulated data is never mistaken for the real network.

Supported commands: the ones the controllers send (`show version`, `show interface`, `show ont info [id]`, `show ont performance <id>`, `show pon port status [port]`) and the ones in `notes.md` (`show gpon pon-errors gpon-1/1/1`, `show interface gpon-1/1/1 ont 4 statistics`, `show gpon ont-optical gpon-1/1/1`, and so on). Anything else returns `% Invalid command`.

The mock output is an approximation and has not been compared with a real E7-20. Capture real CLI output and update `mock_e7.py` before trusting a parser against live equipment.

## 🧩 v2: CMS provisioning, lifecycle and cutover

The `/api/v2` routes provision and change ONTs through the Calix CMS northbound interface, queue changes for background workers, stream progress live, export networks for XGS cutovers and plan the SMx side as a dry run. Every `/api/v2` route needs an API key (`API_KEYS`); with `MOCK_MODE=true` and no keys set they are open. The design, delivery rules and what is not yet verified are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); [HANDOFF.md](HANDOFF.md) has a five-minute tour.

For a real CMS, set `CMS_HOST`, `CMS_USERNAME` and `CMS_PASSWORD`, install `requirements-cms.txt`, and run `python -m app.tools.cms_check --network <name>` first. This has only been run against the mock CMS server in this repository, never a real CMS.

## 📚 API Endpoints

### SSH Management (`/api/v1/ssh`)

- `POST /connect` - Establish SSH connection to Calix E7-20
- `POST /disconnect` - Disconnect SSH session
- `GET /status` - Get SSH connection status
- `POST /command` - Execute CLI command
- `GET /test-connection` - Test SSH connection

### ONT Management (`/api/v1/onts`)

- `GET /` - Get all ONTs
- `GET /{ont_id}` - Get specific ONT by ID
- `GET /{ont_id}/performance` - Get ONT performance metrics
- `GET /pon/status` - Get PON port status
- `POST /analyze-rogue` - Analyze ONTs for rogue behavior
- `GET /summary` - Get ONT summary statistics
- `GET /search` - Search and filter ONTs

### Provisioning (`/api/v2`)

- `GET /nbi/health` - CMS backend and circuit breaker state
- `GET /networks/{network}/onts/discovered` - ONTs not provisioned yet
- `GET /networks/{network}/onts/{ont_id}` - State of one ONT
- `POST /networks/{network}/onts/status-check` - Read up to 200 ONTs in parallel
- `POST /networks/{network}/activations` - Activate an ONT with its ports and services (202, runs as a job)

### ONT lifecycle (`/api/v2/networks/{network}`)

Ports are `eth1`..`eth8`, or `rg1` for the router side. Changes answer 202 and run as repeat-safe jobs.

- `GET /ont-profiles`, `GET /vlans` - What exists on the network
- `GET /onts/{ont_id}/ports` - Ethernet ports (with bridge/router mode) and the router side
- `POST /onts/{ont_id}/ports/{port}/enable` and `/disable`; `PATCH /onts/{ont_id}/ports/{port}`
- `POST /onts/{ont_id}/ports/{port}/services/{n}/enable` and `/disable`; `PATCH` and `DELETE` on the same path
- `DELETE /onts/{ont_id}?serial_number=...&force=false` - Delete an ONT

### XGS cutover, read-only (`/api/v2`)

- `GET /networks/{network}/export?pon=|card=&format=json|csv|snapshot` - The network in SMx terms
- `POST /networks/{network}/snapshot-compare` - What differs between a saved snapshot and CMS now
- `POST /smx/plans` - SMx requests for an export (dry run, nothing sent)
- `GET /networks/{network}/subscribers/{id}/smx-plan` - The same for one subscriber, read live

### Legacy GPON subscribers (`/api/v2/networks/{network}`)

- `POST /signups` - New subscriber: provision their ONT and services on CMS (202, runs as a job)
- `POST /cutovers` - Hand a subscriber over to XGS: read CMS, publish a migration request (202, runs as a job)

### Jobs (`/api/v2/jobs`)

- `GET /` - Recent jobs
- `GET /{job_id}` - Job status, result or error
- `GET /{job_id}/events` - Live progress (Server-Sent Events)

### System Endpoints

- `GET /` - Root endpoint with API information
- `GET /health` - Health check
- `GET /api/v1/status` - Detailed API status

## 🏗️ Architecture Overview

### Models (`app/models/`)

- **Data Validation**: Pydantic models ensure data integrity
- **Type Safety**: Strong typing for all API inputs/outputs
- **Documentation**: Auto-generated API documentation

### Routes (`app/routes/`)

- **Request Handling**: HTTP endpoint definitions
- **Input Validation**: Automatic validation using Pydantic models
- **Error Handling**: Consistent error responses
- **Authentication**: Future-ready for authentication

### Services (`app/services/`)

- **Business Logic**: Core application functionality
- **SSH Management**: Connection handling and command execution
- **Data Processing**: ONT data collection and analysis
- **Caching**: Performance optimization with data caching

### Utils (`app/utils/`)

- **Helper Functions**: Common utility functions
- **Data Parsing**: CLI output parsing utilities
- **Validation**: Input validation helpers
- **Formatting**: Data formatting utilities

## 🧪 Testing

Run the full suite (it was run on Python 3.10 and 3.12):

```bash
pip install -r requirements-test.txt
pytest
```

## 🔒 Security Notes

- Never commit `.env` files with real credentials
- Use environment variables for sensitive data
- Consider using SSH keys instead of passwords
- Set appropriate CORS settings for production
- All CLI commands are sanitized to prevent injection

## 🐛 Troubleshooting

### Common Issues

1. **Port already in use**: Change the port in `.env` or kill the process using port 8000
2. **SSH connection failed**: Verify credentials and network connectivity
3. **Import errors**: Ensure virtual environment is activated and dependencies are installed
4. **Module not found**: Check that all `__init__.py` files are present

### Logs

Check the console output for detailed logs. The application uses Python's logging module with INFO level by default.

## 🔄 Development Workflow

1. **Make Changes**: Edit files in the appropriate modules
2. **Auto-reload**: Server automatically restarts on file changes
3. **Test Endpoints**: Use the interactive docs at `/docs`
4. **Check Logs**: Monitor console output for errors and info

## 📈 Next Steps

1. **Implement Parsing**: Update parsing functions for actual Calix CLI output
2. **Add Authentication**: Implement user authentication and authorization
3. **Database Integration**: Add persistent storage for historical data
4. **Real-time Monitoring**: Implement WebSocket connections for real-time updates
5. **SMx client**: the export and the SMx plan exist; sending to SMx is the dev team's (see HANDOFF.md)
