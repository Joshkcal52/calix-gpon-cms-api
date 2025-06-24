# Rogue ONT Detection API

A FastAPI application for detecting rogue ONTs on Calix E7-20 GPON systems.

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
│   │   ├── __init__.py
│   │   ├── ont.py               # ONT data models
│   │   └── ssh.py               # SSH connection models
│   ├── routes/                   # API route handlers
│   │   ├── __init__.py
│   │   ├── ssh.py               # SSH management endpoints
│   │   └── ont.py               # ONT management endpoints
│   ├── services/                 # Business logic services
│   │   ├── __init__.py
│   │   ├── ssh_service.py       # SSH connection service
│   │   └── ont_service.py       # ONT data service
│   └── utils/                    # Utility functions
│       ├── __init__.py
│       └── helpers.py           # Helper functions
├── main.py                      # FastAPI application entry point
├── config.py                    # Configuration settings
├── requirements.txt             # Python dependencies
├── env_example.txt              # Environment variables template
├── README.md                    # This file
├── notes.md                     # Project notes
└── venv/                        # Virtual environment
```

## 🔧 Configuration

The application uses Pydantic settings for configuration management. Key settings include:

- **SSH Connection**: Host, username, password, port, timeout
- **API Settings**: Host, port, reload mode
- **Logging**: Log level configuration

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

Run tests with pytest:

```bash
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
5. **Export Functionality**: Add data export for SMx integration
