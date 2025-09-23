# Punjab Data Analysis System

A containerized data analysis pipeline for processing Punjab property tax data using Docker and Apache Airflow. This system extracts data from PostgreSQL databases and generates comprehensive analytical reports.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Environment                       │
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │   Airflow       │    │   Analysis      │                │
│  │   Services      │    │   Container     │                │
│  │                 │    │                 │                │
│  │ ┌─────────────┐ │    │ ┌─────────────┐ │                │
│  │ │  Scheduler  │ │────┼▶│ punjab-     │ │                │
│  │ │  Webserver  │ │    │ │ analysis:   │ │                │
│  │ │  Database   │ │    │ │ v3.0.0      │ │                │
│  │ └─────────────┘ │    │ └─────────────┘ │                │
│  └─────────────────┘    └─────────────────┘                │
│                                                             │
│  Data Flow:                                                 │
│  1. Airflow triggers DockerOperator                        │
│  2. Container extracts data from PostgreSQL → CSV files    │
│  3. Container analyzes CSV data → generates reports        │
│  4. Container auto-removes after completion                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- **Docker** and **Docker Compose**
- **4GB RAM** and **2 CPU cores** minimum
- **PostgreSQL database** with Punjab property tax data

### 1. Clone and Setup
```bash
git clone <repository-url>
cd punjab-analysis-docker-complete
```

### 2. Configure Database Connection
Edit `secrets/db_config.yaml` with your database details:
```yaml
database:
  host: your-postgres-host
  port: 5432
  name: your-database
  user: your-username
  password: your-password
```

### 3. Build the Analysis Image
```bash
cd src/
./build.sh
```

### 4. Start Airflow Services
```bash
cd ..
export AIRFLOW_UID=$(id -u)
docker-compose up -d
```

### 5. Access Airflow UI
- Open http://localhost:8082
- Login: `admin` / `admin123`
- Enable DAG: `punjab_analysis_docker_pipeline`

### 6. Run Analysis
**Via Airflow UI:**
- Navigate to DAGs → `punjab_analysis_docker_pipeline`
- Click "Trigger DAG"
- Configure tenant_ids: `["pb.adampur", "pb.samana"]`

**Via CLI:**
```bash
# Process single tenant
docker-compose exec airflow-scheduler airflow dags trigger \
  punjab_analysis_docker_pipeline \
  -c '{"tenant_ids": ["pb.adampur"]}'

# Process multiple tenants
docker-compose exec airflow-scheduler airflow dags trigger \
  punjab_analysis_docker_pipeline \
  -c '{"tenant_ids": ["pb.adampur", "pb.samana", "pb.amloh"]}'
```

## 📊 How It Works

### Data Processing Pipeline
1. **Extraction Phase**: Container connects to PostgreSQL and extracts property, owner, and unit data in chunks
2. **Analysis Phase**: Container processes CSV files and generates comprehensive reports
3. **Cleanup Phase**: Container removes old data files to save disk space

### Supported Tenants
- `pb.adampur` - Adampur region
- `pb.samana` - Samana region
- `pb.amloh` - Amloh region

### Output Files
Analysis reports are saved to `local-reports/` directory:
```
local-reports/
├── Punjab_Data_Analysis_adampur_20250923.csv
├── Punjab_Data_Analysis_samana_20250923.csv
└── Punjab_Data_Analysis_amloh_20250923.csv
```

## 🛠️ Manual Testing

### Test Individual Components
```bash
# Test extraction
docker run --rm \
  --network host \
  -v $(pwd)/data:/data \
  -v $(pwd)/secrets:/run/secrets:ro \
  -e MODE=extract \
  -e TENANT_ID=pb.adampur \
  -e CONFIG_PATH=/run/secrets/db_config.yaml \
  punjab-analysis:v3.0.0

# Test analysis
docker run --rm \
  --network host \
  -v $(pwd)/data:/data \
  -v $(pwd)/local-reports:/output \
  -v $(pwd)/secrets:/run/secrets:ro \
  -e MODE=analyze \
  -e TENANT_ID=pb.adampur \
  punjab-analysis:v3.0.0
```

## ⚙️ Configuration

### Environment Variables
- **`MODE`**: `extract` or `analyze`
- **`TENANT_ID`**: Punjab region identifier
- **`CONFIG_PATH`**: Path to database configuration file
- **`DATA_DIR`**: Directory for CSV data storage
- **`OUTPUT_DIR`**: Directory for analysis reports

### Database Configuration
The `secrets/db_config.yaml` file supports:
- Environment-specific configurations (development, staging, production)
- Tenant-specific settings (chunk sizes, priorities)
- Connection pooling and timeout settings

Example configuration:
```yaml
database:
  host: localhost
  port: 5432
  name: airflowdb
  user: postgres
  password: postgres

settings:
  chunk_size: 50000
  debug_mode: true
  data_dir: /data
  output_dir: /output

tenants:
  pb.adampur:
    chunk_size: 25000
    priority: high
```

## 🔍 Troubleshooting

### Common Issues

**Container fails to start:**
```bash
# Check Docker status
docker ps
docker images | grep punjab-analysis

# Rebuild if needed
cd src/ && ./build.sh
```

**Database connection fails:**
```bash
# Verify configuration
cat secrets/db_config.yaml

# Test connectivity
docker run --rm --network host postgres:13 \
  psql -h localhost -p 5432 -U postgres -d airflowdb
```

**Airflow DAG not visible:**
```bash
# Check DAG syntax
python airflow/dags/punjab_analysis_docker_dag.py

# Check Airflow logs
docker-compose logs airflow-scheduler
```

**Volume mount issues:**
- Ensure data directories exist: `mkdir -p data local-reports`
- Check file permissions: `chmod 755 data local-reports secrets`

### Logs and Monitoring
- **Airflow UI**: http://localhost:8082 for task status and logs
- **Container logs**: Check individual task logs in Airflow UI
- **System logs**: `docker-compose logs -f`

## 📁 Project Structure

```
punjab-analysis-docker-complete/
├── src/                          # Application source code
│   ├── main.py                   # Entry point router
│   ├── extract_data.py           # PostgreSQL data extraction
│   ├── analyze_data.py           # CSV data analysis
│   ├── config_loader.py          # Configuration management
│   ├── cleanup_data.py           # Data cleanup utilities
│   ├── Dockerfile                # Container definition
│   ├── requirements.txt          # Python dependencies
│   └── build.sh                  # Build script
├── airflow/                      # Airflow configuration
│   └── dags/
│       └── punjab_analysis_docker_dag.py  # Main workflow DAG
├── secrets/                      # Configuration files
│   └── db_config.yaml           # Database connection settings
├── data/                         # CSV data storage (auto-created)
├── local-reports/               # Analysis output (auto-created)
├── docker-compose.yml           # Airflow services definition
└── README.md                    # This file
```

## 🔒 Security Features

- **No hardcoded credentials** - All sensitive data in external config files
- **Non-root container execution** - Enhanced security posture
- **Read-only secret mounts** - Prevents accidental credential modification
- **Automatic container cleanup** - Reduces attack surface
- **Network isolation** - Containers only access required services

## 🎯 Key Features

- **Multi-tenant Support**: Process multiple Punjab regions in parallel
- **Scalable Processing**: Configurable chunk sizes for large datasets
- **Automatic Retry Logic**: Built-in failure recovery with exponential backoff
- **Resource Efficiency**: Containers auto-remove after task completion
- **Comprehensive Monitoring**: Full visibility through Airflow UI
- **Flexible Configuration**: Environment and tenant-specific settings

## 📈 Performance

- **Extraction Speed**: ~50,000 records per chunk (configurable)
- **Memory Usage**: ~500MB per container during processing
- **Storage**: Temporary CSV files, final reports retained
- **Concurrent Processing**: Support for multiple tenants simultaneously

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-feature`
3. Make changes and test thoroughly
4. Submit pull request with detailed description

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Version**: v3.0.0
**Architecture**: Docker Containers with Airflow Orchestration
**Last Updated**: September 2025