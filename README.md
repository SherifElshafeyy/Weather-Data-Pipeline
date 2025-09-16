# Weather Data Pipeline

A comprehensive **ETL data pipeline** that extracts weather data from the WeatherStack API, processes it through PostgreSQL, and transforms it using dbt for analytical reporting. The entire pipeline is orchestrated with Apache Airflow and containerized using Docker.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   WeatherStack  │    │   PostgreSQL    │    │       dbt       │    │    Reports      │
│      API        │───▶│   Raw Data      │───▶│  Transformations│───▶│   Analytics     │
│                 │    │                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                        ▲                        ▲                        ▲
         │                        │                        │                        │
    ┌─────────────────────────────────────────────────────────────────────────────────────┐
    │                           Apache Airflow Orchestration                              │
    │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
    │  │API Sensor   │ │Data Ingestion│ │dbt Transform│ │  dbt Tests  │ │  Cleanup    │ │
    │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ │
    └─────────────────────────────────────────────────────────────────────────────────────┘
                                    Docker Containerization
```

## 🛠️ Technology Stack

- **Orchestration**: Apache Airflow
- **Database**: PostgreSQL 14.19
- **Data Transformation**: dbt (Data Build Tool)
- **API**: WeatherStack API
- **Languages**: Python, SQL
- **Containerization**: Docker & Docker Compose
- **Environment**: Linux (WSL)

## 📁 Project Structure

```
weather-data-project/
├── airflow/
│   └── dags/
│       └── orchestrator.py          # Main Airflow DAG
├── api-request/
│   └── insert_record.py             # Weather data ingestion logic
├── dbt/
│   ├── my_project/
│   │   ├── dbt_project.yml          # dbt project configuration
│   │   ├── models/
│   │   │   ├── fact/
│   │   │   │   └── weather_data_cleansed.sql
│   │   │   └── mart/
│   │   │       ├── city_avg_data.sql
│   │   │       └── country_avg_data.sql
│   │   ├── sources/
│   │   │   ├── sources.yml          # Source table definitions
│   │   │   └── schema.yml           # Model tests & documentation
│   │   └── macros/
│   │       └── range_values.sql     # Custom dbt test macro
│   └── profiles.yml                 # dbt database connections
├── postgres/
│   ├── data/                        # PostgreSQL data volume
│   └── airflow_init.sql            # Database initialization
└── docker-compose.yml              # Container orchestration
```

## 🚀 Features

### **Data Pipeline Capabilities**
- **Real-time Weather Data**: Fetches current weather data from WeatherStack API for New York
- **Data Quality Assurance**: Implements dbt tests for data validation and quality checks
- **Automated Scheduling**: Runs every 45 minutes using Airflow scheduler
- **Error Handling**: Automatic cleanup of invalid data when tests fail
- **Deduplication**: Removes duplicate records using row numbering
- **Time Zone Handling**: Converts UTC timestamps to local time zones

### **Data Transformations**
- **Fact Layer**: Cleansed weather data with deduplication and timezone conversion
- **Mart Layer**: Aggregated daily averages by city and country
- **Custom Tests**: Temperature range validation using custom dbt macros
- **Data Lineage**: Full traceability through dbt's lineage graphs

### **Infrastructure**
- **Containerized Architecture**: All components run in Docker containers
- **Scalable Design**: Easy to extend for multiple cities or data sources
- **Network Isolation**: Custom Docker network for secure communication
- **Persistent Storage**: PostgreSQL data persistence across container restarts

## 📋 Prerequisites

- Docker and Docker Compose
- Linux environment (tested on WSL)
- WeatherStack API key (free tier available)
- 4GB+ RAM recommended

## 🔧 Installation & Setup

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd weather-data-project
```

### 2. Environment Configuration
Update the API key in `api-request/insert_record.py`:
```python
api_key = "your_weatherstack_api_key_here"
```

### 3. Start the Pipeline
```bash
# Start all services
docker-compose up -d

# Check container status
docker-compose ps
```

### 4. Initialize Airflow
The pipeline will automatically:
- Initialize PostgreSQL database
- Set up Airflow with standalone mode
- Create necessary schemas and tables

### 5. Access Services
- **Airflow UI**: http://localhost:8000
- **PostgreSQL**: localhost:5000 (host:port)
  - Database: `db`
  - User: `db_user`
  - Password: `db_password`

## 📊 Data Flow

### **1. Data Ingestion**
- Airflow sensor checks WeatherStack API availability
- Python script fetches current weather data for New York
- Raw data inserted into `dev.raw_weather_data` table with run_id tracking

### **2. Data Transformation (dbt)**
- **Fact Layer**: `weather_data_cleansed`
  - Deduplicates records using ROW_NUMBER()
  - Converts timestamps to local timezone
  - Filters to most recent record per city/time combination

- **Mart Layer**: Aggregation models
  - `city_avg_data`: Daily weather averages by city
  - `country_avg_data`: Daily weather averages by country

### **3. Data Quality Testing**
- **Unique/Not Null**: Ensures data integrity
- **Range Validation**: Temperature must be between -20°C and 80°C
- **Accepted Values**: Wind direction validation against compass values
- **Automatic Cleanup**: Failed test data is automatically removed

## 🧪 Data Quality & Testing

The pipeline implements comprehensive data quality checks:

### **Built-in dbt Tests**
```yaml
tests:
  - unique           # Ensures unique primary keys
  - not_null         # Validates required fields
  - accepted_values  # Validates wind direction values
```

### **Custom Tests**
- **Temperature Range**: Custom macro validates realistic temperature values
- **Run-time Cleanup**: Automatically removes data that fails quality tests

## 🔄 Pipeline Schedule

- **Frequency**: Every 45 minutes
- **Catchup**: Disabled (only processes current runs)
- **Retry Logic**: Built-in Airflow retry mechanisms
- **Monitoring**: Airflow UI provides complete pipeline visibility

## 📈 Usage Examples

### **Query Daily City Averages**
```sql
SELECT city, day, avg_temperature, avg_humidity 
FROM dev.city_avg_data 
WHERE day >= CURRENT_DATE - INTERVAL '7 days';
```

### **Monitor Data Quality**
```sql
SELECT run_id, COUNT(*) as records 
FROM dev.weather_data_cleansed 
GROUP BY run_id 
ORDER BY run_id DESC;
```

### **Check Raw vs Cleansed Data**
```sql
-- Raw data count
SELECT COUNT(*) FROM dev.raw_weather_data;

-- Cleansed data count (should be <= raw due to deduplication)
SELECT COUNT(*) FROM dev.weather_data_cleansed;
```

## 🐳 Docker Services

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| PostgreSQL | `postgres_container` | 5000:5432 | Data storage |
| Airflow | `airflow_container` | 8000:8080 | Orchestration |
| dbt | `dbt_container` | - | Data transformation |

## 🔍 Monitoring & Debugging

### **Airflow UI**
- Monitor DAG runs and task status
- View logs for troubleshooting
- Manual trigger capabilities

### **Database Queries**
```sql
-- Check latest data
SELECT * FROM dev.weather_data_cleansed ORDER BY inserted_at DESC LIMIT 5;

-- Monitor pipeline runs
SELECT run_id, COUNT(*), MAX(inserted_at) as last_run 
FROM dev.raw_weather_data 
GROUP BY run_id 
ORDER BY last_run DESC;
```

### **dbt Commands**
```bash
# Run transformations
docker exec dbt_container dbt run

# Run tests
docker exec dbt_container dbt test

# Generate documentation
docker exec dbt_container dbt docs generate
```

## 🔧 Customization Options

### **Adding More Cities**
Modify the API URL in `insert_record.py` to include multiple locations or make it configurable.

### **Additional Weather Metrics**
The WeatherStack API provides more data points (UV index, air quality) that can be easily added to the schema.

### **Different Aggregations**
Create new mart models for hourly, weekly, or monthly aggregations.

### **Data Sources**
Replace or supplement WeatherStack with other weather APIs or data sources.

## 🚨 Troubleshooting

### **Common Issues**

**1. Container Startup Issues**
```bash
# Check container logs
docker-compose logs [service_name]

# Restart services
docker-compose restart
```

**2. Database Connection Issues**
- Verify PostgreSQL container is running
- Check network connectivity between containers
- Validate database credentials

**3. dbt Test Failures**
- Review temperature data for unrealistic values
- Check wind direction values against accepted list
- Examine duplicate records

**4. API Issues**
- Verify WeatherStack API key is valid
- Check API rate limits
- Monitor API response status codes

## 📝 Development Notes

- **Mock Data**: Test functions available for pipeline testing without API calls
- **Run ID Tracking**: Each pipeline run is tracked for data lineage
- **Timezone Handling**: Automatic conversion from UTC to local timezones
- **Error Recovery**: Automatic cleanup of failed pipeline runs

## 🎯 Future Enhancements

- [ ] Multi-city data collection
- [ ] Historical data backfilling
- [ ] Real-time dashboards
- [ ] Data retention policies
- [ ] Additional weather APIs integration
- [ ] Machine learning weather predictions
- [ ] Alerting for extreme weather conditions

## 📄 License

This project is developed for educational and demonstration purposes.

---

**Built with ❤️ using modern data engineering practices**