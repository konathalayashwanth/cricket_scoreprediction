# Cricket Score Prediction - Data Engineering Pipeline

## 🏏 Project Overview

A production-grade data engineering solution built on Databricks using **Declarative Automation Bundles (DABs)** for automated cricket match analytics and score prediction. This project implements a complete **Medallion Architecture** (Bronze → Silver → Gold) with comprehensive testing, CI/CD automation, and enterprise-grade data quality controls.

## 🎯 Business Objective

Transform raw cricket match data into actionable insights and predictive models for:
- Real-time match score predictions
- Player performance analytics
- Team strategy optimization
- Historical trend analysis
- Venue-based performance metrics

## 🏗️ Architecture

### Medallion Architecture Implementation

```
┌─────────────────┐
│  Data Sources   │
│  (Raw Cricket   │
│     Data)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ INGESTION LAYER │  ← Data acquisition and validation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BRONZE LAYER   │  ← Raw data with SCD Type 2 history
│  (Structured)   │     • matches_info_structured
└────────┬────────┘     • ball_by_ball_structured
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │  ← Cleaned, validated, business rules
│  (Curated)      │     • Deduplication
└────────┬────────┘     • Schema enforcement
         │              • Data quality checks
         ▼
┌─────────────────┐
│   GOLD LAYER    │  ← Aggregated analytics & KPIs
│  (Analytics)    │     • Player statistics
└─────────────────┘     • Team performance
                        • Venue analytics
                        • Match summaries
```

## 🚀 Key Features

### 1. **Automated Pipeline Orchestration**
- Multi-task workflow with dependency management
- Incremental data processing
- Error handling and recovery mechanisms
- Job scheduling and monitoring

### 2. **Data Quality & Governance**
- **SCD Type 2** (Slowly Changing Dimensions) for historical tracking
- Schema validation and enforcement
- Data completeness checks (null validation, coverage metrics)
- Duplicate detection and handling
- Audit columns (`is_current`, `effective_from`, `effective_to`)

### 3. **Advanced Analytics**
- Player batting statistics (runs, strike rate, average, boundaries)
- Player bowling statistics (wickets, economy, bowling average)
- Team performance metrics (win/loss ratios, head-to-head)
- Venue statistics and home advantage analysis
- Innings progression and powerplay analysis
- Partnership analytics

### 4. **Enterprise Testing Framework**
- **Pytest-based unit testing** with fixtures and custom assertions
- Layer-specific test suites (Ingestion, Bronze, Silver, Gold)
- Data quality validation tests
- Schema compliance tests
- Aggregation correctness tests
- Detailed test reporting with custom TestReporter class

### 5. **Infrastructure as Code (IaC)**
- Declarative Automation Bundles (DABs) for deployment
- Environment-specific configurations (dev, prod)
- Version-controlled infrastructure
- Automated deployment pipelines

## 📁 Project Structure

```
dabsproject/
│
├── src/                          # Source notebooks (pipeline logic)
│   ├── ingestion_layer.ipynb     # Data acquisition & validation
│   ├── Bronze_layer.ipynb        # Raw structured data with SCD2
│   ├── silver_layer.ipynb        # Data cleaning & business rules
│   └── Business_layer.ipynb      # Analytics & KPI aggregations
│
├── tests/                        # Comprehensive test suite
│   ├── conftest.py               # Pytest fixtures & test utilities
│   ├── test_ingestion_layer.py  # Ingestion validation tests
│   ├── test_bronze_layer.py     # Bronze layer & SCD2 tests
│   ├── test_business_layer.py   # Gold layer analytics tests
│   ├── run_tests.ipynb           # Databricks test runner notebook
│   └── README_TESTS.md           # Testing documentation
│
├── resources/                    # DABs resource definitions
│   └── jobs.yml                  # Job configuration (tasks, dependencies)
│
├── databricks.yml                # Bundle configuration (targets, workspace)
├── README.md                     # This file
├── TESTING_GUIDE.md              # Detailed testing instructions
└── pyproject.toml                # Python project metadata
```

## 🛠️ Technology Stack

| Component | Technology |
|-----------|-----------|
| **Platform** | Databricks (AWS) |
| **Language** | Python, SQL |
| **Orchestration** | Databricks Jobs (DABs) |
| **Storage** | Unity Catalog (Delta Lake) |
| **Testing** | Pytest |
| **Version Control** | Git |
| **Compute** | Spark 13.3.x on i3.xlarge clusters |

## 📊 Data Pipeline Layers

### 1️⃣ Ingestion Layer
**Purpose**: Acquire and validate raw cricket data from source systems

**Key Functions**:
- Data extraction from CSV/API sources
- Initial schema validation
- Source data profiling
- Raw data cataloging in Unity Catalog

**Output**: Raw data ready for structured processing

---

### 2️⃣ Bronze Layer (Raw Structured)
**Purpose**: Create structured, auditable raw data with full history tracking

**Key Features**:
- **SCD Type 2 implementation** for historical tracking
- Maintains complete data lineage
- Audit columns: `is_current`, `effective_from`, `effective_to`, `record_hash`
- No data transformation (preserves source fidelity)

**Tables**:
- `matches_info_structured` - Match metadata with history
- `ball_by_ball_structured` - Granular ball-level data with history

**Schema Example**:
```sql
CREATE TABLE bronze.matches_info_structured (
    match_key STRING,
    season STRING,
    venue STRING,
    team1 STRING,
    team2 STRING,
    winner STRING,
    is_current BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    record_hash STRING
)
```

---

### 3️⃣ Silver Layer (Curated)
**Purpose**: Clean, validated, business-ready data

**Key Transformations**:
- Deduplication
- Schema standardization
- Business rule validation
- Data type enforcement
- Null handling strategies
- Referential integrity checks

**Quality Checks**:
- Completeness validation (>95% non-null)
- Uniqueness constraints
- Valid value ranges
- Cross-table consistency

**Tables**:
- `matches_cleansed`
- `ball_by_ball_cleansed`
- `player_master`

---

### 4️⃣ Gold Layer (Analytics)
**Purpose**: Aggregated business metrics and KPIs for analytics & ML

**Key Aggregations**:

1. **Match Summary**
   - Match-level statistics
   - Date dimensions (year, month)
   - Venue information

2. **Player Batting Stats**
   - Total runs, balls faced
   - Strike rate, batting average
   - Boundaries (fours, sixes)

3. **Player Bowling Stats**
   - Total wickets, runs conceded
   - Economy rate, bowling average
   - Overs bowled

4. **Team Performance**
   - Win/loss records
   - Win rate percentages
   - Head-to-head statistics

5. **Venue Analytics**
   - Matches per venue
   - Average scores
   - Home advantage metrics

6. **KPI Tables**
   - Innings progression analysis
   - Powerplay performance
   - Partnership statistics

**Tables**: 10+ analytics-ready tables for downstream consumption

## 🧪 Testing Framework

### Test Coverage
- **80+ unit tests** across 4 test suites
- **Layer-specific validation** (Ingestion → Bronze → Silver → Gold)
- **Schema compliance** testing
- **Data quality** assertions
- **Aggregation correctness** validation

### Running Tests

```bash
# Run all tests
pytest tests/ -v -s

# Run specific layer tests
pytest tests/test_bronze_layer.py -v -s

# Run with coverage report
pytest tests/ --cov=src --cov-report=html
```

### Test Features
- **Custom fixtures** for Spark session, test catalogs, sample data
- **Assertion helpers** with detailed failure reporting
- **TestReporter class** for structured test output
- **Data validation helpers** for schema and quality checks

See [TESTING_GUIDE.md](TESTING_GUIDE.md) for detailed testing instructions.

## 📦 Deployment

### Prerequisites
- Databricks workspace (AWS)
- Databricks CLI installed and configured
- Unity Catalog enabled
- Appropriate cluster permissions

### Setup

1. **Clone the repository**:
```bash
git clone <repository-url>
cd dabsproject
```

2. **Configure Databricks CLI**:
```bash
databricks auth login --host <workspace-url>
```

3. **Validate the bundle**:
```bash
databricks bundle validate
```

4. **Deploy to dev**:
```bash
databricks bundle deploy --target dev
```

5. **Deploy to prod**:
```bash
databricks bundle deploy --target prod
```

### Environment Configuration

The bundle supports multiple deployment targets:

- **Dev**: Development environment with name prefixes
  - Auto-paused schedules
  - Single-user access
  - Source-linked deployment

- **Prod**: Production environment
  - Scheduled triggers enabled
  - Multi-user permissions
  - Optimized for performance

Configuration is managed in `databricks.yml`.

## 🔄 CI/CD Pipeline

### Automated Workflows
1. **Validate** - Bundle validation on every commit
2. **Test** - Run pytest suite on pull requests
3. **Deploy** - Automated deployment to dev/prod environments
4. **Monitor** - Job execution monitoring and alerting

### Deployment Workflow
```
Code Commit → Validation → Tests → Dev Deploy → Prod Deploy
```

## 📈 Pipeline Execution

### Job Configuration

The `Cricket_board` job orchestrates the entire pipeline:

```yaml
Tasks:
  1. ingestion_layer    ← Data acquisition
  2. bronze_layer       ← Depends on: ingestion_layer
  3. silver_layer       ← Depends on: bronze_layer
  4. gold_layer         ← Depends on: silver_layer
```

### Running the Pipeline

**Via Databricks UI**:
1. Navigate to **Workflows** → **Jobs**
2. Find `(dev <user>) Cricket_board`
3. Click **Run Now**

**Via CLI**:
```bash
databricks bundle run Cricket_board --target dev
```

### Monitoring

- **Job UI**: Real-time task execution status
- **Task Logs**: Detailed execution logs per task
- **Lineage**: Visual dependency graph
- **Metrics**: Execution time, data volume, success rate

## 🎓 Key Learnings & Best Practices

### Data Engineering Principles Applied
1. **Idempotency** - Pipelines can be re-run safely
2. **Incremental Processing** - Only process new/changed data
3. **Fail-Fast** - Validation at every layer
4. **Observability** - Comprehensive logging and monitoring
5. **Modularity** - Decoupled layers for maintainability

### Delta Lake Best Practices
- **OPTIMIZE** commands for file compaction
- **Z-ORDER** clustering on frequently filtered columns
- **VACUUM** for old file cleanup
- **Time Travel** for point-in-time recovery

### Performance Optimizations
- Broadcast joins for small dimension tables
- Partition pruning on date columns
- Caching for frequently accessed data
- Adaptive Query Execution (AQE) enabled

## 🔐 Security & Governance

- **Unity Catalog** for centralized governance
- **Row-level security** on sensitive tables
- **Column-level encryption** for PII data
- **Audit logging** for compliance
- **RBAC** (Role-Based Access Control)

## 📊 Sample Analytics Queries

### Top Run Scorers
```sql
SELECT 
    player_name,
    team,
    total_runs,
    strike_rate,
    average
FROM gold.player_batting_stats
ORDER BY total_runs DESC
LIMIT 10;
```

### Team Win Rate
```sql
SELECT 
    team,
    total_matches,
    wins,
    ROUND((wins / total_matches) * 100, 2) as win_percentage
FROM gold.team_performance
ORDER BY win_percentage DESC;
```

### Venue Statistics
```sql
SELECT 
    venue,
    total_matches,
    avg_first_innings_score,
    home_team_win_rate
FROM gold.venue_stats
ORDER BY total_matches DESC;
```

## 🐛 Troubleshooting

### Common Issues

**Issue**: Job fails on bronze layer
- **Solution**: Check source data availability and schema changes

**Issue**: Tests fail on data quality checks
- **Solution**: Review data completeness thresholds in test configuration

**Issue**: Bundle deployment errors
- **Solution**: Run `databricks bundle validate` for detailed error messages

## 📚 Documentation

- [TESTING_GUIDE.md](TESTING_GUIDE.md) - Comprehensive testing instructions
- [tests/README_TESTS.md](tests/README_TESTS.md) - Test suite documentation
- [Databricks DABs Documentation](https://docs.databricks.com/dev-tools/bundles/)

## 🤝 Contributing

This is a portfolio project demonstrating data engineering best practices. Feedback and suggestions are welcome!

## 👤 Author

**Your Name**
- Email: konathalayashwanth10@gmail.com
- LinkedIn: [Your LinkedIn Profile]
- GitHub: [Your GitHub Profile]

## 📄 License

This project is for educational and portfolio purposes.

---

## 🎯 Project Highlights for Recruiters

✅ **Production-Ready**: Enterprise-grade data pipeline with testing and CI/CD

✅ **Modern Stack**: Databricks, Delta Lake, Unity Catalog, Spark

✅ **Best Practices**: Medallion Architecture, SCD Type 2, Data Quality Checks

✅ **Infrastructure as Code**: Declarative Automation Bundles (DABs)

✅ **Comprehensive Testing**: 80+ unit tests with pytest framework

✅ **Real-World Application**: Cricket analytics with business value

✅ **Scalable Design**: Handles incremental data, optimized for performance

✅ **Well-Documented**: Clear architecture, setup, and execution guides

---

**⭐ If you find this project interesting, please star the repository!**
