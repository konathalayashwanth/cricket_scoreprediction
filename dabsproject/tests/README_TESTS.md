# Cricket Score Prediction - Unit Tests

This directory contains comprehensive pytest test suites for the Cricket Score Prediction project.

## Test Structure

### Test Files

1. **`conftest.py`** - Shared fixtures and test utilities
   - Spark session fixture
   - Sample data fixtures
   - Test helper functions
   - Assertion helpers with detailed reporting

2. **`test_ingestion_layer.py`** - Tests for raw data ingestion (Bronze raw tables)
   - Tests for `matchesdata` table
   - Tests for `names` table
   - Tests for `people` table
   - Data structure validation
   - Data freshness checks

3. **`test_bronze_layer.py`** - Tests for Bronze layer transformations (SCD Type 2)
   - Tests for `matches_info_structured` table
   - Tests for `matches_innings_structured` table
   - SCD Type 2 logic validation
   - Data quality checks

4. **`test_business_layer.py`** - Tests for Business/Gold layer aggregations
   - Tests for `match_summary` table
   - Tests for player statistics tables
   - Tests for team performance metrics
   - Tests for venue statistics
   - Tests for head-to-head records
   - Tests for KPI tables
   - Aggregation correctness validation

## Running Tests in Databricks

### Prerequisites

1. **Install pytest** (if not already installed):
   ```python
   %pip install pytest
   ```

2. **Ensure data is loaded**:
   - Run the `ingestion_layer` notebook first
   - Run the `Bronze_layer` notebook second
   - Run the `Business_layer` notebook third

### Running All Tests

From a Databricks notebook:

```python
import pytest
import sys

# Run all tests
pytest.main([
    "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests",
    "-v",  # Verbose output
    "-s",  # Show print statements
    "--tb=short"  # Short traceback format
])
```

### Running Individual Test Files

#### Test Ingestion Layer Only

```python
import pytest

pytest.main([
    "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests/test_ingestion_layer.py",
    "-v",
    "-s",
    "--tb=short"
])
```

#### Test Bronze Layer Only

```python
import pytest

pytest.main([
    "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests/test_bronze_layer.py",
    "-v",
    "-s",
    "--tb=short"
])
```

#### Test Business Layer Only

```python
import pytest

pytest.main([
    "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests/test_business_layer.py",
    "-v",
    "-s",
    "--tb=short"
])
```

### Running Specific Test Classes or Functions

```python
import pytest

# Run specific test class
pytest.main([
    "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests/test_bronze_layer.py::TestBronzeLayer",
    "-v",
    "-s"
])

# Run specific test function
pytest.main([
    "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests/test_bronze_layer.py::TestBronzeLayer::test_scd_type2_logic_matches_info",
    "-v",
    "-s"
])
```

## Test Output Format

Tests use a custom `test_reporter` fixture that provides detailed, colorful output:

```
======================================================================
🧪 STARTING TEST: Matches Info Structured Table Exists
======================================================================
✓ PASS: Table exists
    Table: cricketscoreprediction.bronze.matches_info_structured
✓ PASS: Table has data
    Row count: 1243
✓ PASS: Has SCD Type 2 columns
    Required: ['valid_from', 'valid_to', 'is_current', 'checksum']
✓ PASS: Has match_key (business key)
✓ PASS: Has current records (is_current=true)
    Current records: 1243
✓ PASS: No null match_keys
    Null count: 0
✓ PASS: No null valid_from timestamps
    Null count: 0

----------------------------------------------------------------------
📊 TEST SUMMARY: Matches Info Structured Table Exists
   Total Checks: 7
   Passed: 7
   Failed: 0
   ✅ ALL CHECKS PASSED
======================================================================
```

## Understanding Test Results

### Assertion Output

Each test includes detailed assertion messages:

- **✓ PASS**: Check passed successfully
- **✗ FAIL**: Check failed with details
- **Message**: Additional context about the check

### Test Summary

After each test:
- Total number of checks
- Number passed/failed
- Overall status
- List of failed checks (if any)

## Test Categories

### 1. Existence Tests
- Verify tables exist
- Verify columns exist
- Verify schemas are correct

### 2. Data Quality Tests
- No null values in critical columns
- Data type validation
- Value range validation
- Coverage checks (percentage of populated fields)

### 3. Business Logic Tests
- SCD Type 2 implementation correctness
- Aggregation accuracy
- Referential integrity
- Uniqueness constraints

### 4. Data Freshness Tests
- Reasonable data volumes
- Data diversity (multiple venues, teams, etc.)

## Debugging Failed Tests

### Step 1: Identify the Failed Test

Look for ✗ FAIL markers in the output.

### Step 2: Check the Error Message

Each failed check includes:
- Description of what was checked
- Expected vs. actual values
- Stack trace (if applicable)

### Step 3: Run in Isolation

Run just the failed test:

```python
import pytest

pytest.main([
    "tests/test_bronze_layer.py::TestBronzeLayer::test_scd_type2_logic_matches_info",
    "-v",
    "-s",
    "--tb=long"  # Full traceback for debugging
])
```

### Step 4: Investigate the Data

Manually query the table to understand the issue:

```python
df = spark.table("cricketscoreprediction.bronze.matches_info_structured")
display(df.limit(10))
```

## Example: Complete Test Run Notebook

Create a notebook cell with:

```python
# Install pytest if needed
%pip install pytest

# Import required modules
import pytest
import sys

# Add tests directory to path
sys.path.insert(0, "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests")

print("\n" + "="*70)
print("RUNNING ALL CRICKET SCORE PREDICTION TESTS")
print("="*70)

# Run all tests with detailed output
pytest.main([
    "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests",
    "-v",
    "-s",
    "--tb=short",
    "--color=yes"
])
```

## Test Maintenance

### Adding New Tests

1. Add test functions to the appropriate test class
2. Follow the naming convention: `test_<description>`
3. Use the `test_reporter` fixture for consistent output
4. Include multiple checks per test when appropriate

### Updating Tests

When data schemas change:
1. Update the fixtures in `conftest.py`
2. Update column name lists in test assertions
3. Update expected row counts if applicable

## Best Practices

1. **Run tests after each pipeline run** to ensure data quality
2. **Use verbose output** (`-v -s`) for debugging
3. **Test incrementally** - run individual layers as you develop
4. **Review failed tests immediately** - data quality issues compound
5. **Keep tests fast** - avoid full table scans when possible
6. **Document expected values** - use comments to explain thresholds

## Common Issues and Solutions

### Issue: "Table not found"
**Solution**: Ensure the pipeline notebooks have been run first.

### Issue: "Expected X rows, got 0"
**Solution**: Check that data was successfully ingested.

### Issue: "Multiple current records for key"
**Solution**: SCD Type 2 MERGE logic may have an issue - check the Bronze layer notebook.

### Issue: "Import errors"
**Solution**: Ensure pytest is installed: `%pip install pytest`

## Contact

For questions about these tests, refer to the project documentation or contact the data engineering team.
