"""Pytest configuration and shared fixtures for cricket score prediction tests."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, ArrayType, DoubleType, LongType, TimestampType
)
from datetime import datetime
import json


@pytest.fixture(scope="session")
def spark():
    """Create or get existing Spark session for testing."""
    spark = SparkSession.builder \
        .appName("Cricket Score Prediction Tests") \
        .getOrCreate()
    
    # Set shuffle partitions to a small number for faster testing
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    
    yield spark
    
    # Note: Don't stop spark in Databricks as it's managed
    # spark.stop()


@pytest.fixture(scope="function")
def test_catalog():
    """Return the test catalog name."""
    return "cricketscoreprediction"


@pytest.fixture(scope="function")
def bronze_schema():
    """Return the bronze schema name."""
    return "bronze"


@pytest.fixture(scope="function")
def silver_schema():
    """Return the silver schema name."""
    return "silver"


@pytest.fixture(scope="function")
def gold_schema():
    """Return the gold schema name."""
    return "gold"


@pytest.fixture(scope="function")
def sample_match_data():
    """Create sample match data for testing."""
    return [
        {
            "info": {
                "city": "Mumbai",
                "dates": ["2024-04-01"],
                "match_type": "T20",
                "teams": ["Mumbai Indians", "Chennai Super Kings"],
                "toss": {
                    "decision": "bat",
                    "winner": "Mumbai Indians"
                },
                "officials": {
                    "umpires": ["Umpire 1", "Umpire 2"]
                },
                "venue": "Wankhede Stadium",
                "player_of_match": ["Player 1"],
                "outcome": {
                    "result": "win",
                    "winner": "Mumbai Indians",
                    "by": {
                        "runs": 10,
                        "wickets": None
                    }
                },
                "overs": 20
            },
            "innings": [
                {
                    "team": "Mumbai Indians",
                    "super_over": False,
                    "target": {
                        "overs": 20.0,
                        "runs": 180
                    },
                    "powerplays": [
                        {
                            "from": 0.1,
                            "to": 6.0,
                            "type": "powerplay"
                        }
                    ],
                    "overs": [
                        {
                            "over": 1,
                            "deliveries": [
                                {
                                    "actual_delivery": "1.1",
                                    "batter": "Rohit Sharma",
                                    "bowler": "Deepak Chahar",
                                    "non_striker": "Ishan Kishan",
                                    "runs": {
                                        "batter": 4,
                                        "extras": 0,
                                        "total": 4,
                                        "non_boundary": False
                                    },
                                    "extras": {},
                                    "wickets": []
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]


@pytest.fixture(scope="function")
def sample_names_data():
    """Create sample names data for testing."""
    return [
        {"id": 1, "name": "Rohit Sharma", "type": "player"},
        {"id": 2, "name": "Virat Kohli", "type": "player"},
        {"id": 3, "name": "MS Dhoni", "type": "player"}
    ]


@pytest.fixture(scope="function")
def sample_people_data():
    """Create sample people data for testing."""
    return [
        {"id": 1, "name": "Rohit Sharma", "country": "India", "role": "Batsman"},
        {"id": 2, "name": "Virat Kohli", "country": "India", "role": "Batsman"},
        {"id": 3, "name": "MS Dhoni", "country": "India", "role": "Wicketkeeper"}
    ]


@pytest.fixture(scope="function")
def cleanup_test_tables(spark, test_catalog, bronze_schema, gold_schema):
    """Cleanup fixture that removes test tables after each test.
    
    Usage:
        def test_something(spark, cleanup_test_tables):
            # Add tables to cleanup
            cleanup_test_tables.append(f"{catalog}.{schema}.test_table")
            # Your test code
    """
    tables_to_cleanup = []
    
    yield tables_to_cleanup
    
    # Cleanup after test
    for table in tables_to_cleanup:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"✓ Cleaned up table: {table}")
        except Exception as e:
            print(f"✗ Failed to cleanup {table}: {str(e)}")


@pytest.fixture(scope="function")
def assert_helper():
    """Helper functions for common assertions."""
    class AssertHelper:
        @staticmethod
        def assert_table_exists(spark, table_name):
            """Assert that a table exists."""
            try:
                spark.table(table_name)
                print(f"✓ PASS: Table '{table_name}' exists")
                return True
            except Exception as e:
                print(f"✗ FAIL: Table '{table_name}' does not exist: {str(e)}")
                raise AssertionError(f"Table '{table_name}' does not exist")
        
        @staticmethod
        def assert_row_count(spark, table_name, expected_count=None, min_count=None):
            """Assert row count conditions."""
            actual_count = spark.table(table_name).count()
            
            if expected_count is not None:
                assert actual_count == expected_count, \
                    f"Expected {expected_count} rows, got {actual_count}"
                print(f"✓ PASS: Table '{table_name}' has exactly {actual_count} rows")
            
            if min_count is not None:
                assert actual_count >= min_count, \
                    f"Expected at least {min_count} rows, got {actual_count}"
                print(f"✓ PASS: Table '{table_name}' has at least {min_count} rows ({actual_count} total)")
            
            return actual_count
        
        @staticmethod
        def assert_columns_exist(spark, table_name, required_columns):
            """Assert that required columns exist in table."""
            df = spark.table(table_name)
            actual_columns = set(df.columns)
            required_set = set(required_columns)
            
            missing = required_set - actual_columns
            
            assert not missing, \
                f"Missing columns in '{table_name}': {missing}"
            
            print(f"✓ PASS: Table '{table_name}' has all required columns: {required_columns}")
            return True
        
        @staticmethod
        def assert_no_nulls(spark, table_name, columns):
            """Assert that specified columns have no null values."""
            df = spark.table(table_name)
            
            for col in columns:
                null_count = df.filter(f"{col} IS NULL").count()
                assert null_count == 0, \
                    f"Column '{col}' in '{table_name}' has {null_count} null values"
                print(f"✓ PASS: Column '{col}' in '{table_name}' has no nulls")
            
            return True
        
        @staticmethod
        def assert_column_values(spark, table_name, column, expected_values):
            """Assert that a column contains expected values."""
            df = spark.table(table_name)
            actual_values = set([row[column] for row in df.select(column).distinct().collect()])
            expected_set = set(expected_values)
            
            missing = expected_set - actual_values
            
            assert not missing, \
                f"Missing values in column '{column}' of '{table_name}': {missing}"
            
            print(f"✓ PASS: Column '{column}' in '{table_name}' contains expected values")
            return True
        
        @staticmethod
        def print_test_summary(test_name, passed_checks, total_checks):
            """Print a summary of test results."""
            print("\n" + "="*60)
            print(f"TEST: {test_name}")
            print(f"Results: {passed_checks}/{total_checks} checks passed")
            if passed_checks == total_checks:
                print("✅ ALL CHECKS PASSED")
            else:
                print(f"❌ {total_checks - passed_checks} CHECKS FAILED")
            print("="*60 + "\n")
    
    return AssertHelper()


@pytest.fixture(scope="function")
def test_reporter():
    """Test reporter for detailed test output."""
    class TestReporter:
        def __init__(self):
            self.checks = []
            self.current_test = None
        
        def start_test(self, test_name):
            """Start a new test."""
            self.current_test = test_name
            self.checks = []
            print("\n" + "="*70)
            print(f"🧪 STARTING TEST: {test_name}")
            print("="*70)
        
        def add_check(self, description, passed, message=""):
            """Add a check result."""
            status = "✓ PASS" if passed else "✗ FAIL"
            self.checks.append((description, passed, message))
            print(f"{status}: {description}")
            if message:
                print(f"    {message}")
        
        def end_test(self):
            """End the test and print summary."""
            passed = sum(1 for _, p, _ in self.checks if p)
            total = len(self.checks)
            
            print("\n" + "-"*70)
            print(f"📊 TEST SUMMARY: {self.current_test}")
            print(f"   Total Checks: {total}")
            print(f"   Passed: {passed}")
            print(f"   Failed: {total - passed}")
            
            if passed == total:
                print("   ✅ ALL CHECKS PASSED")
            else:
                print(f"   ❌ {total - passed} CHECK(S) FAILED")
                print("\n   Failed checks:")
                for desc, p, msg in self.checks:
                    if not p:
                        print(f"   - {desc}")
                        if msg:
                            print(f"     {msg}")
            
            print("="*70 + "\n")
            
            # Assert all passed
            assert passed == total, f"{total - passed} check(s) failed in {self.current_test}"
    
    return TestReporter()
