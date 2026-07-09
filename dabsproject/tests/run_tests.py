# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Install pytest
# Install pytest for unit testing
%pip install pytest --quiet
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Test Configuration
import sys
import pytest

# Configuration
TEST_DIR = "/Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject/tests"

# Add tests directory to Python path
sys.path.insert(0, TEST_DIR)

print("✓ Test environment configured")
print(f"Test directory: {TEST_DIR}")

# COMMAND ----------

# DBTITLE 1,Run All Tests
print("\n" + "="*80)
print("RUNNING ALL TESTS - CRICKET SCORE PREDICTION")
print("="*80 + "\n")

# Run all test files
result = pytest.main([
    TEST_DIR,
    "-v",          # Verbose output
    "-s",          # Show print statements
    "--tb=short",  # Short traceback
    "--color=yes" # Colored output
])

if result == 0:
    print("\n✅ ALL TESTS PASSED!")
else:
    print(f"\n❌ SOME TESTS FAILED (exit code: {result})")

# COMMAND ----------

# DBTITLE 1,Run Ingestion Layer Tests Only
print("\n" + "="*80)
print("TESTING: INGESTION LAYER (Bronze Raw Tables)")
print("="*80 + "\n")

result = pytest.main([
    f"{TEST_DIR}/test_ingestion_layer.py",
    "-v",
    "-s",
    "--tb=short"
])

if result == 0:
    print("\n✅ INGESTION LAYER TESTS PASSED!")
else:
    print(f"\n❌ INGESTION LAYER TESTS FAILED (exit code: {result})")

# COMMAND ----------

# DBTITLE 1,Run Bronze Layer Tests Only
print("\n" + "="*80)
print("TESTING: BRONZE LAYER (SCD Type 2 Transformations)")
print("="*80 + "\n")

result = pytest.main([
    f"{TEST_DIR}/test_bronze_layer.py",
    "-v",
    "-s",
    "--tb=short"
])

if result == 0:
    print("\n✅ BRONZE LAYER TESTS PASSED!")
else:
    print(f"\n❌ BRONZE LAYER TESTS FAILED (exit code: {result})")

# COMMAND ----------

# DBTITLE 1,Run Business Layer Tests Only
print("\n" + "="*80)
print("TESTING: BUSINESS LAYER (Gold Aggregations & KPIs)")
print("="*80 + "\n")

result = pytest.main([
    f"{TEST_DIR}/test_business_layer.py",
    "-v",
    "-s",
    "--tb=short"
])

if result == 0:
    print("\n✅ BUSINESS LAYER TESTS PASSED!")
else:
    print(f"\n❌ BUSINESS LAYER TESTS FAILED (exit code: {result})")

# COMMAND ----------

# DBTITLE 1,Run Specific Test
# Example: Run a specific test function
# Uncomment and modify the test path as needed

# TEST_PATH = f"{TEST_DIR}/test_bronze_layer.py::TestBronzeLayer::test_scd_type2_logic_matches_info"

# print(f"\nRunning specific test: {TEST_PATH}\n")

# result = pytest.main([
#     TEST_PATH,
#     "-v",
#     "-s",
#     "--tb=long"  # Full traceback for debugging
# ])

print("Uncomment the code above to run a specific test")

# COMMAND ----------

# DBTITLE 1,Generate Test Report
# Generate a detailed test report
print("\n" + "="*80)
print("GENERATING DETAILED TEST REPORT")
print("="*80 + "\n")

result = pytest.main([
    TEST_DIR,
    "-v",
    "-s",
    "--tb=short",
    "--junit-xml=/tmp/test_results.xml",  # Generate XML report
    "--html=/tmp/test_report.html",       # Generate HTML report (requires pytest-html)
    "--self-contained-html"               # Self-contained HTML
])

print("\n📊 Test reports generated:")
print("   - XML: /tmp/test_results.xml")
print("   - HTML: /tmp/test_report.html")

if result == 0:
    print("\n✅ ALL TESTS PASSED!")
else:
    print(f"\n❌ SOME TESTS FAILED (exit code: {result})")

# COMMAND ----------


