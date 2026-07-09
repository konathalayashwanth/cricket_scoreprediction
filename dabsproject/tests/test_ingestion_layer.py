"""Unit tests for the Ingestion Layer (Bronze raw data loading).

Run this test file separately in Databricks:
    pytest tests/test_ingestion_layer.py -v -s
"""

import pytest
from pyspark.sql.functions import col
import json


class TestIngestionLayer:
    """Test suite for ingestion layer data loading."""
    
    def test_bronze_matchesdata_table_exists(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test that matches data table exists and has correct structure."""
        test_reporter.start_test("Bronze Matches Data Table Exists")
        
        table_name = f"{test_catalog}.{bronze_schema}.matchesdata"
        
        # Check 1: Table exists
        try:
            df = spark.table(table_name)
            test_reporter.add_check("Table exists", True, f"Table: {table_name}")
        except Exception as e:
            test_reporter.add_check("Table exists", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 2: Has data
        row_count = df.count()
        has_data = row_count > 0
        test_reporter.add_check("Table has data", has_data, 
                               f"Row count: {row_count}")
        
        # Check 3: Expected columns exist
        expected_cols = ["info", "innings"]
        actual_cols = df.columns
        has_expected_cols = all(col in actual_cols for col in expected_cols)
        test_reporter.add_check("Has expected columns (info, innings)", has_expected_cols,
                               f"Actual columns: {actual_cols}")
        
        # Check 4: Info column is not null
        info_not_null = df.filter(col("info").isNotNull()).count()
        test_reporter.add_check("Info column has non-null values", info_not_null > 0,
                               f"Non-null count: {info_not_null}")
        
        # Check 5: Innings column is not null
        innings_not_null = df.filter(col("innings").isNotNull()).count()
        test_reporter.add_check("Innings column has non-null values", innings_not_null > 0,
                               f"Non-null count: {innings_not_null}")
        
        test_reporter.end_test()
    
    def test_bronze_names_table_exists(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test that names table exists and has correct structure."""
        test_reporter.start_test("Bronze Names Table Exists")
        
        table_name = f"{test_catalog}.{bronze_schema}.names"
        
        # Check 1: Table exists
        try:
            df = spark.table(table_name)
            test_reporter.add_check("Table exists", True, f"Table: {table_name}")
        except Exception as e:
            test_reporter.add_check("Table exists", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 2: Has data
        row_count = df.count()
        has_data = row_count > 0
        test_reporter.add_check("Table has data", has_data,
                               f"Row count: {row_count}")
        
        # Check 3: Schema is valid
        columns = df.columns
        test_reporter.add_check("Table has columns", len(columns) > 0,
                               f"Columns: {columns}")
        
        # Check 4: No completely empty rows
        non_null_rows = df.dropna(how='all').count()
        test_reporter.add_check("No completely empty rows", non_null_rows == row_count,
                               f"Non-null rows: {non_null_rows}/{row_count}")
        
        test_reporter.end_test()
    
    def test_bronze_people_table_exists(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test that people table exists and has correct structure."""
        test_reporter.start_test("Bronze People Table Exists")
        
        table_name = f"{test_catalog}.{bronze_schema}.people"
        
        # Check 1: Table exists
        try:
            df = spark.table(table_name)
            test_reporter.add_check("Table exists", True, f"Table: {table_name}")
        except Exception as e:
            test_reporter.add_check("Table exists", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 2: Has data
        row_count = df.count()
        has_data = row_count > 0
        test_reporter.add_check("Table has data", has_data,
                               f"Row count: {row_count}")
        
        # Check 3: Schema is valid
        columns = df.columns
        test_reporter.add_check("Table has columns", len(columns) > 0,
                               f"Columns: {columns}")
        
        # Check 4: No completely empty rows
        non_null_rows = df.dropna(how='all').count()
        test_reporter.add_check("No completely empty rows", non_null_rows == row_count,
                               f"Non-null rows: {non_null_rows}/{row_count}")
        
        test_reporter.end_test()
    
    def test_matchesdata_structure(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test the structure and nested fields of matchesdata."""
        test_reporter.start_test("Matches Data Structure Validation")
        
        table_name = f"{test_catalog}.{bronze_schema}.matchesdata"
        
        try:
            df = spark.table(table_name)
            test_reporter.add_check("Table accessible", True)
        except Exception as e:
            test_reporter.add_check("Table accessible", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 1: info.venue exists
        try:
            venue_count = df.select("info.venue").filter(col("info.venue").isNotNull()).count()
            test_reporter.add_check("info.venue field exists and has data", venue_count > 0,
                                   f"Venue count: {venue_count}")
        except Exception as e:
            test_reporter.add_check("info.venue field exists", False, f"Error: {str(e)}")
        
        # Check 2: info.teams exists
        try:
            teams_count = df.select("info.teams").filter(col("info.teams").isNotNull()).count()
            test_reporter.add_check("info.teams field exists and has data", teams_count > 0,
                                   f"Teams count: {teams_count}")
        except Exception as e:
            test_reporter.add_check("info.teams field exists", False, f"Error: {str(e)}")
        
        # Check 3: info.match_type exists
        try:
            match_type_count = df.select("info.match_type").filter(col("info.match_type").isNotNull()).count()
            test_reporter.add_check("info.match_type field exists and has data", match_type_count > 0,
                                   f"Match type count: {match_type_count}")
        except Exception as e:
            test_reporter.add_check("info.match_type field exists", False, f"Error: {str(e)}")
        
        # Check 4: innings is array type
        try:
            innings_sample = df.select("innings").filter(col("innings").isNotNull()).limit(1).collect()
            has_innings = len(innings_sample) > 0
            test_reporter.add_check("Innings is array with data", has_innings,
                                   f"Sample innings present: {has_innings}")
        except Exception as e:
            test_reporter.add_check("Innings is array", False, f"Error: {str(e)}")
        
        # Check 5: Sample data quality
        try:
            sample_df = df.limit(5)
            sample_count = sample_df.count()
            test_reporter.add_check("Can sample data", sample_count > 0,
                                   f"Sample size: {sample_count}")
        except Exception as e:
            test_reporter.add_check("Can sample data", False, f"Error: {str(e)}")
        
        test_reporter.end_test()
    
    def test_data_freshness(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test that data was ingested successfully (freshness check)."""
        test_reporter.start_test("Data Freshness Check")
        
        table_name = f"{test_catalog}.{bronze_schema}.matchesdata"
        
        try:
            df = spark.table(table_name)
            test_reporter.add_check("Table accessible", True)
        except Exception as e:
            test_reporter.add_check("Table accessible", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 1: Table has reasonable amount of data
        row_count = df.count()
        has_reasonable_data = row_count >= 10  # At least 10 matches
        test_reporter.add_check("Has reasonable data volume (>=10 rows)", has_reasonable_data,
                               f"Row count: {row_count}")
        
        # Check 2: Data diversity - multiple venues
        try:
            venue_count = df.select("info.venue").distinct().count()
            has_diverse_venues = venue_count > 1
            test_reporter.add_check("Has diverse venues (>1)", has_diverse_venues,
                                   f"Unique venues: {venue_count}")
        except Exception as e:
            test_reporter.add_check("Has diverse venues", False, f"Error: {str(e)}")
        
        # Check 3: Data diversity - multiple teams
        try:
            from pyspark.sql.functions import explode
            team_count = df.select(explode("info.teams").alias("team")).distinct().count()
            has_diverse_teams = team_count > 2
            test_reporter.add_check("Has diverse teams (>2)", has_diverse_teams,
                                   f"Unique teams: {team_count}")
        except Exception as e:
            test_reporter.add_check("Has diverse teams", False, f"Error: {str(e)}")
        
        test_reporter.end_test()


if __name__ == "__main__":
    # For running directly in Databricks
    print("\n" + "="*70)
    print("INGESTION LAYER TESTS")
    print("="*70)
    pytest.main([__file__, "-v", "-s", "--tb=short"])
