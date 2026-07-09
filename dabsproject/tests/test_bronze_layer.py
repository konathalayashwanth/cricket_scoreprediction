"""Unit tests for the Bronze Layer (SCD Type 2 transformations).

Run this test file separately in Databricks:
    pytest tests/test_bronze_layer.py -v -s
"""

import pytest
from pyspark.sql.functions import col, count, sum as spark_sum


class TestBronzeLayer:
    """Test suite for Bronze layer transformations with SCD Type 2."""
    
    def test_matches_info_structured_table_exists(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test that matches_info_structured table exists with SCD Type 2 columns."""
        test_reporter.start_test("Matches Info Structured Table Exists")
        
        table_name = f"{test_catalog}.{bronze_schema}.matches_info_structured"
        
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
        
        # Check 3: SCD Type 2 columns exist
        required_scd_cols = ["valid_from", "valid_to", "is_current", "checksum"]
        actual_cols = df.columns
        has_scd_cols = all(col in actual_cols for col in required_scd_cols)
        test_reporter.add_check("Has SCD Type 2 columns", has_scd_cols,
                               f"Required: {required_scd_cols}")
        
        # Check 4: Business key column exists
        has_match_key = "match_key" in actual_cols
        test_reporter.add_check("Has match_key (business key)", has_match_key)
        
        # Check 5: Has current records
        current_count = df.filter(col("is_current") == True).count()
        has_current = current_count > 0
        test_reporter.add_check("Has current records (is_current=true)", has_current,
                               f"Current records: {current_count}")
        
        # Check 6: No nulls in critical columns
        null_match_keys = df.filter(col("match_key").isNull()).count()
        no_null_keys = null_match_keys == 0
        test_reporter.add_check("No null match_keys", no_null_keys,
                               f"Null count: {null_match_keys}")
        
        # Check 7: Valid_from is not null for all records
        null_valid_from = df.filter(col("valid_from").isNull()).count()
        no_null_valid_from = null_valid_from == 0
        test_reporter.add_check("No null valid_from timestamps", no_null_valid_from,
                               f"Null count: {null_valid_from}")
        
        test_reporter.end_test()
    
    def test_matches_innings_structured_table_exists(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test that matches_innings_structured table exists with SCD Type 2 columns."""
        test_reporter.start_test("Matches Innings Structured Table Exists")
        
        table_name = f"{test_catalog}.{bronze_schema}.matches_innings_structured"
        
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
        
        # Check 3: SCD Type 2 columns exist
        required_scd_cols = ["valid_from", "valid_to", "is_current", "checksum"]
        actual_cols = df.columns
        has_scd_cols = all(col in actual_cols for col in required_scd_cols)
        test_reporter.add_check("Has SCD Type 2 columns", has_scd_cols,
                               f"Required: {required_scd_cols}")
        
        # Check 4: Business key column exists
        has_innings_key = "innings_key" in actual_cols
        test_reporter.add_check("Has innings_key (business key)", has_innings_key)
        
        # Check 5: Has current records
        current_count = df.filter(col("is_current") == True).count()
        has_current = current_count > 0
        test_reporter.add_check("Has current records (is_current=true)", has_current,
                               f"Current records: {current_count}")
        
        # Check 6: Delivery column exists and is struct type
        has_delivery = "delivery" in actual_cols
        test_reporter.add_check("Has delivery column (struct)", has_delivery)
        
        test_reporter.end_test()
    
    def test_scd_type2_logic_matches_info(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test SCD Type 2 logic for matches_info_structured."""
        test_reporter.start_test("SCD Type 2 Logic - Matches Info")
        
        table_name = f"{test_catalog}.{bronze_schema}.matches_info_structured"
        
        try:
            df = spark.table(table_name)
            test_reporter.add_check("Table accessible", True)
        except Exception as e:
            test_reporter.add_check("Table accessible", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 1: Current records have null valid_to
        current_records = df.filter(col("is_current") == True)
        current_with_null_valid_to = current_records.filter(col("valid_to").isNull()).count()
        current_total = current_records.count()
        all_current_null_valid_to = current_with_null_valid_to == current_total
        test_reporter.add_check("All current records have null valid_to", all_current_null_valid_to,
                               f"{current_with_null_valid_to}/{current_total} have null valid_to")
        
        # Check 2: Expired records have non-null valid_to
        expired_records = df.filter(col("is_current") == False)
        if expired_records.count() > 0:
            expired_with_valid_to = expired_records.filter(col("valid_to").isNotNull()).count()
            expired_total = expired_records.count()
            all_expired_have_valid_to = expired_with_valid_to == expired_total
            test_reporter.add_check("All expired records have valid_to timestamp", all_expired_have_valid_to,
                                   f"{expired_with_valid_to}/{expired_total} have valid_to")
        else:
            test_reporter.add_check("Expired records check", True,
                                   "No expired records found (acceptable for initial load)")
        
        # Check 3: Each match_key has exactly one current record
        from pyspark.sql.functions import count
        key_current_counts = df.filter(col("is_current") == True) \
            .groupBy("match_key") \
            .agg(count("*").alias("current_count"))
        
        multiple_current = key_current_counts.filter(col("current_count") > 1).count()
        one_current_per_key = multiple_current == 0
        test_reporter.add_check("Each match_key has exactly one current record", one_current_per_key,
                               f"Keys with multiple current records: {multiple_current}")
        
        # Check 4: Checksums are not null
        null_checksums = df.filter(col("checksum").isNull()).count()
        no_null_checksums = null_checksums == 0
        test_reporter.add_check("No null checksums", no_null_checksums,
                               f"Null checksums: {null_checksums}")
        
        # Check 5: valid_from < valid_to for expired records
        if expired_records.count() > 0:
            invalid_dates = expired_records.filter(
                col("valid_from") >= col("valid_to")
            ).count()
            valid_date_range = invalid_dates == 0
            test_reporter.add_check("valid_from < valid_to for expired records", valid_date_range,
                                   f"Invalid date ranges: {invalid_dates}")
        
        test_reporter.end_test()
    
    # def test_scd_type2_logic_innings(self, spark, test_catalog, bronze_schema, test_reporter):
    #     """Test SCD Type 2 logic for matches_innings_structured."""
    #     test_reporter.start_test("SCD Type 2 Logic - Innings")
        
    #     table_name = f"{test_catalog}.{bronze_schema}.matches_innings_structured"
        
    #     try:
    #         df = spark.table(table_name)
    #         test_reporter.add_check("Table accessible", True)
    #     except Exception as e:
    #         test_reporter.add_check("Table accessible", False, f"Error: {str(e)}")
    #         test_reporter.end_test()
    #         return
        
    #     # Check 1: Current records have null valid_to
    #     current_records = df.filter(col("is_current") == True)
    #     current_with_null_valid_to = current_records.filter(col("valid_to").isNull()).count()
    #     current_total = current_records.count()
    #     all_current_null_valid_to = current_with_null_valid_to == current_total
    #     test_reporter.add_check("All current records have null valid_to", all_current_null_valid_to,
    #                            f"{current_with_null_valid_to}/{current_total} have null valid_to")
        
    #     # Check 2: Each innings_key has exactly one current record
    #     from pyspark.sql.functions import count
    #     key_current_counts = df.filter(col("is_current") == True) \
    #         .groupBy("innings_key") \
    #         .agg(count("*").alias("current_count"))
        
    #     multiple_current = key_current_counts.filter(col("current_count") > 1).count()
    #     one_current_per_key = multiple_current == 0
    #     test_reporter.add_check("Each innings_key has exactly one current record", one_current_per_key,
    #                            f"Keys with multiple current records: {multiple_current}")
        
    #     # Check 3: Team column is populated
    #     null_teams = df.filter(col("team").isNull()).count()
    #     has_teams = null_teams == 0
    #     test_reporter.add_check("All records have team populated", has_teams,
    #                            f"Null teams: {null_teams}")
        
    #     # Check 4: Over_number is valid (>= 0)
    #     invalid_overs = df.filter(
    #         col("over_number").isNull() | (col("over_number") < 0)
    #     ).count()
    #     valid_overs = invalid_overs == 0
    #     test_reporter.add_check("All over_numbers are valid (>= 0)", valid_overs,
    #                            f"Invalid overs: {invalid_overs}")
        
    #     test_reporter.end_test()
    
    def test_data_quality_matches_info(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test data quality for matches_info_structured."""
        test_reporter.start_test("Data Quality - Matches Info")
        
        table_name = f"{test_catalog}.{bronze_schema}.matches_info_structured"
        
        try:
            df = spark.table(table_name).filter(col("is_current") == True)
            test_reporter.add_check("Table accessible", True)
        except Exception as e:
            test_reporter.add_check("Table accessible", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 1: Venue is populated
        null_venues = df.filter(col("venue").isNull()).count()
        has_venues = null_venues == 0
        test_reporter.add_check("All current records have venue", has_venues,
                               f"Null venues: {null_venues}")
        
        # Check 2: Teams array is populated
        null_teams = df.filter(col("teams").isNull()).count()
        has_teams = null_teams == 0
        test_reporter.add_check("All current records have teams array", has_teams,
                               f"Null teams: {null_teams}")
        
        # Check 3: Match_type is populated
        null_match_type = df.filter(col("match_type").isNull()).count()
        has_match_type = null_match_type == 0
        test_reporter.add_check("All current records have match_type", has_match_type,
                               f"Null match_type: {null_match_type}")
        
        # Check 4: City exists for most records (>80%)
        total = df.count()
        with_city = df.filter(col("city").isNotNull()).count()
        city_coverage = (with_city / total * 100) if total > 0 else 0
        has_good_city_coverage = city_coverage > 80
        test_reporter.add_check("Good city coverage (>80%)", has_good_city_coverage,
                               f"City coverage: {city_coverage:.1f}%")
        
        # Check 5: Outcome winner exists for most records
        with_winner = df.filter(col("outcome_winner").isNotNull()).count()
        winner_coverage = (with_winner / total * 100) if total > 0 else 0
        has_good_winner_coverage = winner_coverage > 70
        test_reporter.add_check("Good outcome_winner coverage (>70%)", has_good_winner_coverage,
                               f"Winner coverage: {winner_coverage:.1f}%")
        
        test_reporter.end_test()
    
    def test_data_quality_innings(self, spark, test_catalog, bronze_schema, test_reporter):
        """Test data quality for matches_innings_structured."""
        test_reporter.start_test("Data Quality - Innings")
        
        table_name = f"{test_catalog}.{bronze_schema}.matches_innings_structured"
        
        try:
            df = spark.table(table_name).filter(col("is_current") == True)
            test_reporter.add_check("Table accessible", True)
        except Exception as e:
            test_reporter.add_check("Table accessible", False, f"Error: {str(e)}")
            test_reporter.end_test()
            return
        
        # Check 1: Delivery data exists
        null_deliveries = df.filter(col("delivery").isNull()).count()
        has_deliveries = null_deliveries == 0
        test_reporter.add_check("All current records have delivery data", has_deliveries,
                               f"Null deliveries: {null_deliveries}")
        
        # Check 2: Batter information exists in delivery
        with_batter = df.filter(col("delivery.batter").isNotNull()).count()
        total = df.count()
        batter_coverage = (with_batter / total * 100) if total > 0 else 0
        has_good_batter = batter_coverage > 95
        test_reporter.add_check("Good batter coverage (>95%)", has_good_batter,
                               f"Batter coverage: {batter_coverage:.1f}%")
        
        # Check 3: Bowler information exists in delivery
        with_bowler = df.filter(col("delivery.bowler").isNotNull()).count()
        bowler_coverage = (with_bowler / total * 100) if total > 0 else 0
        has_good_bowler = bowler_coverage > 95
        test_reporter.add_check("Good bowler coverage (>95%)", has_good_bowler,
                               f"Bowler coverage: {bowler_coverage:.1f}%")
        
        # Check 4: Runs data exists
        with_runs = df.filter(col("delivery.runs").isNotNull()).count()
        runs_coverage = (with_runs / total * 100) if total > 0 else 0
        has_good_runs = runs_coverage > 95
        test_reporter.add_check("Good runs data coverage (>95%)", has_good_runs,
                               f"Runs coverage: {runs_coverage:.1f}%")
        
        test_reporter.end_test()


if __name__ == "__main__":
    # For running directly in Databricks
    print("\n" + "="*70)
    print("BRONZE LAYER TESTS (SCD TYPE 2)")
    print("="*70)
    pytest.main([__file__, "-v", "-s", "--tb=short"])
