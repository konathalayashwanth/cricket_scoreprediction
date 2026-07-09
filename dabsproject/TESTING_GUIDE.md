# Cricket Score Prediction - Testing Guide

## Quick Start

### 1. Install Dependencies
```bash
pip install -e ".[dev]"
```

### 2. Run All Tests
```bash
cd /Workspace/Users/konathalayashwanth10@gmail.com/cricket_scoreprediction/dabsproject
pytest tests/ -v
```

## Test Organization

### Unit Tests
Each module has dedicated unit tests covering:

#### test_data_loader.py
* `test_init` - DataLoader initialization
* `test_filter_by_team_single_match` - Team filtering
* `test_filter_by_team_empty_name` - Empty name validation
* `test_validate_schema_success` - Schema validation success
* `test_validate_schema_missing_columns` - Missing columns error
* `test_load_match_data_invalid_table_name` - Invalid table name error

#### test_feature_engineering.py
* `test_calculate_run_rate` - Run rate calculation
* `test_calculate_run_rate_custom_columns` - Custom column names
* `test_calculate_team_average` - Team average scores
* `test_calculate_recent_form` - Recent form calculation
* `test_add_wickets_remaining` - Wickets remaining
* `test_add_wickets_remaining_custom_total` - Custom total wickets

#### test_model.py
* `test_init_valid` - Valid model initialization
* `test_init_empty_features` - Empty features error
* `test_init_invalid_features_type` - Invalid type error
* `test_init_empty_label` - Empty label error
* `test_train_valid_data` - Training with valid data
* `test_train_empty_data` - Empty training data error
* `test_train_missing_columns` - Missing columns error
* `test_predict_without_training` - Predict before training error
* `test_predict_after_training` - Successful prediction
* `test_get_feature_importance_untrained` - Feature importance untrained
* `test_get_feature_importance_trained` - Feature importance trained

### Integration Tests
#### test_integration.py
* `test_full_pipeline` - Complete end-to-end workflow
* `test_pipeline_with_validation_error` - Pipeline error handling

## Usage Example

```python
from pyspark.sql import SparkSession
from dabsproject import DataLoader, FeatureEngineer, CricketScorePredictor

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load data
loader = DataLoader(spark)
df = loader.load_match_data("catalog.schema.matches")
india_matches = loader.filter_by_team(df, "India")

# Engineer features
df_with_features = FeatureEngineer.calculate_run_rate(india_matches)
df_with_features = FeatureEngineer.add_wickets_remaining(df_with_features)

# Train model
predictor = CricketScorePredictor(
    feature_cols=["overs", "wickets_lost", "run_rate"],
    label_col="score"
)
model = predictor.train(df_with_features)

# Make predictions
predictions = predictor.predict(test_data)
```

## Continuous Integration

Add this to your CI/CD pipeline:

```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install -e ".[dev]"
      - name: Run tests
        run: pytest tests/ -v --cov=dabsproject
```

## Best Practices

1. **Run tests before commits**: `pytest tests/`
2. **Write tests for new features**: Follow existing patterns
3. **Keep tests isolated**: Each test should be independent
4. **Use fixtures**: Leverage `spark` and `load_fixture` fixtures
5. **Test edge cases**: Include error conditions and boundary values
6. **Maintain coverage**: Aim for >80% code coverage

## Troubleshooting

### Import Errors
If you see import errors, ensure the package is installed:
```bash
pip install -e .
```

### Spark Connection Issues
The `conftest.py` automatically configures Databricks Connect.
Ensure you have valid Databricks credentials.

### Fixture Not Found
Make sure you're using the correct fixture names:
* `spark` - SparkSession
* `load_fixture` - Data loader function
