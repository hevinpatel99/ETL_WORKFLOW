# Data Validation Class

- The `DataValidation` class is designed to validate data in a DataFrame against a configuration specific to the file
  being processed.
- This class is useful for ensuring that the data adheres to certain required structure, including column presence, data
  types, and handling missing values.

## Class: `DataValidation`

### Imports Statements

```python
import os
import pandas as pd

from base_files.file_validation_config import FILE_CONFIG
from base_files.logger_setup import setup_logger
```

### Logger Setup

```python
logger = setup_logger("validation")
```

### Constructor: `__init__(self, data_frame, file_name)`

The constructor initializes the `DataValidation` object by accepting a pandas DataFrame (`data_frame`) and the file
name (`file_name`). These are used for performing the validation.

#### Parameters:

- `data_frame` (pd.DataFrame): The data that will be validated.
- `file_name` (str): The name of the file being processed (without the extension).

```python
def __init__(self, data_frame, file_name):
    self.data_frame = data_frame
    self.file_name = file_name

```
---

### Method: `validate_data(self)`

The `validate_data` method performs a series of validations based on the file name and the corresponding configuration.

It checks:

- Whether the file name is supported (i.e., exists in the `FILE_CONFIG`).
- [View FILE_CONFIG here](/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/ETL_workflow/base_files/file_validation_config.py)
- The presence of required columns.
- The data types of each column.
- The handling of missing data based on a threshold.

#### Returns:

- `True` if the data passes validation.
- `False` if the data fails validation.

```python
def validate_data(self):
    """
    Validate the data dynamically based on file name.
    Returns True if the data is valid, False otherwise.
    """
    name = os.path.splitext(self.file_name)[0]

    if name not in FILE_CONFIG:
        logger.warning(f"Data validation skipped for unsupported file: {self.file_name}")
        return False

    config = FILE_CONFIG[name]

    logger.info(f"FILE_COLUMNS ---- {self.data_frame.columns}")

    self._drop_required_columns(name, config)

    if not self._validate_required_columns(name, config):
        return False

    if not self._validate_data_types(config['EXPECTED_DATA_TYPES']):
        return False

    return True
```

---

### Method: `_validate_required_columns(self, name, config)`

Validates that all required columns, as defined in the configuration file (`FILE_CONFIG`), exist in the DataFrame.

If any required columns are missing due to excessive missing data (null values), they are dynamically removed from the
list of required columns.

#### Parameters:

- `name` (str): The name of the current file being processed (without the extension).
- `config` (dict): The configuration associated with the file, containing the list of required columns.

#### Returns:

- `True` if required columns are found (after accounting for missing data).
- The list of missing columns is logged if any are found.

```python
def _validate_required_columns(self, name, config):
    """
    Validates that all required columns exist in the DataFrame.
    If a column was removed due to high null values, it will not be considered missing.
    """
    current_columns = set(self.data_frame.columns)
    required_columns = set(config['REQUIRED_COLUMNS'])

    missing_columns = required_columns - current_columns

    if missing_columns:
        logger.warning(f"Required columns removed due to high null values: {missing_columns}")

        # Remove missing columns from the required list dynamically
        config['REQUIRED_COLUMNS'] = list(required_columns - missing_columns)

        # Update global FILE_CONFIG to ensure consistency
        FILE_CONFIG[name]['REQUIRED_COLUMNS'] = config['REQUIRED_COLUMNS']

    return True
```
---

### Method: `_validate_data_types(self, expected_data_types)`

Validates the data types of the columns in the DataFrame. The expected data types are retrieved from the configuration
file.

Supported data types include:

- `float64`
- `int64`
- `datetime64[ns]`
- `datetime64[D]`

If a column's type does not match the expected type, the method attempts to convert it. If the conversion fails, the
method logs an error and returns `False`.

#### Parameters:

- `expected_data_types` (dict): A dictionary specifying the expected data types for each column.

#### Returns:

- `True` if all data types are validated successfully.
- `False` if any column has an incorrect data type.

```python
def _validate_data_types(self, expected_data_types):
    """Validate the data types for each column dynamically."""
    for col, expected_type in expected_data_types.items():
        if col in self.data_frame.columns:
            try:
                if expected_type == 'float64':
                    self.data_frame[col] = pd.to_numeric(
                        self.data_frame[col], errors='coerce'
                    ).fillna(0.0).astype('float64')

                elif expected_type == 'int64':
                    self.data_frame[col] = pd.to_numeric(
                        self.data_frame[col], errors='coerce'
                    ).replace([float('inf'), float('-inf')], float('nan')).fillna(0).astype('int64')

                elif expected_type in ['datetime64[ns]', 'datetime64[D]']:
                    # Convert to string, strip spaces, and remove unexpected characters
                    self.data_frame[col] = (
                        self.data_frame[col].astype(str).str.strip()
                        .str.replace(r'[^\x00-\x7F]+', '', regex=True)
                    )

                    # Convert datetime format
                    self.data_frame[col] = self.data_frame[col].apply(
                        lambda x: pd.to_datetime(x, format="%Y-%m-%dT%H:%M:%S", errors='coerce', utc=True)
                        if 'T' in x else pd.to_datetime(x, errors='coerce', utc=True)
                    )

                    # Remove timezone information
                    self.data_frame[col] = self.data_frame[col].dt.tz_localize(None)

                    # Normalize to remove time component if needed
                    if expected_type == 'datetime64[D]':
                        self.data_frame[col] = self.data_frame[col].dt.normalize()

                    self.data_frame[col] = self.data_frame[col].astype('datetime64[ns]')

                    # Log NaT values
                    null_values = self.data_frame[col].isna().sum()
                    if null_values > 0:
                        logger.warning(f"Column '{col}' has {null_values} NaT values after conversion.")

                # Ensure the expected type matches
                if str(self.data_frame[col].dtype) != expected_type:
                    logger.error(
                        f"Column '{col}' has incorrect data type. Expected: {expected_type}, Found: {self.data_frame[col].dtype}"
                    )
                    return False
            except Exception as e:
                logger.error(f"Error converting column '{col}' to {expected_type}: {e}")
                return False
    return True
```
---

### Method: `_drop_required_columns(self, name, config)`

Drops columns with missing data above a specified threshold (90% missing). This ensures that columns with too many
missing values are excluded from the validation process.

After dropping these columns, the configuration is updated to reflect the removed columns. This ensures that future
validations do not expect these columns.

#### Parameters:

- `name` (str): The name of the current file being processed (without the extension).
- `config` (dict): The configuration associated with the file.

```python
def _drop_required_columns(self, name, config):
    threshold = 0.9
    total_rows = len(self.data_frame)

    empty_cols = [
        col for col in self.data_frame.columns if self.data_frame[col].isna().sum() / total_rows >= threshold
    ]

    if empty_cols:
        logger.warning(f"Dropping columns with >= 90% missing values: {empty_cols}")
        self.data_frame.drop(columns=empty_cols, inplace=True)

    # Update required columns and expected data types in config
    config['REQUIRED_COLUMNS'] = [col for col in config['REQUIRED_COLUMNS'] if col not in empty_cols]
    config['EXPECTED_DATA_TYPES'] = {
        col: dtype for col, dtype in config['EXPECTED_DATA_TYPES'].items() if col not in empty_cols
    }

    # Update global FILE_CONFIG
    FILE_CONFIG[name]['REQUIRED_COLUMNS'] = config['REQUIRED_COLUMNS']
    FILE_CONFIG[name]['EXPECTED_DATA_TYPES'] = config['EXPECTED_DATA_TYPES']

    logger.info(f"Updated REQUIRED_COLUMNS: {config['REQUIRED_COLUMNS']}")
    logger.info(f"Updated EXPECTED_DATA_TYPES: {config['EXPECTED_DATA_TYPES']}")
```


