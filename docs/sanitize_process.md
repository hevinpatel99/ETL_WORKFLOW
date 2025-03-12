# Data Sanitization

## Overview

This module provides a class `DataSanitization` responsible for cleaning and sanitizing data in a Pandas DataFrame. The
sanitization includes:

1. Trimming whitespace and capitalizing the first letter of string columns.
2. Formatting date columns to a Cassandra-compatible format (YYYY-MM-DD).
3. Removing rows with invalid email addresses (NaN, None, or empty).

## Class: DataSanitization

### Imports Statements

```python
import os

import pandas as pd

from base_files.file_validation_config import FILE_CONFIG
from base_files.logger_setup import setup_logger
```

### Logger Setup

```python
logger = setup_logger("data_sanitize")  # Create a logger for this module
```

### `__init__(data_frame, file_name)`

The constructor initializes the object with a Pandas DataFrame and the corresponding file name.

**Parameters:**

- `data_frame` (pandas.DataFrame): The data that needs sanitization.
- `file_name` (str): The name of the file, which is checked against a configuration file.

```python
def __init__(self, data_frame, file_name):
        # Initialize the object with DataFrame and file name
        self.data_frame = data_frame
        self.file_name = file_name
```

### `sanitize_data()`

This method applies sanitization to the DataFrame and logs the result.

**Returns:**

- `True` if the sanitization was successful or skipped.
- `False` if the file is unsupported or no sanitization was needed.

```python
def sanitize_data(self):
        logger.info(f"FILE_NAME ---- {self.file_name}")

        name = os.path.splitext(self.file_name)[0]

        print(f"file name: {name}")

        # Check if file_name is in the configuration
        if name not in FILE_CONFIG:
            logger.warning(f"Data sanitization skipped for unsupported file: {self.file_name}")
            return False

        config = FILE_CONFIG[name]
        # print(f"config file : {config}")
        # required_columns = config['REQUIRED_COLUMNS']

        # logger.info(f"FILE_COLUMNS ---- {self.data_frame.columns}")
        self._sanitize_date_columns()

        sanitization_status = self._sanitize_string_columns()

        self._sanitize_remove_columns()

        if sanitization_status:
            logger.info(f"Data sanitization completed successfully for file: {self.file_name}")
        else:
            logger.info(f"No changes were needed in {self.file_name}. Skipping sanitization.")
        return True
```

### `_sanitize_string_columns()`

Sanitizes string columns by:

- Trimming leading/trailing whitespace.
- Capitalizing the first letter of each word (except for the 'EMAIL' column which is converted to lowercase and
  stripped).

**Returns:**

- `True` if any string column was modified.
- `False` if no changes were made.

```python
def _sanitize_string_columns(self):
        """
        Trim whitespace and capitalize the first letter of each word for string columns.
        """
        if self.data_frame.empty:
            logger.warning("Data frame is empty. Skipping sanitization.")
            return False

        string_columns = self.data_frame.select_dtypes(include=['object']).columns

        if not string_columns.any():  # No string columns found
            logger.info("No string columns found in the DataFrame. Skipping sanitization.")
            return False

        modified = False  # Track if any changes are made

        for col in string_columns:
            if col == "EMAIL":
                # Preserve None and NaN values before processing
                self.data_frame[col] = self.data_frame[col].apply(
                    lambda x: x.strip().lower() if isinstance(x, str) else x
                )
            else:
                self.data_frame[col] = self.data_frame[col].apply(
                    lambda x: x.strip().title() if isinstance(x, str) else x
                )

            modified = True
            logger.info(f"Sanitized column '{col}' - Trimmed whitespace & capitalized words.")

        return modified

```

### `_sanitize_date_columns()`

- Sanitizes date columns, ensuring they are formatted to a Cassandra-compatible format (`YYYY-MM-DD`).

**Returns:**

- `True` if any date column was sanitized.
- `False` if no date columns were found.

```python
def _sanitize_date_columns(self):
        """
        Format date columns to Cassandra-compatible format (YYYY-MM-DD).
        Handles both standard date formats and epoch timestamps.
        """
        if self.data_frame.empty:
            logger.warning("Data frame is empty. Skipping date sanitization.")
            return False

        date_columns = ['DOB', 'START_DATE', 'END_DATE', 'DATE', 'DATE_OF_BIRTH', 'DATE_OF_DEATH', 'TIMESTAMP']

        for col in date_columns:
            if col in self.data_frame.columns:
                # Convert to datetime (handling both epoch timestamps and date strings)
                self.data_frame[col] = pd.to_datetime(self.data_frame[col], errors="coerce", unit='ms')

                # Convert to string format (YYYY-MM-DD) while replacing NaN with None
                self.data_frame[col] = self.data_frame[col].dt.strftime('%Y-%m-%d')

                # Replace NaN, 'NaT', or invalid values with None (Cassandra expects NULL)
                self.data_frame[col] = self.data_frame[col].replace(
                    {pd.NaT: None, 'NaT': None, 'nan': None, float('nan'): None, 'None': None})

        return True  # Indicate successful sanitization
```

### `_sanitize_remove_columns()`

- Removes rows from the DataFrame where the 'EMAIL' column contains NaN, None, or an empty string.

**Returns:**

- `True` if sanitization was performed (i.e., rows were removed).
- `False` if no rows were removed or if the 'EMAIL' column is not present.

```python
def _sanitize_remove_columns(self):
    """
    Remove rows where the 'EMAIL' column is NaN, None, or an empty string.
    """
    column_to_check = "EMAIL"  # Change this if you want to check another column

    if column_to_check in self.data_frame.columns:
        logger.info(f"Before sanitization: {len(self.data_frame)} rows")

        # Convert explicit None to NaN (Apply directly on the entire dataframe)
        self.data_frame.replace({column_to_check: {None: pd.NA}}, inplace=True)

        # Remove rows where the column has NaN (which includes None)
        self.data_frame.dropna(subset=[column_to_check], inplace=True)

        # Remove rows where the column is an empty string
        self.data_frame = self.data_frame[self.data_frame[column_to_check].astype(str).str.strip() != ""]

        logger.info(f"After sanitization: {len(self.data_frame)} rows")

        if self.data_frame.empty:
            logger.warning(f"All rows were removed after sanitizing column '{column_to_check}'.")
            return False  # If all rows are removed, return False

        return True  # Indicate that sanitization was performed

    return False  # If the column is not found, return False

```


