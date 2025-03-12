import os
import pandas as pd

from base_files.file_validation_config import FILE_CONFIG
from base_files.logger_setup import setup_logger

logger = setup_logger("validation")


class DataValidation:
    """
    A class responsible for validating data.
    """

    def __init__(self, data_frame, file_name):
        self.data_frame = data_frame
        self.file_name = file_name

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
