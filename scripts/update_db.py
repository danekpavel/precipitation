"""
Updates the database with any new data found in CSV files.
"""

import data_db_csv

if __name__ == '__main__':
    data_db_csv.update_db_precipitation_from_dir()