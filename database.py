from datetime import datetime
import pandas as pd
import logging
from mysql.connector import connect
from contextlib import contextmanager
from typing import Generator

logger = logging.getLogger(__name__)

# station data column name translation from DB names (keys) to CSV names (values)
station_table_cols = {
    'name': 'precip_known',
    'elevation': 'ELEVATION',
    'lat': 'Y',
    'lon': 'X',
    'id_chmu': 'ID',
    'type': 'STATION_TYP'
}


class PrecipitationDB:
    def __init__(self, host, user, password, database):
        self.dbconfig = dict(
            host=host,
            user=user,
            password=password,
            database=database)


    @contextmanager
    def get_connection(self):
        connection = connect(**self.dbconfig)
        try:
            yield connection
        finally:
            connection.close()


    def get_daily_precipitation(self) -> pd.DataFrame:
        query = '''
            WITH daily AS (
                SELECT 
                    station_id, 
                    DATE(datetime) AS day, 
                    SUM(amount) AS rain
                FROM hourly_precip
                GROUP BY station_id, day)
            SELECT s.name, d.day, d.rain
            FROM daily d
            JOIN stations s ON d.station_id = s.id
            '''
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetchall()

        df = pd.DataFrame(res, columns=['station', 'date', 'amount'])
        return df


    def get_precipitation_dates(self) -> list[datetime.date]:
        query = 'SELECT DISTINCT DATE(datetime) FROM hourly_precip WHERE HOUR(datetime) = 23'
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetchall()
        return [s[0] for s in res]


    def insert_precip_data(self, data: Generator[pd.DataFrame, None, None]) -> None:
        """
        Inserts precipitation data in the 'hourly_precip' SQL table.

        Args:
            data: precipitation data in long format
        """

        queries = [
            # temporary table to store data
            '''
            CREATE TEMPORARY TABLE tmp (
                station VARCHAR(100), 
                amount DECIMAL(4, 1),
                datetime DATETIME
            );
            ''',
            # populate the temporary table
            'INSERT INTO tmp VALUES (%s, %s, %s);',
            # get station IDs and insert the data in 'hourly_precip'
            '''
            INSERT INTO hourly_precip
            SELECT s.id AS station_id, t.datetime, t.amount 
            FROM tmp t
            JOIN stations s ON t.station = s.name;
            '''
        ]

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queries[0])
                for d in data:
                    d.dropna(inplace=True)
                    logger.info(f'Inserting {d.shape[0]} rows into database temporary table.')
                    # make tuples from data
                    d = d.loc[:, ['station', 'amount', 'datetime']].itertuples(index=False, name=None)
                    cursor.executemany(queries[1], list(d))
                logger.info(f'Copying data from temporary table to hourly_precip.')
                cursor.execute(queries[2])
                conn.commit()


    def create_tables(self) -> None:
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS stations (
                id SMALLINT AUTO_INCREMENT,
                name VARCHAR(100) UNIQUE NOT NULL,
                elevation DECIMAL(5, 1) NOT NULL,
                lat FLOAT NOT NULL,
                lon FLOAT NOT NULL,
                id_chmu CHAR(8) NOT NULL,
                type VARCHAR(5) NOT NULL,
                PRIMARY KEY(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS hourly_precip (
                station_id SMALLINT,
                datetime DATETIME,
                amount DECIMAL(4,1),
                PRIMARY KEY(station_id, datetime),
                FOREIGN KEY(station_id) REFERENCES stations(id)
            );
            """
        ]
        logger.info('Creating tables `stations` and `hourly_precip`.')
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for q in table_queries:
                    cursor.execute(q)


    def insert_stations(self, stations: pd.DataFrame) -> None:
        query = (f'INSERT INTO stations ({",".join(station_table_cols.keys())}) '
                 f"VALUES ({','.join(['%s']*len(station_table_cols.keys()))})")
        data = stations.loc[:, station_table_cols.values()].itertuples(index=False, name=None)

        logger.info(f'Inserting {stations.shape[0]} rows into `stations` table.')
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, list(data))
                conn.commit()


    def get_stations_data(self) -> pd.DataFrame:
        col_names = station_table_cols.keys()
        query = f'SELECT {", ".join(col_names)} FROM stations'
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                res = cursor.fetchall()

        df = pd.DataFrame(res, columns=col_names)
        return df

    #region Pool implementation
    '''
    def __init__(self, host, user, password, database,
                 pool_name='pool', pool_size=3):
        self.pool = pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            host=host,
            user=user,
            password=password,
            database=database
        )
        logger = logging.getLogger(__name__)
        
    @contextmanager
    def get_connection(self):
        connection = self.pool.get_connection()
        try:
            yield connection
        finally:
            connection.close()
    '''
    #endregion

#df = dm.read_precip_table('2023-10-12')
#add_precip_data(df)

# stations = dm.stations_from_file('2023-10-19')
# create_tables()
