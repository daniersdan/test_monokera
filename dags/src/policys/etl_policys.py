from db_connection.postgres_connection import PostgreSQLConnection
import pandas as pd
import logging
import src.policys.services.constants as const
import os


class Policys:

    def __init__(self, db_host, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        self.__logger = logging.getLogger(__name__)

    def run(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        source_csv = os.path.join(base_dir, 'template', 'MOCK_DATA.csv')
        data = dict()
        data['original_data'] = pd.read_csv(source_csv)
        insured_data = self.__insured(data=data)
        if not policys_data.empty:
            self.__load(
                conn=db,
                data=policys_data,
                schema=const.DB_SCHEMA,
                dest_table=const.POLICY_TABLE,
                truncate=False,
            )

    def __insured(self, data):
        self.__logger.info(f"Staring transfomration to insured data")
        try:
            insured_data = data['original'][const.INSURED_COLUMNS]
        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform insured data. ", e
            )
            raise

    def __premium(self, data):
        self.__logger.info(f"Staring transfomration to premium data")
        try:
            insured_data = data['original'][const.INSURED_COLUMNS]
        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform premium data. ", e
            )
            raise

    def __agents(self, data):
        self.__logger.info(f"Staring transfomration to agents data")
        try:
            insured_data = data['original'][const.INSURED_COLUMNS]
        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform agents data. ", e
            )
            raise

    def __agents(self, data):
        self.__logger.info(f"Staring transfomration to agents data")
        try:
            insured_data = data['original'][const.INSURED_COLUMNS]
        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform agents data. ", e
            )
            raise

    def __extract(self, conn_name, query, params: dict = None, source=None):
        self.__logger.info(f"Staring data extraction for {source}")
        try:
            query = query.as_string(conn_name)
            data = pd.read_sql(text(query), conn_name.connection, params=params)
            self.__logger.info(f"{len(data)} rows extracted from {source}")
            self.__logger.info("Data extraction successfully finished")
        except Exception as e:
            self.__logger.error("An error has occurred during data extraction. ", e)
            raise
        return data

    def __load(self, conn, data, dest_table: str, schema="public", truncate=True):
        try:
            self.__logger.info(f"writing {dest_table} table")
            conn.execute_insert(
                data_frame=data, schema=schema, table_name=dest_table, truncate=truncate
            )
            self.__logger.info(f"{len(data)} rows load in {dest_table}")
            self.__logger.info(
                f"loading operation finished successfully for {dest_table}"
            )
        except Exception as e:
            self.__logger.error(
                f"An error has occurred during load process for {dest_table}. ", e
            )
            raise
