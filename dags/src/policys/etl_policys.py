from sqlalchemy.dialects.mssql.information_schema import columns

from db_connection.postgres_connection import PostgreSQLConnection
import pandas as pd
import logging
import src.policys.services.constants as const
import src.policys.template.sql_queries as sql
import src.policys.services.utils as utils
import os
import uuid
from datetime import datetime
import re


class Policys:

    def __init__(self, db_host, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_fmt)
        self.__logger = logging.getLogger(__name__)

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

    def __extract(self, conn_name, query, params: dict = None, source=None):

        self.__logger.info(f"Staring data extraction for {source}")
        try:
            query = query.as_string(conn_name)
            data = pd.read_sql(query, conn_name.connection, params=params)
            self.__logger.info(f"{len(data)} rows extracted from {source}")
            self.__logger.info("Data extraction successfully finished")
        except Exception as e:
            self.__logger.error("An error has occurred during data extraction. ", e)
            raise
        return data

    def run(self):
        try:
            db = PostgreSQLConnection(
                user=self.db_user,
                password=self.db_password,
                host_name=self.db_host,
                database=self.db_name,
            )
            db.create_connection()
            base_dir = os.path.dirname(os.path.abspath(__file__))
            source_csv = os.path.join(base_dir, "input", "MOCK_DATA.csv")
            data = dict()
            data["original_data"] = pd.read_csv(source_csv)
            data["original_data"] = data["original_data"].applymap(
                lambda x: x.replace("'", "") if isinstance(x, str) else x
            )
            data["insured_info"] = self.__extract(
                db, sql.querie_insured_data, source="Data Insured"
            )

            def is_date_column(series):
                return series.apply(
                    lambda x: bool(re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", str(x)))
                ).all()

            for col in data["original_data"].columns:
                if is_date_column(data["original_data"][col]):
                    data["original_data"][col] = pd.to_datetime(
                        data["original_data"][col], format="%m/%d/%Y", errors="coerce"
                    ).dt.strftime("%Y-%m-%d")

            data["agents_info"] = self.__extract(
                db, sql.querie_agents_data, source="Data Insured"
            )
            insured_new_data, insured_update_data = self.__insured(data=data)
            agents_new_data, agents_update_data = self.__agents(data=data)

            if not insured_new_data.empty:
                self.__load(
                    conn=db,
                    data=insured_new_data,
                    schema=const.DB_SCHEMA,
                    dest_table=const.INSURED_TABLE,
                    truncate=False,
                )
            if not insured_update_data.empty:
                update_query = utils.cte_update_query(
                    update_df=insured_update_data.copy(),
                    fields=utils.INSURED_UPDATE_COLUMNS,
                    update_query=sql.update_insured_data_query.as_string(db),
                )
                utils.postgresql_execute_query(
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_name,
                    host=self.db_host,
                    query_execute=update_query.as_string(db),
                )
                self.__logger.info(
                    f"The update of {insured_update_data.shape[0]} rows was made on table {const.INSURED_TABLE}."
                )

            if not agents_new_data.empty:
                self.__load(
                    conn=db,
                    data=agents_new_data,
                    schema=const.DB_SCHEMA,
                    dest_table=const.AGENTS_TABLE,
                    truncate=False,
                )
            if not agents_update_data.empty:
                update_query = utils.cte_update_query(
                    update_df=agents_update_data.copy(),
                    fields=utils.AGENTS_UPDATE_COLUMNS,
                    update_query=sql.update_agents_data_query.as_string(db),
                )
                utils.postgresql_execute_query(
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_name,
                    host=self.db_host,
                    query_execute=update_query.as_string(db),
                )
                self.__logger.info(
                    f"The update of {insured_update_data.shape[0]} rows was made on table {const.AGENTS_TABLE}."
                )

            data["policy_info"] = self.__extract(
                db, sql.querie_policy_data, source="Data Insured"
            )
            data["insured_info"] = self.__extract(
                db, sql.querie_insured_data, source="Data Insured"
            )
            data["agents_info"] = self.__extract(
                db, sql.querie_agents_data, source="Data Insured"
            )

            policy_new_data, policy_update_data = self.__policy(data=data)

            if not policy_new_data.empty:
                self.__load(
                    conn=db,
                    data=policy_new_data,
                    schema=const.DB_SCHEMA,
                    dest_table=const.POLICY_TABLE,
                    truncate=False,
                )
            if not policy_update_data.empty:
                update_query = utils.cte_update_query(
                    update_df=policy_update_data.copy(),
                    fields=utils.POLICY_UPDATE_COLUMNS,
                    update_query=sql.update_policy_data_query.as_string(db),
                )
                utils.postgresql_execute_query(
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_name,
                    host=self.db_host,
                    query_execute=update_query.as_string(db),
                )
                self.__logger.info(
                    f"The update of {insured_update_data.shape[0]} rows was made on table {const.POLICY_TABLE}."
                )

            data["policy_info"] = self.__extract(
                db, sql.querie_policy_data, source="Data Insured"
            )
            data["payments_info"] = self.__extract(
                db, sql.querie_payments_data, source="Data Insured"
            )
            data["claims_info"] = self.__extract(
                db, sql.querie_claims_data, source="Data Insured"
            )
            payments_new_data = self.__payments(data=data)
            claims_new_data = self.__claims(data=data)
            premium_new_data = self.__premium(data=data)

            if not payments_new_data.empty:
                self.__load(
                    conn=db,
                    data=payments_new_data,
                    schema=const.DB_SCHEMA,
                    dest_table=const.PAYMENTS_TABLE,
                    truncate=False,
                )

            if not claims_new_data.empty:
                self.__load(
                    conn=db,
                    data=claims_new_data,
                    schema=const.DB_SCHEMA,
                    dest_table=const.CLAIMS_TABLE,
                    truncate=False,
                )

            if not premium_new_data.empty:
                self.__load(
                    conn=db,
                    data=premium_new_data,
                    schema=const.DB_SCHEMA,
                    dest_table=const.PREMIUM_TABLE,
                    truncate=False,
                )

        except Exception as e:
            self.__logger.error("An error has occurred during process. ", e)
            raise
        finally:
            db.destroy_connection()

    def __insured(self, data):
        self.__logger.info(f"Staring transfomration to insured data")
        try:
            data_source = data["original_data"][const.INSURED_COLUMNS_CSV]
            data_current = data["insured_info"]
            data_merge = data_source.merge(
                data_current,
                on=["insured_name"],
                how="outer",
                suffixes=["", "_crr"],
                indicator=True,
            )
            new_data = data_merge[data_merge["_merge"] == "left_only"]
            if not new_data.empty:
                new_data = new_data[const.INSURED_COLUMNS_CSV]
                new_data["id"] = new_data.apply(lambda _: uuid.uuid4(), axis=1)
                new_data["created_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["insured_age"] = new_data["insured_age"].astype("Int64")
                new_data["insured_postal_code"] = new_data[
                    "insured_postal_code"
                ].astype("Int64")

            update_data = data_merge[data_merge["_merge"] == "both"]
            update_data = update_data.where(update_data.notnull(), None)
            if not update_data.empty:

                def find_different_columns(row):
                    different_columns = []
                    for i in range(0, len(row), 2):
                        if row.iloc[i] != row.iloc[i + 1]:
                            different_columns.append(row.index[i])
                    return different_columns if different_columns else "N"

                update_data["check_update"] = update_data[
                    [
                        "insured_gender",
                        "insured_gender_crr",
                        "insured_age",
                        "insured_age_crr",
                        "insured_address",
                        "insured_address_crr",
                        "insured_city",
                        "insured_city_crr",
                        "insured_state",
                        "insured_state_crr",
                        "insured_postal_code",
                        "insured_postal_code_crr",
                        "insured_country",
                        "insured_country_crr",
                        "insurance_company",
                        "insurance_company_crr",
                    ]
                ].apply(find_different_columns, axis=1)
                update_data = update_data[update_data["check_update"] != "N"]
                update_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                update_data = update_data[const.INSURED_COLUMNS_TABLE]
                update_data["insured_age"] = update_data["insured_age"].astype("Int64")
                update_data["insured_postal_code"] = update_data[
                    "insured_postal_code"
                ].astype("Int64")

        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform insured data. ", e
            )
            raise
        return new_data, update_data

    def __agents(self, data):
        self.__logger.info(f"Staring transfomration to insured data")
        try:
            data_source = data["original_data"][const.AGENTS_COLUMNS_CSV]
            data_current = data["agents_info"]
            data_merge = data_source.merge(
                data_current,
                on=["agent_email"],
                how="outer",
                suffixes=["", "_crr"],
                indicator=True,
            )
            new_data = data_merge[data_merge["_merge"] == "left_only"]
            if not new_data.empty:
                new_data = new_data[const.AGENTS_COLUMNS_CSV]
                new_data["id"] = new_data.apply(lambda _: uuid.uuid4(), axis=1)
                new_data["created_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )

            update_data = data_merge[data_merge["_merge"] == "both"]
            update_data = update_data.where(update_data.notnull(), None)
            if not update_data.empty:

                def find_different_columns(row):
                    different_columns = []
                    for i in range(0, len(row), 2):
                        if row.iloc[i] != row.iloc[i + 1]:
                            different_columns.append(row.index[i])
                    return different_columns if different_columns else "N"

                update_data["check_update"] = update_data[
                    ["agent_name", "agent_name_crr", "agent_phone", "agent_phone_crr"]
                ].apply(find_different_columns, axis=1)
                update_data = update_data[update_data["check_update"] != "N"]
                update_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                update_data = update_data[const.AGENTS_COLUMNS_TABLE]

        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform insured data. ", e
            )
            raise
        return new_data, update_data

    def __policy(self, data):
        self.__logger.info(f"Staring transfomration to insured data")
        try:
            data_source = data["original_data"][const.POLICYS_COLUMNS_CSV]
            data_current = data["policy_info"]
            agents_info = data["agents_info"]
            insured_info = data["insured_info"]
            data_source = data_source.merge(agents_info, how="left", on="agent_name")
            data_source.rename(columns={"id": "agent_id"}, inplace=True)
            data_source = data_source.merge(insured_info, how="left", on="insured_name")
            data_source.rename(columns={"id": "insured_id"}, inplace=True)
            data_source["policy_number"] = data_source["policy_number"].astype("Int64")
            data_current["policy_number"] = data_current["policy_number"].astype(
                "Int64"
            )
            data_merge = data_source.merge(
                data_current,
                on=["policy_number"],
                how="outer",
                suffixes=["", "_crr"],
                indicator=True,
            )
            new_data = data_merge[data_merge["_merge"] == "left_only"]
            if not new_data.empty:
                new_data["id"] = new_data.apply(lambda _: uuid.uuid4(), axis=1)
                new_data["created_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["policy_number"] = new_data["policy_number"].astype("Int64")

                new_data = new_data[const.POLICYS_COLUMNS_TABLE]

            update_data = data_merge[data_merge["_merge"] == "both"]
            update_data = update_data.where(update_data.notnull(), None)
            if not update_data.empty:

                def find_different_columns(row):
                    different_columns = []
                    for i in range(0, len(row), 2):
                        if row.iloc[i] != row.iloc[i + 1]:
                            different_columns.append(row.index[i])
                    return different_columns if different_columns else "N"

                update_data["check_update"] = update_data[
                    [
                        "policy_start_date",
                        "policy_start_date_crr",
                        "policy_end_date",
                        "policy_end_date_crr",
                        "policy_type",
                        "policy_type_crr",
                        "insured_id",
                        "insured_id_crr",
                        "agent_id",
                        "agent_id_crr",
                    ]
                ].apply(find_different_columns, axis=1)
                update_data = update_data[update_data["check_update"] != "N"]
                update_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                update_data = update_data[const.POLICYS_COLUMNS_TABLE]
                update_data["policy_number"] = update_data["policy_number"].astype(
                    "Int64"
                )

        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform insured data. ", e
            )
            raise
        return new_data, update_data

    def __payments(self, data):
        self.__logger.info(f"Staring transfomration to insured data")
        try:
            data_source = data["original_data"][const.PAYMENTS_COLUMNS_CSV]
            policy_info = data["policy_info"]
            data_current = data["payments_info"]
            policy_info["policy_number"] = policy_info["policy_number"].astype("Int64")
            data_source["policy_number"] = data_source["policy_number"].astype("Int64")
            data_source = data_source.merge(policy_info, how="left", on="policy_number")
            data_source.rename(columns={"id": "policy_id"}, inplace=True)

            data_merge = data_source.merge(
                data_current,
                on=["policy_id"],
                how="outer",
                suffixes=["", "_crr"],
                indicator=True,
            )
            new_data = data_merge[data_merge["_merge"] == "left_only"]
            if not new_data.empty:
                new_data["id"] = new_data.apply(lambda _: uuid.uuid4(), axis=1)
                new_data["created_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["policy_number"] = new_data["policy_number"].astype("Int64")

                new_data = new_data[const.PAYMENTS_COLUMNS_TABLE]

        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform insured data. ", e
            )
            raise
        return new_data

    def __claims(self, data):
        self.__logger.info(f"Staring transfomration to insured data")
        try:
            data_source = data["original_data"][const.CLAIMS_COLUMNS_CSV]
            policy_info = data["policy_info"]
            data_current = data["claims_info"]
            policy_info["policy_number"] = policy_info["policy_number"].astype("Int64")
            data_source["policy_number"] = data_source["policy_number"].astype("Int64")
            data_source = data_source.merge(policy_info, how="left", on="policy_number")
            data_source.rename(columns={"id": "policy_id"}, inplace=True)

            data_merge = data_source.merge(
                data_current,
                on=["policy_id"],
                how="outer",
                suffixes=["", "_crr"],
                indicator=True,
            )
            new_data = data_merge[data_merge["_merge"] == "left_only"]
            if not new_data.empty:
                new_data["id"] = new_data.apply(lambda _: uuid.uuid4(), axis=1)
                new_data["created_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["policy_number"] = new_data["policy_number"].astype("Int64")

                new_data = new_data[const.CLAIMS_COLUMNS_TABLE]

        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform insured data. ", e
            )
            raise
        return new_data

    def __premium(self, data):
        self.__logger.info(f"Staring transfomration to insured data")
        try:
            data_source = data["original_data"][const.PREMIUM_COLUMNS_CSV]
            policy_info = data["policy_info"]
            data_current = data["claims_info"]
            policy_info["policy_number"] = policy_info["policy_number"].astype("Int64")
            data_source["policy_number"] = data_source["policy_number"].astype("Int64")
            data_source = data_source.merge(policy_info, how="left", on="policy_number")
            data_source.rename(columns={"id": "policy_id"}, inplace=True)

            data_merge = data_source.merge(
                data_current,
                on=["policy_id"],
                how="outer",
                suffixes=["", "_crr"],
                indicator=True,
            )
            new_data = data_merge[data_merge["_merge"] == "left_only"]
            if not new_data.empty:
                new_data["id"] = new_data.apply(lambda _: uuid.uuid4(), axis=1)
                new_data["created_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["updated_at"] = pd.to_datetime(
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                )
                new_data["policy_number"] = new_data["policy_number"].astype("Int64")

                new_data = new_data[const.PREMIUM_COLUMNS_TABLE]

        except Exception as e:
            self.__logger.error(
                f"An error has occurred during transform insured data. ", e
            )
            raise
        return new_data
