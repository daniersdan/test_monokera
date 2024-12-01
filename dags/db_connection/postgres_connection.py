import io
import pandas as pd
import psycopg2
import sqlalchemy
from abc import ABC

class DataConnection(ABC):

    def create_connection(self):
        pass

    def execute_query(self, query):
        pass

    def update_query(self, query, values):
        pass

    def execute_insert(self, data_frame, table_name, truncate):
        pass

    def execute_upsert(self, data_frame, table_name, primary_keys):
        pass

    def destroy_connection(self):
        pass

    def google_sheet_to_df(self, sheet_name):
        pass


class PostgreSQLConnection(DataConnection):
    def __init__(
        self,
        user,
        password,
        host_name,
        database,
        schema="public",
        engine=None,
        connection=None,
    ):
        self.user = user
        self.password = password
        self.host_name = host_name
        self.database = database
        self.schema = schema
        self.engine = engine
        self.connection = connection

    def create_connection(self):
        try:
            self.engine = sqlalchemy.create_engine(
                "postgresql+psycopg2://{}:{}@{}/{}".format(
                    self.user, self.password, self.host_name, self.database
                )
            )
            self.connection = self.engine.connect()
        except Exception as ex:
            print("Exception while creating the postgresql connection {}".format(ex))
            raise

    def update_query(self, query, values):
        try:
            conn = self.engine.connect()
            cursor = conn.connection.cursor()

            psycopg2.extras.execute_values(
                cursor, query.as_string(cursor), values, template=None, page_size=100
            )
            conn.connection.commit()

        except Exception as ex:
            print("Exception while executing the update query {}").format(ex)
            raise

    def execute_query(self, query):
        try:
            return self.connection.execute(query)
        except Exception as ex:
            print("Exception while executing the postgresql query {}".format(ex))
            raise

    def execute_upsert(self, data_frame, schema, table_name, primary_keys, sep="|"):
        to_be_deleted = data_frame[[x[0] for x in primary_keys]].drop_duplicates()
        if len(to_be_deleted.index) < len(data_frame.index):
            raise ValueError(
                "Primary key constraint is violated in passed `data_frame`."
            )
        del_stream = io.StringIO()
        del_stream.write(
            to_be_deleted.to_csv(sep=sep, encoding="utf8", index=False, header=False)
        )
        del_stream.seek(0)
        try:
            conn = self.engine.connect()
            curs = conn.connection.cursor()
            curs.execute(
                """CREATE TEMP TABLE to_be_deleted_{0}
                            (
                                {1}
                            )""".format(
                    table_name, ",\n".join(["{} {}".format(*x) for x in primary_keys])
                )
            )
            curs.copy_from(del_stream, "to_be_deleted_{0}".format(table_name), sep=sep)
            curs.execute(
                """DELETE FROM
                                {0}.{1}
                            WHERE
                            ({2}) IN (SELECT {2} FROM to_be_deleted_{1})
                            """.format(
                    schema, table_name, ", ".join([x[0] for x in primary_keys])
                )
            )
            self.execute_insert(
                data_frame=data_frame,
                schema=schema,
                table_name=table_name,
                truncate=False,
                sep=sep,
            )
            conn.connection.commit()
        except Exception as exception:
            print(
                "Failure while upsert data to the table {}. {}".format(
                    table_name, exception
                )
            )
            raise exception
        finally:
            if curs is not None:
                curs.close()
            if conn is not None:
                conn.connection.close()

    def execute_insert(self, data_frame, schema, table_name, truncate=False, sep="|"):
        try:

            pandas_sql_engine = pd.io.sql.pandasSQL_builder(self.engine, schema=schema)
            table = pd.io.sql.SQLTable(
                name=table_name,
                pandas_sql_engine=pandas_sql_engine,
                frame=data_frame,
                index=False,
                schema=schema,
                if_exists="fail",
            )

            columns = (
                str(list(data_frame.columns))
                .replace("[", "")
                .replace("]", "")
                .replace("'", "")
            )

            if not table.exists():
                table.create()
            if truncate:
                self.execute_query("TRUNCATE TABLE {}.{}".format(schema, table_name))
            string_data_io = io.StringIO()
            data_frame.to_csv(string_data_io, sep=sep, index=False)
            string_data_io.seek(0)
            with self.engine.connect() as connection:
                try:
                    with connection.connection.cursor() as cursor:
                        copy_cmd = (
                            "COPY %s.%s (%s) FROM STDIN HEADER DELIMITER '%s' CSV"
                            % (
                                schema,
                                table_name,
                                columns,
                                sep,
                            )
                        )
                        cursor.copy_expert(copy_cmd, string_data_io)
                    connection.connection.commit()
                finally:
                    if connection is not None:
                        connection.connection.close()
                    if cursor is not None:
                        cursor.close()

        except ValueError as exception:
            print(
                "Failure while inserting data to the table {}. {}".format(
                    table_name, exception
                )
            )
            raise

    def create_schema(self, schema: str) -> bool:
        create_schema = f"""
            CREATE SCHEMA {schema};
        """

        has_schema = self.has_schema(schema)
        if not has_schema:
            _ = self.connection.execute(create_schema)
        return has_schema

    def has_schema(self, schema: str) -> bool:
        has_schema = self.connection.engine.dialect.has_schema(
            schema=schema, connection=self.connection
        )

        return has_schema

    def create_table(
        self,
        schema: str,
        table_name: str,
        df_signature: pd.DataFrame,
        exists: bool = False,
    ) -> bool:
        exists = self.has_table(schema, table_name)
        if not exists:
            df_signature.to_sql(
                table_name,
                self.connection,
                schema=schema.lower(),
                if_exists="append",
                index=False,
            )
            exists = True
        else:
            exists = False

        return exists

    def has_table(self, schema: str, table_name: str) -> bool:
        exists = self.connection.engine.has_table(table_name, schema=schema)

        return exists

    def destroy_connection(self):
        try:
            if self.connection is not None:
                self.connection.close()
        except Exception as ex:
            print("Exception while destroying the postgresql connection {}".format(ex))
            raise
