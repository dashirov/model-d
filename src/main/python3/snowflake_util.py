import hashlib
import random
import time
from typing import Dict, List, Tuple

random.seed()

logger = None


def configure_connection(conn):
    conn.execute("alter session set abort_detached_query=true")
    conn.execute("alter session set lock_timeout=60")
    conn.execute("alter session set timezone='UTC'")


def get_latest_sync_time(conn):
    """Returns a datetime.datetime object with the approximate time of the most recent Fivetran sync."""
    # Check several of the most frequently updated tables
    sql = """
    SELECT max(_fivetran_synced) FROM (
      SELECT max(_fivetran_synced) AS _fivetran_synced FROM fivetran.opdb_public.event_logs
      UNION ALL SELECT max(_fivetran_synced) AS _fivetran_synced FROM fivetran.opdb_public.delivery_meals
      UNION ALL SELECT max(_fivetran_synced) AS _fivetran_synced FROM fivetran.opdb_public.ledger_items
      UNION ALL SELECT max(_fivetran_synced) AS _fivetran_synced FROM fivetran.opdb_public.leads
    )
    """
    row = conn.execute(sql).first()
    return row[0] if row else None


def store(
    engine,
    dest_table: str,
    columns: List[str],
    csv_name: str,
    primary_key=None,
    primary_key_values=None,
    skip_header=0
):
    """Stores CSV data into a Snowflake table using the COPY command.

    :param engine: SqlAlchemy engine
    :param dest_table: name of table into which to store records
    :param columns: list of column names to be stored
    :param csv_name: local file containing CSV data to be stored
    :param primary_key: if present, will delete records from dest_table for
           which these column values match those for a record being stored
    :param primary_key_values: if present, will delete records from dest_table
           for which the primary_key column matches any of these values
           (for now, only supports a single primary_key column)
    :param skip_header: number of header rows in CSV file
    """
    temp_table = None
    suffix = __make_unique_suffix(dest_table)

    # Ensure these statements execute as a single transaction
    # -- see https://docs.sqlalchemy.org/en/13/core/connections.html#using-transactions
    with engine.begin() as conn:
        configure_connection(conn)

        stage_name = __create_stage(conn, dest_table, suffix)
        __put(conn, csv_name, stage_name)

        if primary_key_values:
            # Delete existing rows with specified primary key values
            # For now, only allow a single primary key column -- otherwise the SQL gets complicated
            if not primary_key:
                raise ValueError("Must specify primary key column")
            elif not isinstance(primary_key, str):
                raise ValueError("Currently supports only a single primary key column")
            # This produces
            #   DELETE FROM "freshly.demand_planner.MODEL_B1_SURVIVAL_FUNCTION" WHERE ship_date IN (%(ship_date_1)s)
            # stmt = table(dest_table).delete().where(literal_column(primary_key).in_(primary_key_values))
            # sql = str(stmt.compile(engine))
            sql = f"DELETE FROM {dest_table} WHERE {primary_key} IN ({format_sql_value(primary_key_values)})"
            __log(sql)
            conn.execute(sql)
        elif primary_key:
            # If we need to delete existing rows with unknown values, copy into a temp table
            temp_table += "_temp_" + suffix
            sql = f"CREATE TEMPORARY TABLE {temp_table} LIKE {dest_table}"
            __log(sql)
            conn.execute(sql)

        __copy(conn, columns, stage_name, dest_table, skip_header)

        if temp_table:
            # Delete rows from the final table that match uploaded rows
            sql = f"DELETE FROM {dest_table} t1 USING {temp_table} t2"
            delim = " WHERE"
            for column in primary_key:
                sql += f"{delim} t1.{column} = t2.{column}"
                delim = " AND"
            __log(sql)
            conn.execute(sql)
            # Insert the rows from the temp table into the final table
            column_str = ",".join(columns)
            sql = (
                f"INSERT INTO {dest_table} ({column_str})"
                f"  SELECT {column_str} from {temp_table}"
            )
            __log(sql)
            conn.execute(sql)
            # The temp table should be dropped automatically when the connection
            # is closed, but doesn't hurt to drop it explicitly
            sql = f"DROP TABLE IF EXISTS {temp_table}"
            __log(sql)
            conn.execute(sql)

        # The stage should be dropped automatically when the connection is closed,
        # but doesn't hurt to drop it explicitly
        conn.execute(f"DROP STAGE {stage_name}")


def __make_unique_suffix(dest_table: str) -> str:
    timestamp = time.time()
    random_value = random.random()
    hash_key = f"{dest_table}:{timestamp}:{random_value}"
    # The hash is 64 characters; Snowflake supports identifiers up to 256 chars
    # https://docs.snowflake.net/manuals/sql-reference/identifiers.html
    return hashlib.sha256(hash_key.encode()).hexdigest()


def __create_stage(conn, dest_table: str, suffix: str) -> str:
    # Ensure that the stage name is unique
    stage_name = dest_table + "_" + suffix

    # Create a temporary stage so that if there's an error that causes the
    # input to be incapable of loading, it won't be stuck there forever
    # until we drop the stage.  The temporary stage will be dropped when
    # the session (connection) is closed.
    sql = f"CREATE TEMPORARY STAGE {stage_name}"
    __log(sql)
    conn.execute(sql)
    return stage_name


def __put(conn, csv_file: str, stage_name: str):
    sql = f"PUT file://{csv_file} @{stage_name} AUTO_COMPRESS=TRUE"
    __log(sql)
    conn.execute(sql)


def __copy(conn, columns: List[str], stage_name: str, dest_table: str, skip_header: int):
    column_str = ", ".join(columns)
    csv_columns = ", ".join(["t.$" + str(i) for i in range(1, len(columns) + 1)])
    sql = f"""COPY INTO {dest_table} ({column_str}) 
        FROM (SELECT {csv_columns} FROM @{stage_name} t)
        FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' DATE_FORMAT='AUTO' TIMESTAMP_FORMAT='AUTO' SKIP_HEADER={skip_header}
            FIELD_OPTIONALLY_ENCLOSED_BY='"' EMPTY_FIELD_AS_NULL=TRUE ENCODING=UTF8 NULL_IF='')
        ON_ERROR=ABORT_STATEMENT PURGE=TRUE
        """
    __log(sql)
    conn.execute(sql)


def format_sql_value(v):
    if isinstance(v, List):
        return ", ".join([format_sql_value(val) for val in v])
    elif isinstance(v, int) or isinstance(v, float) or isinstance(v, bool):
        return str(v)
    else:
        return "'" + v.replace("'", "''") + "'"


def __log(msg):
    if logger:
        logger.debug(msg)
