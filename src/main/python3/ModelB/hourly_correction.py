import pandas as pd
from sqlalchemy.sql import text
import inspect
import boto3
import tempfile
import time
from typing import Dict, List, Tuple

import snowflake_util


LOG_FORMAT = "[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s"

# In Python 3.7, dictionary iteration order is guaranteed to be insertion order
OUTPUT_COLUMNS = {
    'CHECKPOINT': 'TIMESTAMP_TZ',
    'FACILITY_ID': 'INTEGER',
    'FREIGHT_ID': 'INTEGER',
    'ALIGNMENT_TIMESTAMP': 'TIMESTAMP_TZ',
    'FINALIZATION_TIMESTAMP': 'TIMESTAMP_TZ',
    'PREDICTED_CON': 'NUMBER(38,15)',
    'PREDICTED_COR': 'NUMBER(38,15)',
    'PREDICTED_COA': 'NUMBER(38,15)',
    'PREDICTED_ORDERS': 'NUMBER(38,15)',
    'CON': 'NUMBER(38,15)',
    'COR': 'NUMBER(38,15)',
    'COA': 'NUMBER(38,15)',
    'ORDERS': 'NUMBER(38,15)',
    'DELTA_CON': 'NUMBER(38,15)',
    'ADJUSTED_CON': 'NUMBER(38,15)',
    'DELTA_COR': 'NUMBER(38,15)',
    'ADJUSTED_COR': 'NUMBER(38,15)',
    'DELTA_COA': 'NUMBER(38,15)',
    'ADJUSTED_COA': 'NUMBER(38,15)',
    'SHIP_DATE': 'DATE NOT NULL'
}


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


class SelfCorrection:
    def __init__(self, engine, s3_base_url, aws_key_id=None, aws_secret_key=None,
                 table_name='FRESHLY.ADHOC.MODEL_D_PREDICTED_OUTCOMES',
                 survival_table_name='FRESHLY.ADHOC.MODEL_B_TRAINING_SURVIVAL_FUNCTION',
                 logger=None):
        if engine is not None:
            self.engine = engine
        else:
            raise ValueError("Expecting engine to be passed at initialization")

        self.s3_base_url = s3_base_url
        self.aws_key_id = aws_key_id
        self.aws_secret_key = aws_secret_key
        self.table_name = table_name
        self.survival_table_name = survival_table_name
        self.logger = logger

        self.info("Initializing Model D daily/hourly correction process")

        self.s3client = boto3.client("s3")

    def set_logger(self, logger):
        self.logger = logger

    def execute(self, ship_dates: List[str],
                knowledge_time_utc: str = None,
                lambda_context=None) -> List[str]:
        max_time = 0
        with self.engine.connect() as conn:
            snowflake_util.configure_connection(conn)

            if knowledge_time_utc:
                self.info(f"Using knowledge time {knowledge_time_utc}")
            else:
                # Only use data from last completely synced hour
                # Assume that if we have any data from a given hour, the previous hour has been completely synced
                last_sync_time = snowflake_util.get_latest_sync_time(conn)
                knowledge_time_utc = last_sync_time.replace(minute=0, second=0, microsecond=0).isoformat()
                self.info(f"Last sync time {last_sync_time.isoformat()}, knowledge time {knowledge_time_utc}")

            self.create_table(conn)
            if not ship_dates:
                ship_dates = self.processing_plan(conn, knowledge_time_utc)
            self.info(f"Ship dates to process: {ship_dates}")
            for i, ship_date in enumerate(ship_dates):
                seconds_remaining = lambda_context.get_remaining_time_in_millis() / 1000 if lambda_context \
                    else 999999999
                if seconds_remaining < max_time * 2:
                    self.info(f"Too little time remaining ({seconds_remaining} seconds) "
                              f"vs. max time {max_time} seconds to process a single ship date")
                    return ship_dates[i:]

                start = time.time()
                self.info(f"Processing ship date {ship_date}: "
                          f"{seconds_remaining} seconds remaining for Lambda")
                prediction = self.update_ship_date_prediction(conn, ship_date, knowledge_time_utc)
                self.store_prediction(ship_date=ship_date, dataframe=prediction)
                end = time.time()
                elapsed = end - start
                if elapsed > max_time:
                    max_time = elapsed
                self.info(f"Processed ship date {ship_date} in {elapsed} seconds")
        return []

    def processing_plan(self, connection, knowledge_time_utc: str = 'CURRENT_TIMESTAMP') -> List[str]:
        connection.execute(
            text(
                """
                SET KNOWLEDGE_TIME = CASE :knowledge_time
                                     WHEN 'CURRENT_TIMESTAMP' THEN CURRENT_TIMESTAMP 
                                     ELSE TO_TIMESTAMP_TZ( :knowledge_time ) 
                                     END
                """
            ),
            knowledge_time=knowledge_time_utc,
        )

        sql = text(
            """
        
        SELECT DISTINCT "DELIVERIES"."SHIP_DATE"::TEXT AS SHIP_DATE 
        FROM "DELIVERIES" 
        WHERE $KNOWLEDGE_TIME >= DELIVERIES.CREATED_AT
         AND DATEADD('DAY', -3, DELIVERIES.SHIP_DATE) >= TO_DATE($KNOWLEDGE_TIME)   
        ORDER BY SHIP_DATE DESC
        """
        )
        # sql = "SELECT DISTINCT DELIVERIES.SHIP_DATE::TEXT SHIP_DATE FROM DELIVERIES   ORDER BY 1 DESC"

        self.debug(sql)
        targets = pd.read_sql_query(
            sql, connection, params={"knowledge_time": "CURRENT_TIMESTAMP"}
        )
        return [row.ship_date for index, row in targets.iterrows()]

    def update_ship_date_prediction(self, connection, ship_date, knowledge_time_utc: str = 'CURRENT_TIMESTAMP'):
        """
        Retrieves actual number of skips, compares with predicted survival, and adjusts accordingly.

        All calculations should be done using only data that would have been available as of the specified
        knowledge time (in UTC).
        """
        self.debug("Updating {0}".format(ship_date))
        connection.execute(
            text("""SET SHIP_DATE = TO_DATE( :ship_date )"""), ship_date=ship_date
        )
        connection.execute(
            text(
                """
                    SET KNOWLEDGE_TIME = CASE :knowledge_time
                                         WHEN 'CURRENT_TIMESTAMP' THEN CURRENT_TIMESTAMP 
                                         ELSE TO_TIMESTAMP_TZ( :knowledge_time ) 
                                         END
                                         """
            ),
            knowledge_time=knowledge_time_utc,
        )

        sql = text(
# language=SQL
"""
WITH FIRST_ORDERS AS (
    -- First order for each subscription
    SELECT SUBSCRIPTION_ID,
           MIN(START_DATE)::DATE AS START_DATE
    FROM WEEKLY_ORDERS
    WHERE WEEKLY_ORDERS.CREATED_AT <= $KNOWLEDGE_TIME  -- records created prior to knowledge time (CREATED_AT is UTC)
    GROUP BY 1
),
TIMEZONES AS (
    -- A place to control for local timezones, in the future expect these to be present in the operational database
    -- FACILITY TIMEZONE IS NECESSARY TO COMPUTE CENSORSHIP TIMESTAMPS
    SELECT 1 AS FACILITY_ID, 'US/Arizona' AS FACILITY_TIMEZONE
    UNION
    SELECT 2 AS FACILITY_ID, 'US/Eastern' AS FACILITY_TIMEZONE
    UNION
    SELECT 3 AS FACILITY_ID, 'US/Eastern' AS FACILITY_TIMEZONE
),
"CHECKPOINTS" AS (
    -- CHECKPOINT_NUMBER is the number of hours before midnight at the start of the ship date
    -- so it counts down: the larger the CHECKPOINT_NUMBER, the earlier the CHECKPOINT timestamp
    -- CHECKPOINT has no time zone
    /* Snowflake says the seq2() function "does not necessarily produce a gap-free sequence"
    SELECT seq2()                                                     AS CHECKPOINT_NUMBER,
           DATEADD('HOUR', -1 * seq2(), TO_TIMESTAMP_NTZ($SHIP_DATE)) AS "CHECKPOINT"
    FROM table (generator(rowcount => 650))
    */
    SELECT 'foo',           row_number() OVER (PARTITION BY 1 ORDER BY 1) - 1                                 AS CHECKPOINT_NUMBER, 
        dateadd(HOUR, -1 * (row_number() OVER (PARTITION BY 1 ORDER BY 1) - 1), TO_TIMESTAMP_NTZ($SHIP_DATE)) AS CHECKPOINT
    FROM TABLE (generator(rowcount => 650))
),
"EVENTS" AS (
    -- Events that affect an order's "alive" status: creation, skips, and unskips
    -- Includes only events known as of KNOWLEDGE_TIME
    SELECT WEEKLY_ORDERS.ID         AS WEEKLY_ORDER_ID,
           WEEKLY_ORDERS.CREATED_AT AS EVENT_TIME,
           TRUE                     AS ORDER_IS_ALIVE
    FROM WEEKLY_ORDERS
             JOIN DELIVERIES
                  ON (DELIVERIES.WEEKLY_ORDER_ID = WEEKLY_ORDERS.ID AND DELIVERIES.SHIP_DATE = $SHIP_DATE)
    WHERE WEEKLY_ORDERS.CREATED_AT <= $KNOWLEDGE_TIME
        AND DELIVERIES.CREATED_AT <= $KNOWLEDGE_TIME
    UNION
    SELECT WEEKLY_ORDERS.ID      AS WEEKLY_ORDER_ID,
           EVENT_LOGS.CREATED_AT AS EVENT_TIME,
           IFF(EVENT_LOGS.CHANGE_SET:status[1] IN ('not_paid', 'skipped_closed', 'skipped_open', 'admin_canceled_closed', 'admin_canceled_open'),
               FALSE, TRUE)      AS ORDER_IS_ALIVE
    FROM WEEKLY_ORDERS
        JOIN DELIVERIES ON (DELIVERIES.WEEKLY_ORDER_ID = WEEKLY_ORDERS.ID AND 
                            DELIVERIES.SHIP_DATE = $SHIP_DATE)
        JOIN EVENT_LOGS ON (EVENT_LOGS.OBJECT_TYPE = 'deliveries' AND 
                            EVENT_LOGS.OBJECT_ID = DELIVERIES.ID AND
                            EVENT_LOGS.CHANGE_SET:status IS NOT NULL)
    WHERE WEEKLY_ORDERS.CREATED_AT <= $KNOWLEDGE_TIME
        AND DELIVERIES.CREATED_AT <= $KNOWLEDGE_TIME
        AND EVENT_LOGS.CREATED_AT <= $KNOWLEDGE_TIME
),
ENUMERATED AS (
    -- Events with checkpoints
    -- Includes only events known as of KNOWLEDGE_TIME
    SELECT WEEKLY_ORDER_ID,
           CHECKPOINT_NUMBER,
           "CHECKPOINT",
           ORDER_IS_ALIVE,
           ROW_NUMBER() OVER (PARTITION BY CHECKPOINT_NUMBER, WEEKLY_ORDER_ID ORDER BY EVENT_TIME DESC) AS RECENCY
    FROM "EVENTS"
             JOIN CHECKPOINTS ON (EVENT_TIME <= "CHECKPOINT" AND "CHECKPOINT" <= CURRENT_TIMESTAMP AND
                                  "CHECKPOINT" <= $KNOWLEDGE_TIME) --IMPORTANT
),
ACTUALS AS (
    -- Live orders as of KNOWLEDGE_TIME
    SELECT "CHECKPOINT",
           FACILITY_ID,
           FREIGHT_ID,
           SUM(IFF(IS_FIRST_FOR_SUBSCRIPTION, DELIVERIES.NUMBER_OF_MEALS, 0))     AS CON,
           SUM(IFF(NOT IS_FIRST_FOR_SUBSCRIPTION, DELIVERIES.NUMBER_OF_MEALS, 0)) AS COR,
           SUM(DELIVERIES.NUMBER_OF_MEALS)                                        AS COA,
           SUM(1)                                                                 AS ORDERS
    FROM ENUMERATED
             JOIN DELIVERIES USING (WEEKLY_ORDER_ID)
    WHERE RECENCY = 1
        AND ORDER_IS_ALIVE = TRUE
    GROUP BY 1, 2, 3
),
DELIVERY_ORDINALS AS (
    -- Calculates tenure in weeks (1-4+), finalization timestamp, birth hour, etc. for each delivery
    -- DATEADD(HOUR, n, DATE) returns a TIMESTAMP_NTZ by default, so it is converted to a TIMESTAMP_TZ 
    -- using the facility time zone 
    SELECT
        WEEKLY_ORDERS.ID AS WEEKLY_ORDER_ID,
        DELIVERIES.FACILITY_ID,
        DELIVERIES.FREIGHT_ID,
        DELIVERIES.PLAN_ID,
        DELIVERIES.NUMBER_OF_MEALS,
        DELIVERIES.IS_FIRST_FOR_SUBSCRIPTION,
        LEAST(1 + DATEDIFF('WEEK', FIRST_ORDERS.START_DATE, WEEKLY_ORDERS.START_DATE), 4) AS TENURE,
        FIVETRAN.OPDB_PUBLIC.udf_ntz_to_tz( DATEADD(
                        'hour', +17, DELIVERIES.SHIP_DATE), TIMEZONES.FACILITY_TIMEZONE ) ALIGNMENT_TIMESTAMP,
        FIVETRAN.OPDB_PUBLIC.udf_ntz_to_tz( DATEADD(
                        'hour', +17, DATEADD(
                        'day', -3, DELIVERIES.SHIP_DATE)), TIMEZONES.FACILITY_TIMEZONE ) FINALIZATION_TIMESTAMP,
        DATE_TRUNC('HOUR', WEEKLY_ORDERS.CREATED_AT) AS BIRTH_HOUR,
        DELIVERIES.SHIP_DATE
    FROM FIRST_ORDERS
        JOIN FIVETRAN.OPDB_PUBLIC.WEEKLY_ORDERS ON FIRST_ORDERS.SUBSCRIPTION_ID = WEEKLY_ORDERS.SUBSCRIPTION_ID
        JOIN FIVETRAN.OPDB_PUBLIC.DELIVERIES ON DELIVERIES.WEEKLY_ORDER_ID = WEEKLY_ORDERS.ID
        JOIN TIMEZONES ON DELIVERIES.FACILITY_ID = TIMEZONES.FACILITY_ID
    WHERE DELIVERIES.SHIP_DATE = $SHIP_DATE
),
ORDERS AS (
    -- Counts number of orders for each group and birth hour
    SELECT 
        DELIVERY_ORDINALS.BIRTH_HOUR,
        DELIVERY_ORDINALS.FACILITY_ID || '-' ||
            DELIVERY_ORDINALS.FREIGHT_ID || '-' ||
            DELIVERY_ORDINALS.PLAN_ID || '-' ||
            DELIVERY_ORDINALS.TENURE AS GRP,
        DELIVERY_ORDINALS.IS_FIRST_FOR_SUBSCRIPTION,
        DELIVERY_ORDINALS.ALIGNMENT_TIMESTAMP,
        DELIVERY_ORDINALS.FINALIZATION_TIMESTAMP,
        DELIVERY_ORDINALS.NUMBER_OF_MEALS,
        DELIVERY_ORDINALS.SHIP_DATE,
        DELIVERY_ORDINALS.FACILITY_ID,
        DELIVERY_ORDINALS.FREIGHT_ID,
        COUNT(DISTINCT(WEEKLY_ORDER_ID)) AS ORDERS
    FROM DELIVERY_ORDINALS
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
PREDICTED AS (
    -- Applies survival rate to each group of orders
    SELECT
        ORDERS.ALIGNMENT_TIMESTAMP,
        ORDERS.FINALIZATION_TIMESTAMP,
        -- Align order birth time with 1am on the ship date (no time zone) in reverse order
        DATEADD('HOUR', CHECKPOINTS.CHECKPOINT_NUMBER + 1, ORDERS.BIRTH_HOUR) AS "CHECKPOINT",
        ORDERS.FACILITY_ID,
        ORDERS.FREIGHT_ID,
        SUM(IFF(ORDERS.IS_FIRST_FOR_SUBSCRIPTION, 
                ORDERS.ORDERS * ORDERS.NUMBER_OF_MEALS * COALESCE(MODEL_B_SURVIVAL_FUNCTION.SURVIVAL_RATE, MODEL_B_SURVIVAL_FUNCTION_MISSING.SURVIVAL_RATE), 
                0)) 
            AS "CON≈",
        SUM(IFF(NOT ORDERS.IS_FIRST_FOR_SUBSCRIPTION, 
                ORDERS.ORDERS * ORDERS.NUMBER_OF_MEALS * COALESCE(MODEL_B_SURVIVAL_FUNCTION.SURVIVAL_RATE, MODEL_B_SURVIVAL_FUNCTION_MISSING.SURVIVAL_RATE), 
                0)) 
            AS "COR≈",
        SUM(ORDERS.ORDERS * ORDERS.NUMBER_OF_MEALS * COALESCE(MODEL_B_SURVIVAL_FUNCTION.SURVIVAL_RATE, MODEL_B_SURVIVAL_FUNCTION_MISSING.SURVIVAL_RATE)) 
            AS "COA≈",
        SUM(ORDERS.ORDERS * COALESCE(MODEL_B_SURVIVAL_FUNCTION.SURVIVAL_RATE, MODEL_B_SURVIVAL_FUNCTION_MISSING.SURVIVAL_RATE)) 
            AS "ORDERS≈"
    
    FROM ORDERS
        LEFT JOIN :survival_table_name MODEL_B_SURVIVAL_FUNCTION
            ON (TO_DATE(MODEL_B_SURVIVAL_FUNCTION.SHIP_DATE)=TO_DATE(ORDERS.SHIP_DATE)
            AND MODEL_B_SURVIVAL_FUNCTION.GRP=ORDERS.GRP)
    
        -- If there is no survival curve for this freight (because it was recently added), use the survival curve for
        -- the entire facility (freight ID 0)
        LEFT JOIN :survival_table_name MODEL_B_SURVIVAL_FUNCTION_MISSING
            ON ( TO_DATE(MODEL_B_SURVIVAL_FUNCTION_MISSING.SHIP_DATE)=TO_DATE(ORDERS.SHIP_DATE)
            -- AND MODEL_B_SURVIVAL_FUNCTION.GRP is NULL
            AND MODEL_B_SURVIVAL_FUNCTION_MISSING.GRP = 
                split(ORDERS.GRP, '-')[0] || '-' || 
                '0'                       || '-' ||
                split(ORDERS.GRP, '-')[2] || '-' ||
                split(ORDERS.GRP, '-')[3]
            -- AND MODEL_B_SURVIVAL_FUNCTION_MISSING.GRP=ORDERS.GRP
            )
            AND (MODEL_B_SURVIVAL_FUNCTION.TIMELINE IS NULL OR MODEL_B_SURVIVAL_FUNCTION_MISSING.TIMELINE = MODEL_B_SURVIVAL_FUNCTION.TIMELINE)
    
        JOIN CHECKPOINTS ON (
            COALESCE(MODEL_B_SURVIVAL_FUNCTION.TIMELINE,MODEL_B_SURVIVAL_FUNCTION_MISSING.TIMELINE) = CHECKPOINTS.CHECKPOINT_NUMBER AND
            -- TODO: should this be CHECKPOINTS.CHECKPOINT_NUMBER + 1 as in the select?
            DATEADD('HOUR', CHECKPOINTS.CHECKPOINT_NUMBER, ORDERS.BIRTH_HOUR) <= ORDERS.ALIGNMENT_TIMESTAMP
        )
    GROUP BY 1, 2, 3, 4, 5
),
COMPLETE_PREDICTION AS (
    -- Computes delta and adjusted prediction for each group of orders, and joins with ACTUALS
    SELECT *,
        IFF("CON≈" <> 0,
            LAST_VALUE("CON" / NULLIF("CON≈", 0))
                IGNORE NULLS
                OVER (
                    PARTITION BY FACILITY_ID, FREIGHT_ID
                    ORDER BY "CHECKPOINT"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 
            NULL)
            AS "CON Δ",
        IFF("CON" IS NULL AND "CON≈" <> 0,
            "CON≈" * LAST_VALUE("CON" / NULLIF("CON≈", 0))
                IGNORE NULLS
                OVER (
                    PARTITION BY FACILITY_ID, FREIGHT_ID
                    ORDER BY "CHECKPOINT"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
            "CON")
            AS ADJUSTED_CON,
        -- COR delta = actual COR / predicted COR for most recent hour with actual COR
        IFF("COR≈" <> 0,
            LAST_VALUE("COR" / NULLIF("COR≈", 0))
                IGNORE NULLS
                OVER (PARTITION BY FACILITY_ID, FREIGHT_ID
                    ORDER BY "CHECKPOINT"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 
            NULL) 
            AS "COR Δ",
        -- Adjusted COR = actual COR (past) or predicted COR * COR delta (future)
        IFF("COR" IS NULL AND "COR≈" <> 0,
            "COR≈" * LAST_VALUE("COR" / NULLIF("COR≈", 0))
                IGNORE NULLS
                OVER (PARTITION BY FACILITY_ID, FREIGHT_ID
                    ORDER BY "CHECKPOINT"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
            "COR")
            AS ADJUSTED_COR,
        IFF("COA≈" <> 0,
            LAST_VALUE("COA" / NULLIF("COA≈", 0))
                IGNORE NULLS
                OVER (
                    PARTITION BY FACILITY_ID, FREIGHT_ID
                    ORDER BY "CHECKPOINT"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
            NULL) 
            AS "COA Δ",
        IFF("COA" IS NULL AND "COA≈" <> 0,
            "COA≈" * LAST_VALUE("COA" / NULLIF("COA≈", 0))
                IGNORE NULLS
                OVER (
                    PARTITION BY FACILITY_ID, FREIGHT_ID
                    ORDER BY "CHECKPOINT"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),
            "COA") 
            AS ADJUSTED_COA
    FROM PREDICTED
        LEFT OUTER JOIN ACTUALS USING ( "CHECKPOINT",
                                        "FACILITY_ID",
                                        "FREIGHT_ID" )
)
SELECT "CHECKPOINT",
       "FACILITY_ID",
       "FREIGHT_ID",
       "ALIGNMENT_TIMESTAMP",
       "FINALIZATION_TIMESTAMP",
       "CON≈",
       "COR≈",
       "COA≈",
       "ORDERS≈",
       "CON",
       "COR",
       "COA",
       "ORDERS",
       "CON Δ",
       IFF("CHECKPOINT" <= FINALIZATION_TIMESTAMP,
           ADJUSTED_CON,
           LAST_VALUE(IFF("CHECKPOINT" = FINALIZATION_TIMESTAMP, ADJUSTED_CON, NULL))
                      IGNORE NULLS
                      OVER (
                          PARTITION BY FACILITY_ID, FREIGHT_ID
                          ORDER BY "CHECKPOINT"
                          ROWS BETWEEN 72 PRECEDING AND CURRENT ROW
                      )
           ) AS "CON ±",
       "COR Δ",
       IFF("CHECKPOINT" <= FINALIZATION_TIMESTAMP,
           ADJUSTED_COR,
           LAST_VALUE(IFF("CHECKPOINT" = FINALIZATION_TIMESTAMP, ADJUSTED_COR, NULL))
                      IGNORE NULLS
                      OVER (
                          PARTITION BY FACILITY_ID, FREIGHT_ID
                          ORDER BY "CHECKPOINT"
                          ROWS BETWEEN 72 PRECEDING AND CURRENT ROW
                      )
           ) AS "COR ±",
       "COA Δ",
       IFF("CHECKPOINT" <= FINALIZATION_TIMESTAMP,
           ADJUSTED_COA,
           LAST_VALUE(IFF("CHECKPOINT" = FINALIZATION_TIMESTAMP, ADJUSTED_COA, NULL))
                      IGNORE NULLS
                      OVER (
                          PARTITION BY FACILITY_ID, FREIGHT_ID
                          ORDER BY "CHECKPOINT"
                          ROWS BETWEEN 72 PRECEDING AND CURRENT ROW
                      )
           ) AS "COA ±"

FROM COMPLETE_PREDICTION
""".replace(':survival_table_name', self.survival_table_name)
        )

        self.debug(sql)
        updated_prediction = pd.read_sql(sql, connection)
        updated_prediction["ship_date"] = ship_date
        return updated_prediction

    def store_prediction(self, ship_date, dataframe):
        # self.debug("Serializing to S3")

        # This is not currently needed, and the dependencies for Parquet are large
        # s3_url = S3Url(self.s3_base_url + "target_prediction_{0}.pqt".format(ship_date))
        # with tempfile.NamedTemporaryFile(mode="w", delete=True) as pqt:
        #     dataframe.to_parquet(pqt.name)
        #     pqt.flush()
        #     pqt.seek(0)
        #     self.s3client.upload_file(pqt.name, s3_url.bucket, s3_url.key)

        with tempfile.NamedTemporaryFile(mode="w", encoding='utf-8', delete=True) as csv:
            self.debug(f"Storing data to {csv.name}")
            dataframe.to_csv(csv, index=False)
            csv.flush()
            self.store_prediction_to_db(ship_date, csv.name)

    def create_table(self, connection):
        columns = [k + ' ' + v for k, v in OUTPUT_COLUMNS.items()]
        sql = text("CREATE TABLE IF NOT EXISTS " + self.table_name + " (" + ", ".join(columns) + ")")
        # sql = text(
        #     """
        #     CREATE TABLE IF NOT EXISTS :table_name
        #     (
        #         CHECKPOINT TIMESTAMP_TZ,
        #         FACILITY_ID INTEGER,
        #         FREIGHT_ID INTEGER,
        #         ALIGNMENT_TIMESTAMP TIMESTAMP_TZ,
        #         FINALIZATION_TIMESTAMP TIMESTAMP_TZ,
        #         PREDICTED_CON NUMBER(38,15),
        #         PREDICTED_COR NUMBER(38,15),
        #         PREDICTED_COA NUMBER(38,15),
        #         PREDICTED_ORDERS NUMBER(38,15),
        #         CON NUMBER(38,15),
        #         COR NUMBER(38,15),
        #         COA NUMBER(38,15),
        #         ORDERS NUMBER(38,15),
        #         DELTA_CON NUMBER(38,15),
        #         ADJUSTED_CON NUMBER(38,15),
        #         DELTA_COR NUMBER(38,15),
        #         ADJUSTED_COR NUMBER(38,15),
        #         DELTA_COA NUMBER(38,15),
        #         ADJUSTED_COA NUMBER(38,15),
        #         SHIP_DATE DATE NOT NULL
        #     );
        #     """.replace(':table_name', self.table_name)
        # )
        self.debug(sql)
        connection.execute(sql)

    def store_prediction_to_db(self, ship_date, csv_name):
        self.debug(f"Preparing to store {ship_date} prediction dataset {csv_name} into Snowflake")
        snowflake_util.store(self.engine, self.table_name,
                             OUTPUT_COLUMNS.keys(),
                             csv_name, primary_key='ship_date', primary_key_values=ship_date,
                             skip_header=1)

        self.debug(f"Ship date {ship_date} stored to Snowflake")

    def info(self, msg):
        if self.logger:
            self.logger.info(msg)

    def debug(self, msg):
        if self.logger:
            self.logger.debug(msg)
