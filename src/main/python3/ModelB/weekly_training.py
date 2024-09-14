from lifelines import *
import pandas as pd
from sqlalchemy.sql import text
import boto3
import tempfile
import time
from typing import Dict, List, Tuple

import snowflake_util

LOG_FORMAT = "%(asctime)-15s %(message)s"


class WeeklyTraining:
    def __init__(self, engine, training_weeks=4, s3_base_url=None, aws_key_id=None, aws_secret_key=None,
                 table_name='FRESHLY.ADHOC.MODEL_B_TRAINING_SURVIVAL_FUNCTION', logger=None):
        if engine is not None:
            self.engine = engine
        else:
            raise ValueError("Expecting engine to be passed at initialization")

        self.training_weeks = int(training_weeks)
        self.s3_base_url = s3_base_url
        self.aws_key_id = aws_key_id
        self.aws_secret_key = aws_secret_key
        self.table_name = table_name
        self.logger = logger

        self.info("Initializing Model D weekly training process")

        self.s3client = boto3.client("s3")

    def set_training_weeks(self, number_of_training_weeks):
        self.training_weeks = int(number_of_training_weeks)

    def set_logger(self, logger):
        self.logger = logger

    def execute(self, ship_dates: List[str],
                knowledge_time_utc: str = None,
                lambda_context=None) -> List[str]:
        if not knowledge_time_utc:
            knowledge_time_utc = 'CURRENT_TIMESTAMP'
        self.info(f"Using knowledge time {knowledge_time_utc}")
        max_time = 0
        with self.engine.connect() as conn:
            snowflake_util.configure_connection(conn)
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
                survival = self.compute_survival_function(conn, ship_date, knowledge_time_utc)
                self.store_survival_function(ship_date=ship_date, sf_dataframe=survival)
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

        connection.execute(
            text(
                """
                SET TRAINING_WEEKS = :training_weeks
                """
            ),
            training_weeks=self.training_weeks,
        )

        sql = text(
            """
            SELECT DISTINCT "DELIVERIES"."SHIP_DATE"::TEXT AS SHIP_DATE 
            FROM "DELIVERIES" 
            WHERE $KNOWLEDGE_TIME >= DELIVERIES.CREATED_AT
             AND DATEADD('DAY',-3, DELIVERIES.SHIP_DATE) >= TO_DATE($KNOWLEDGE_TIME)   
              -- The very first complete day EVENT_LOGS were implemented was 2019-03-30
              -- No matter what we don't want to attempt to apply this process on anything
              -- that wouldn't have underlying data to base our predictions on 
              AND DELIVERIES.SHIP_DATE >= DATEADD('week',$TRAINING_WEEKS,'2019-03-30'::DATE)
            ORDER BY SHIP_DATE
            """
        )
        # sql = "SELECT DISTINCT DELIVERIES.SHIP_DATE::TEXT SHIP_DATE FROM DELIVERIES  WHERE SHIP_DATE >= '2015-05-01'  ORDER BY 1 DESC"
        self.debug(sql)
        targets = pd.read_sql_query(
            sql, connection, params={"knowledge_time": "CURRENT_TIMESTAMP"}
        )
        return [row.ship_date for index, row in targets.iterrows()]

    def compute_survival_function(self, connection, ship_date, knowledge_time_utc: str = 'CURRENT_TIMESTAMP'):
        connection.execute(
            text("""SET SHIP_DATE = TO_DATE( :ship_date )"""),
            ship_date=ship_date
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

        connection.execute(
            text("""SET TRAINING_WEEKS = :training_weeks"""),
            training_weeks=self.training_weeks,
        )
        sql = text(
# language=SQL
"""
WITH COMPLETE_FILTER AS (
    -- Deliveries to be used for training data
    SELECT DISTINCT DELIVERIES.SHIP_DATE
    FROM DELIVERIES
         -- SAME DAY OF WEEK
    WHERE DATE_PART('DOW', DELIVERIES.SHIP_DATE) = DATE_PART('DOW', $SHIP_DATE)
      -- finalized prior to or exactly today 
      AND DATEADD('DAY', -3, DELIVERIES.SHIP_DATE) <= TO_DATE($KNOWLEDGE_TIME)
      -- prior to the day you're running estimates for  
      AND $SHIP_DATE >= DELIVERIES.SHIP_DATE
      -- records created prior to knowledge time (different from today)
      AND $KNOWLEDGE_TIME >= DELIVERIES.CREATED_AT
      -- ship date completed prior to knowledge time (different from today) 
      AND $KNOWLEDGE_TIME >= DELIVERIES.SHIP_DATE
    ORDER BY DELIVERIES.SHIP_DATE DESC
    LIMIT $TRAINING_WEEKS
),
FIRST_ORDERS AS (
    -- First order for each subscription
    -- WE ARE USING USER BASED TENURE, BUT THERE IS A STRONG REASON TO SWITCH TO SUBSCRIPTION BASED TENURE
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
MASTER_DATASET AS (
    -- Calculates tenure in weeks (1+), finalization timestamp, birth hour, etc. for each delivery
    -- DATEADD(HOUR, n, DATE) returns a TIMESTAMP_NTZ by default, so it is converted to a TIMESTAMP_TZ 
    -- using the facility time zone 
    SELECT WEEKLY_ORDERS.USER_ID,
           WEEKLY_ORDERS.SUBSCRIPTION_ID,
           DELIVERIES.ID                                                           AS DELIVERY_ID,
           WEEKLY_ORDERS.CREATED_AT                                                AS BIRTH_TIME,
           WEEKLY_ORDERS.ID                                                        AS WEEKLY_ORDER_ID,
           1 + DATEDIFF('WEEK', FIRST_ORDERS.START_DATE, WEEKLY_ORDERS.START_DATE) AS USER_TENURE,
           DELIVERIES.FACILITY_ID,
           FACILITIES.DISPLAY_NAME                                                 AS FACILITY,
           DELIVERIES.FREIGHT_ID,
           FREIGHTS.NAME                                                           AS FREIGHT,
           DELIVERIES.PLAN_ID,
           PLANS.NAME                                                              AS PLAN,
           TIMEZONES.FACILITY_TIMEZONE,
           udf_ntz_to_tz(DATEADD('hour', +17, DELIVERIES.SHIP_DATE),
                         TIMEZONES.FACILITY_TIMEZONE)                                 ALIGNMENT_TIMESTAMP,
           udf_ntz_to_tz(DATEADD('hour', +17, DATEADD('day', -3, DELIVERIES.SHIP_DATE)),
                         TIMEZONES.FACILITY_TIMEZONE)                                 FINALIZATION_TIMESTAMP,
           DELIVERIES.SHIP_DATE
    FROM FIRST_ORDERS
        JOIN FIVETRAN.OPDB_PUBLIC.WEEKLY_ORDERS ON FIRST_ORDERS.SUBSCRIPTION_ID = WEEKLY_ORDERS.SUBSCRIPTION_ID
        JOIN FIVETRAN.OPDB_PUBLIC.DELIVERIES ON DELIVERIES.WEEKLY_ORDER_ID = WEEKLY_ORDERS.ID
        JOIN COMPLETE_FILTER ON COMPLETE_FILTER.SHIP_DATE = DELIVERIES.SHIP_DATE
        JOIN PLANS ON PLANS.ID = DELIVERIES.PLAN_ID
        JOIN FREIGHTS ON FREIGHTS.ID = DELIVERIES.FREIGHT_ID
        JOIN FACILITIES ON FACILITIES.ID = DELIVERIES.FACILITY_ID
        JOIN TIMEZONES ON TIMEZONES.FACILITY_ID = FACILITIES.ID
    WHERE ($KNOWLEDGE_TIME >= WEEKLY_ORDERS.CREATED_AT OR WEEKLY_ORDERS.CREATED_AT IS NULL)
      AND ($KNOWLEDGE_TIME >= DELIVERIES.CREATED_AT OR DELIVERIES.CREATED_AT IS NULL)
      AND ($KNOWLEDGE_TIME >= PLANS.CREATED_AT OR PLANS.CREATED_AT IS NULL)
      AND ($KNOWLEDGE_TIME >= FREIGHTS.CREATED_AT OR FREIGHTS.CREATED_AT IS NULL)
      AND ($KNOWLEDGE_TIME >= FACILITIES.CREATED_AT OR FACILITIES.CREATED_AT IS NULL)
),
"EVENTS" AS (
    -- Events that affect an order's "alive" status: creation, skips, and unskips
    -- Includes only events known as of KNOWLEDGE_TIME
    SELECT WEEKLY_ORDERS.ID         AS WEEKLY_ORDER_ID,
           WEEKLY_ORDERS.CREATED_AT AS EVENT_TIME,
           TRUE                     AS ORDER_IS_ALIVE
    FROM WEEKLY_ORDERS
        JOIN DELIVERIES ON (DELIVERIES.WEEKLY_ORDER_ID = WEEKLY_ORDERS.ID)
        JOIN COMPLETE_FILTER ON (COMPLETE_FILTER.SHIP_DATE = DELIVERIES.SHIP_DATE)
    WHERE $KNOWLEDGE_TIME >= WEEKLY_ORDERS.CREATED_AT
      AND $KNOWLEDGE_TIME >= DELIVERIES.CREATED_AT
    UNION
    SELECT WEEKLY_ORDERS.ID      AS WEEKLY_ORDER_ID,
           EVENT_LOGS.CREATED_AT AS EVENT_TIME,
           IFF(EVENT_LOGS.CHANGE_SET:status[1] IN ('not_paid', 'skipped_closed', 'skipped_open', 'admin_canceled_closed', 'admin_canceled_open'),
               FALSE, TRUE)      AS ORDER_IS_ALIVE
    FROM WEEKLY_ORDERS
        JOIN DELIVERIES ON (DELIVERIES.WEEKLY_ORDER_ID = WEEKLY_ORDERS.ID)
        JOIN COMPLETE_FILTER ON (COMPLETE_FILTER.SHIP_DATE = DELIVERIES.SHIP_DATE)
        JOIN EVENT_LOGS ON (EVENT_LOGS.OBJECT_TYPE = 'deliveries' AND 
                            EVENT_LOGS.OBJECT_ID = DELIVERIES.ID AND
                            EVENT_LOGS.CHANGE_SET:status IS NOT NULL)
    WHERE $KNOWLEDGE_TIME >= WEEKLY_ORDERS.CREATED_AT
      AND $KNOWLEDGE_TIME >= DELIVERIES.CREATED_AT
      AND $KNOWLEDGE_TIME >= EVENT_LOGS.CREATED_AT
),
ENUMERATED_EVENTS AS (
    -- Add sequence of events for each order, last known alive time
    SELECT WEEKLY_ORDERS.ID                                       AS WEEKLY_ORDER_ID,
           WEEKLY_ORDERS.STATUS,
           WEEKLY_ORDERS.CREATED_AT                               AS BIRTH_TIME,
           EVENTS.EVENT_TIME,
           EVENTS.ORDER_IS_ALIVE,
           row_number() OVER (
               PARTITION BY EVENTS.weekly_order_id
               ORDER BY EVENTS.EVENT_TIME )                       AS EVENT_NUMBER,
           row_number() OVER (
               PARTITION BY EVENTS.weekly_order_id
               ORDER BY EVENTS.EVENT_TIME DESC)                   AS EVENT_RECENCY,
           max(CASE WHEN EVENTS.ORDER_IS_ALIVE THEN EVENTS.EVENT_TIME ELSE NULL END) OVER (
               PARTITION BY WEEKLY_ORDERS.ID
               ORDER BY EVENTS.EVENT_TIME
               RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LAST_KNOWN_ALIVE
    FROM WEEKLY_ORDERS
        JOIN DELIVERIES ON (DELIVERIES.WEEKLY_ORDER_ID = WEEKLY_ORDERS.ID)
        JOIN TIMEZONES ON DELIVERIES.FACILITY_ID = TIMEZONES.FACILITY_ID
        LEFT OUTER JOIN EVENTS ON (WEEKLY_ORDERS.ID = EVENTS.WEEKLY_ORDER_ID)
         -- CHOP OFF EVENTS AT FINALIZATION. DO NOT ATTEMPT TO LOOK BEYOND IT THROUGH SHIPPING.
    WHERE udf_ntz_to_tz(DATEADD('hour', +17, DATEADD('day', -3, DELIVERIES.SHIP_DATE)),
                        TIMEZONES.FACILITY_TIMEZONE) > EVENTS.EVENT_TIME
      AND $KNOWLEDGE_TIME >= WEEKLY_ORDERS.CREATED_AT
      AND $KNOWLEDGE_TIME >= DELIVERIES.CREATED_AT
),
PREPARED_FOR_PLUCKING AS (
    -- Consider the series of events for an order as a series of streaks defined by alive status
    -- Add the timestamp of the beginning of each streak
    SELECT ENUMERATED_EVENTS.*,
           min(EVENT_TIME) OVER (
               PARTITION BY WEEKLY_ORDER_ID, LAST_KNOWN_ALIVE, ORDER_IS_ALIVE
               ORDER BY EVENT_NUMBER ) AS FIRST_TIME_IN_STREAK
    FROM ENUMERATED_EVENTS
),
WEEKLY_ORDER_CHANGE_HISTORY AS (
    -- The death time of the order (if any) is the time at which the most recent event occurred
    -- that changed the status from alive to dead, i.e., the beginning of the last streak
    -- (assuming the order is not currently alive)
    SELECT WEEKLY_ORDER_ID,
           MIN(CASE WHEN NOT ORDER_IS_ALIVE AND EVENT_RECENCY = 1 THEN FIRST_TIME_IN_STREAK END) AS DEATH_TIME
    FROM PREPARED_FOR_PLUCKING
    WHERE EVENT_RECENCY = 1
    GROUP BY 1
)
SELECT DATEDIFF('HOUR', MASTER_DATASET.BIRTH_TIME,
                IFF(DEATH_TIME IS NOT NULL, DEATH_TIME, FINALIZATION_TIMESTAMP)) AS LIFESPAN,
       WEEKLY_ORDER_CHANGE_HISTORY.DEATH_TIME IS NOT NULL                        AS DIED_BOOL,
       MASTER_DATASET.FACILITY_ID || '-' ||
       MASTER_DATASET.FREIGHT_ID || '-' ||
       MASTER_DATASET.PLAN_ID || '-' ||
       IFF(MASTER_DATASET.USER_TENURE <= 3, MASTER_DATASET.USER_TENURE, 4)       AS GRP,
       COUNT(DISTINCT MASTER_DATASET.WEEKLY_ORDER_ID)                            AS WEIGHT
FROM MASTER_DATASET
         JOIN WEEKLY_ORDER_CHANGE_HISTORY USING(WEEKLY_ORDER_ID)
GROUP BY 1, 2, 3
-- Also compute data across all freights for a facility (freight ID 0)
-- This will be used in the hourly adjustment process for newly-added freights
UNION ALL
SELECT DATEDIFF('HOUR', MASTER_DATASET.BIRTH_TIME,
                IFF(DEATH_TIME IS NOT NULL, DEATH_TIME, FINALIZATION_TIMESTAMP)) AS LIFESPAN,
       WEEKLY_ORDER_CHANGE_HISTORY.DEATH_TIME IS NOT NULL                        AS DIED_BOOL,
       MASTER_DATASET.FACILITY_ID || '-' ||
       '0' || '-' ||
       MASTER_DATASET.PLAN_ID || '-' ||
       IFF(MASTER_DATASET.USER_TENURE <= 3, MASTER_DATASET.USER_TENURE, 4)       AS GRP,
       COUNT(DISTINCT MASTER_DATASET.WEEKLY_ORDER_ID)                            AS WEIGHT
FROM MASTER_DATASET
         JOIN WEEKLY_ORDER_CHANGE_HISTORY USING(WEEKLY_ORDER_ID)
GROUP BY 1, 2, 3
-- Also compute data across all plans (plan ID 0)
-- This will be used by the demand planner to compute USR
UNION ALL
SELECT DATEDIFF('HOUR', MASTER_DATASET.BIRTH_TIME,
                IFF(DEATH_TIME IS NOT NULL, DEATH_TIME, FINALIZATION_TIMESTAMP)) AS LIFESPAN,
       WEEKLY_ORDER_CHANGE_HISTORY.DEATH_TIME IS NOT NULL                        AS DIED_BOOL,
       MASTER_DATASET.FACILITY_ID || '-' ||
       MASTER_DATASET.FREIGHT_ID || '-' ||
       '0' || '-' ||
       IFF(MASTER_DATASET.USER_TENURE <= 3, MASTER_DATASET.USER_TENURE, 4)       AS GRP,
       COUNT(DISTINCT MASTER_DATASET.WEEKLY_ORDER_ID)                            AS WEIGHT
FROM MASTER_DATASET
         JOIN WEEKLY_ORDER_CHANGE_HISTORY USING(WEEKLY_ORDER_ID)
GROUP BY 1, 2, 3
-- Also compute data across all plans & all freights
-- This will be used by the demand planner to compute USR
UNION ALL
SELECT DATEDIFF('HOUR', MASTER_DATASET.BIRTH_TIME,
                IFF(DEATH_TIME IS NOT NULL, DEATH_TIME, FINALIZATION_TIMESTAMP)) AS LIFESPAN,
       WEEKLY_ORDER_CHANGE_HISTORY.DEATH_TIME IS NOT NULL                        AS DIED_BOOL,
       MASTER_DATASET.FACILITY_ID || '-' ||
       '0' || '-' ||
       '0' || '-' ||
       IFF(MASTER_DATASET.USER_TENURE <= 3, MASTER_DATASET.USER_TENURE, 4)       AS GRP,
       COUNT(DISTINCT MASTER_DATASET.WEEKLY_ORDER_ID)                            AS WEIGHT
FROM MASTER_DATASET
         JOIN WEEKLY_ORDER_CHANGE_HISTORY USING(WEEKLY_ORDER_ID)
GROUP BY 1, 2, 3
"""
        )
        self.debug(sql)
        target = pd.read_sql(sql, connection)
        self.debug(f"Retrieved {len(target.index)} training records")
        if target.empty:
            raise ValueError(f"Nothing useful from DB for {ship_date}")

        out = pd.DataFrame()
        kmf = KaplanMeierFitter()

        self.debug("Feeding Kaplan-Meier fitter")
        count = 0
        small_count = 0
        for name, grouped_df in target.groupby("grp"):
            count += 1
            if len(grouped_df) < 100:
                small_count += 1
            else:
                kmf.fit(
                    grouped_df["lifespan"],
                    grouped_df["died_bool"],
                    label=name,
                    weights=grouped_df["weight"],
                    timeline=range(0, 650, 1),
                )
                if out.empty:
                    out = kmf.survival_function_
                else:
                    out = out.join(kmf.survival_function_)
        self.debug(f"Excluded {small_count} of {count} groups as < 100 records")
        return out

    def store_survival_function(self, ship_date, sf_dataframe):
        if sf_dataframe.empty:
            raise ValueError("Won't serialize empty dataframe")

        # Save as a binary file - data frame remains unadulterated
        # This is not currently needed, and the dependencies for Parquet are large
        # survival_s3_url = S3Url(self.s3_base_url + f"training_survival_function_{ship_date}.pqt")
        # self.debug(f"Upload to {survival_s3_url.url}")
        # with tempfile.NamedTemporaryFile(mode="w", delete=True) as pqt:
        #     sf_dataframe.to_parquet(pqt.name)
        #     pqt.flush()
        #     pqt.seek(0)
        #     self.s3client.upload_file(
        #         pqt.name, survival_s3_url.bucket, survival_s3_url.key
        #     )

        # prepare dataset for sql engine ingestion: convert the shape of data to be usable in SQL queries
        self.debug("Melting survival rates")
        sf_dataframe["timeline"] = sf_dataframe.index.astype(int)
        sf_dataframe.rename_axis("grp", axis="columns", inplace=True)
        survival = sf_dataframe.melt(
            id_vars=["timeline"], var_name="grp", value_name="survival_rate"
        ).set_index("grp")

        survival["ship_date"] = ship_date
        with tempfile.NamedTemporaryFile(mode="w", encoding='utf-8', delete=True) as csv:
            self.debug(f"Storing data to {csv.name}")
            survival.to_csv(csv, index=True)
            csv.flush()
            self.store_survival_function_to_db(ship_date, csv.name)

    def create_table(self, connection):
        sql = text(
            """
            CREATE TABLE IF NOT EXISTS :table_name (
                grp VARCHAR(20) NOT NULL, 
                timeline INTEGER NOT NULL,  
                survival_rate DECIMAL(17, 15) NOT NULL, 
                ship_date DATE NOT NULL
            )
            """.replace(':table_name', self.table_name)
        )
        self.debug(sql)
        connection.execute(sql)

    def store_survival_function_to_db(self, ship_date, survival_csv_name):
        self.debug(f"Storing data for {ship_date} into Snowflake")
        snowflake_util.store(self.engine, self.table_name,
                             ['grp', 'timeline', 'survival_rate', 'ship_date'],
                             survival_csv_name, primary_key='ship_date', primary_key_values=ship_date,
                             skip_header=1)

        self.debug(f"Ship date {ship_date} stored to Snowflake")

    def info(self, msg):
        if self.logger:
            self.logger.info(msg)

    def debug(self, msg):
        if self.logger:
            self.logger.debug(msg)

