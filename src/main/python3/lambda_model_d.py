import json
import logging
import os
import sys
import traceback
from pathlib import Path

import boto3
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text

import snowflake_util
from ModelB.weekly_training import WeeklyTraining
from ModelB.hourly_correction import SelfCorrection

#################################################
# Set these to appropriate values
APP_NAME = "Demand model D"  # human-readable project name
ERROR_SUBJECT = "Error in " + APP_NAME
#################################################

ENVIRONMENT = os.environ.get('env', 'default')
AWS_REGION = os.environ.get('aws_region')
SNOWFLAKE_SECRET_ID = os.environ.get('snowflake_secret_id', 'production/snowflake/model_user')
SNOWFLAKE_DATABASE = os.environ.get('snowflake_db', 'freshly_dev')
S3_PATH = os.environ.get('s3_path', 'freshly-data-dev/model-d-local/')
SNS_TOPIC_ARN = os.environ.get('sns_topic_arn')
LAMBDA_NAME = os.environ.get('lambda_name', 'model_d-local')
LAMBDA_VERSION = os.environ.get('lambda_version')
LOG_LEVEL = os.environ.get('log_level', 'INFO')

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('model_d')
logger.setLevel(LOG_LEVEL)
snowflake_util.logger = logger

logger.info(f"Initializing {LAMBDA_NAME} version {LAMBDA_VERSION} for environment {ENVIRONMENT}")

secretsmgr = boto3.client('secretsmanager', region_name=AWS_REGION)
sns = boto3.resource('sns')
aws_lambda = boto3.client('lambda')

# Cache Snowflake creds
SNOWFLAKE_CREDS = json.loads(secretsmgr.get_secret_value(SecretId=SNOWFLAKE_SECRET_ID)['SecretString'])
SNOWFLAKE_SCHEMA = 'demand_planner'
SNOWFLAKE_WAREHOUSE = 'stitch_warehouse'
ENGINE = create_engine(URL(
    account=SNOWFLAKE_CREDS['account'],
    user=SNOWFLAKE_CREDS['username'],
    # role=config['snowflake']['role'],
    password=SNOWFLAKE_CREDS['password'],
    warehouse=SNOWFLAKE_CREDS['warehouse'],
    database='fivetran',
    schema='opdb_public',
    timezone='UTC'
))

DEFAULT_CONFIG = {
    "training_weeks": 13
}

TABLE_SURVIVAL = '.'.join([SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, 'MODEL_D_SURVIVAL_FUNCTION'])
TABLE_PREDICTION = '.'.join([SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, 'MODEL_D_PREDICTION'])

COL_SURVIVAL_SHIP_DATE = 'ship_date'
SURVIVAL_COLUMNS = ['grp', 'timeline', 'survival_rate', COL_SURVIVAL_SHIP_DATE]


def lambda_handler(event, context):
    logger.info(f"Handle event {event}")
    mode = event.get('mode')
    if not mode:
        notify_error(f"No mode specified: exiting")
        return

    config = dict(DEFAULT_CONFIG)
    config.update(event)
    config['survival_table_name'] = TABLE_SURVIVAL
    config['prediction_table_name'] = TABLE_PREDICTION
    config['s3path'] = S3_PATH

    try:
        if mode == 'weekly':
            remaining_ship_dates = run_weekly(config, context)
        elif mode == 'hourly':
            remaining_ship_dates = run_hourly(config, context)
        else:
            notify_error(f"Invalid mode {mode}: exiting")
            return
        if remaining_ship_dates:
            schedule_remaining_ship_dates(config, remaining_ship_dates)
        else:
            logger.info(f"Finished processing for mode {mode}")
    except Exception:
        msg = "An error occurred"
        notify_error(msg, sys.exc_info())


def run_weekly(config, context):
    weekly_process = WeeklyTraining(
        ENGINE,
        training_weeks=config['training_weeks'],
        s3_base_url='s3://' + config['s3path'],
        table_name=config['survival_table_name'],
        logger=logger
    )
    return weekly_process.execute(config.get('ship_dates'), config.get('knowledge_time'), context)


def run_hourly(config, context):
    hourly_process = SelfCorrection(
        ENGINE,
        s3_base_url='s3://' + config['s3path'],
        table_name=config['prediction_table_name'],
        survival_table_name=config['survival_table_name'],
        logger=logger
    )
    return hourly_process.execute(config.get('ship_dates'), config.get('knowledge_time'), context)


def schedule_remaining_ship_dates(config, remaining_ship_dates):
    logger.info(f"Invoking lambda for {remaining_ship_dates}")
    config['ship_dates'] = remaining_ship_dates
    payload = json.dumps(config).encode('utf-8')
    aws_lambda.invoke(FunctionName=LAMBDA_NAME,
                      InvocationType='Event',
                      LogType='None',
                      Payload=payload)
    logger.info(f"Invoked lambda for {remaining_ship_dates}")


def query(conn, sql, **params):
    msg = str(sql)
    for name, value in params.items():
        msg = msg.replace(':' + name, snowflake_util.format_sql_value(value))
    logger.debug(msg)
    return conn.execute(sql, **params)


def load_sql(filename) -> str:
    """Loads a SQL statement from a local file and returns it as a string.

    First looks in the current directory (where it will be in the lambda deployment package).
    If not found there, looks in ../sql (for testing locally).
    """
    p = Path(filename)
    if not p.exists():
        p = Path('../sql') / filename
    return p.read_text('utf8')


def notify_error(msg, exc_info, subject=ERROR_SUBJECT, alert=False):
    logger.error(msg, exc_info=exc_info)
    msg = "%s: %s\n%s" % (APP_NAME, msg, ''.join(traceback.format_exception(*exc_info)))

    if alert and SNS_TOPIC_ARN:
        # Publish a message to SNS
        # Only do this for serious errors so as not to spam the email list & Slack channel
        try:
            topic = sns.Topic(SNS_TOPIC_ARN)
            topic.publish(Subject=subject, Message=msg)
        except Exception:
            logger.exception("Error publishing message '%s' to SNS topic %s", msg, SNS_TOPIC_ARN)
