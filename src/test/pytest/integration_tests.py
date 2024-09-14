# Integration tests
# These will store data in the FRESHLY_DEV schema

import time

import snowflake_util
from lambda_model_d import lambda_handler, ENGINE


def do_test(event, context=None):
    lambda_handler(event, context)
    ENGINE.dispose()


class TestContext:
    def __init__(self, millis_remaining):
        self.millis_remaining = millis_remaining
        self.start = time.time()

    def get_remaining_time_in_millis(self):
        now = time.time()
        elapsed = now - self.start
        self.start = now
        return self.millis_remaining - (elapsed * 1000)


def test_last_sync():
    with ENGINE.connect() as conn:
        snowflake_util.configure_connection(conn)
        d = snowflake_util.get_latest_sync_time(conn)
        print(f"Last sync time is {d}")


# def test_weekly():
#     do_test({'mode': 'weekly'})


# def test_hourly():
#     do_test({'mode': 'hourly'})


# def test_hourly_date():
#     do_test({'mode': 'hourly', 'ship_dates': ['2019-10-23']})


# def test_context():
#     do_test({'mode': 'hourly', 'ship_dates': ['2019-09-22', '2019-09-20', '2019-09-19', '2019-09-18', '2019-09-17']},
#             TestContext(60000))

# def test_knowledge_time():
#     test({
#         'mode': 'hourly',
#         'ship_dates': ['2019-10-04'],
#         'knowledge_time': '2019-09-17T16:00:00+00:00',
#     })



