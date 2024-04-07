import time

import requests
from bs4 import BeautifulSoup

from helper.helper_db import init_db, close_db

from datetime import datetime
from prefect import flow, task, serve

from etl_score_alert import extract, init, transform, process


# from etl_score_alert import transform, process


@flow
def flow_run_score_alert_night():
    match_start_time = 10
    match_link = init(match_start_time)
    if match_link == '':
        exit(1)
    while True:
        dict_raw_data = extract(match_link)
        dict_processed_data = transform(dict_raw_data)
        delay = process(dict_processed_data)

        current_time = datetime.now()
        current_hour = current_time.hour
        if current_hour == match_start_time + 4:
            break

        print("Sleeping for", delay, "seconds")
        time.sleep(delay)

    # run(10)
    # transform()
    # process()


if __name__ == "__main__":
    flow_run_score_alert_night.serve(name="deployment_etl_score_alert_night", cron="15 14 * * *")
    # flow_run_score_alert_night()
