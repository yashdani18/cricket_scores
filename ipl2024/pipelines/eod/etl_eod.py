from prefect import flow

from etl_result import extract as etl_result_extract, transform as etl_result_transform, load as etl_result_load
from etl_scorecard import extract as etl_scorecard_extract, load as etl_scorecard_load
from helper.helper_telegram import send_pipeline_start_message, send_pipeline_end_message


@flow
def flow_run_etl_eod_result():
    pipeline = 'EOD'
    send_pipeline_start_message(pipeline)
    etl_result_load(etl_result_transform(etl_result_extract()))
    send_pipeline_end_message(pipeline)


@flow
def flow_etl_scorecard():
    pipeline = 'Scorecard'
    send_pipeline_start_message(pipeline)
    etl_scorecard_load(etl_scorecard_extract())
    send_pipeline_end_message(pipeline)


@flow
def flow_eod():
    flow_run_etl_eod_result()
    flow_etl_scorecard()


if __name__ == "__main__":
    flow_eod.serve(name="deployment_etl_eod", cron="0 19 * * *")
    # flow_run_etl_eod_result()
    # flow_etl_scorecard()
