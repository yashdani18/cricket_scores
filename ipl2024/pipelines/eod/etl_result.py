import time

import mysql.connector
from datetime import date

import requests
from bs4 import BeautifulSoup

from config.mysql import HOST, USER, PASSWORD, DATABASE
from helper.helper_telegram import send_pipeline_start_message, send_pipeline_end_message

from ipl2024.constants import \
    TEAM1, TEAM2, \
    TEAM1_RUNS, TEAM1_WICKETS, TEAM1_OVERS, \
    TEAM2_RUNS, TEAM2_WICKETS, TEAM2_OVERS, \
    WINNER, POTM, \
    DICTIONARY_TEAMS, MATCH_NUMBER, WON_BY_NUMBER, WON_BY_METRIC, LINK

from prefect import flow, task

"""
USAGE: 
Prerequisites: This script uses a SQL table (filtered by current date) that has the schedule for all matches
Trigger this script after all matches for a given day end
Actions:
1. Create a new entry in the 'result' table
2. Update column='winner' at corresponding entry in schedule_result table
"""


def init_db():
    db = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )

    cursor = db.cursor()
    return db, cursor


def close_db(db, cursor):
    cursor.close()
    db.close()


def fetch_result(url: str) -> dict:
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml')
    try:
        score_team1 = soup.find(False, {'class': ['cb-col cb-col-100 cb-min-tm cb-text-gray']}).text
        score_team2 = soup.find(False, {'class': ['cb-col cb-col-100 cb-min-tm']}).text
        potm = soup.find(False, {'class': ['cb-col cb-col-50 cb-mom-itm']}).find('a').text
        result = soup.find(False, {'class': ['cb-col cb-col-100 cb-min-stts cb-text-complete']}).text
        return {
            'score_team1': score_team1,
            'score_team2': score_team2,
            'potm': potm,
            'result': result
        }
    except AttributeError as e:
        print("Exception:", str(e))
        exit(1)


def clean_scores(score: str) -> dict:
    try:
        team_score_overs = score.split()
        team = team_score_overs[0]
        runs_wickets = team_score_overs[1]
        runs = int(runs_wickets.split('/')[0])
        wickets = 10
        if len(runs_wickets.split('/')) > 1:
            wickets = int(runs_wickets.split('/')[1])
        overs = float(team_score_overs[2][1:-1])
        return {
            'team': team,
            'runs': runs,
            'wickets': wickets,
            'overs': overs
        }
    except Exception as e:
        print(str(e))
        exit(1)


@task(name='etl_result_extract')
def extract():
    """
    get current date, and fetch records from db for that date
    visit the corresponding link and get the result
    :return: array of dictionaries
    """
    db, cursor = init_db()
    cursor.execute(
        f'SELECT {LINK}, {MATCH_NUMBER} FROM schedule_result WHERE match_date <= "{date.today()}" and winner is NULL')
    # cursor.execute(f'SELECT * FROM schedule_result WHERE match_date = "{date.today()}"')
    # cursor.execute(f'SELECT * FROM schedule_result WHERE match_date <= "{date.today()}" and winner is null')
    # cursor.execute(f'SELECT * FROM schedule_result WHERE match_date="2024-03-23"')

    rows = cursor.fetchall()

    arr_result = []
    for index, row in enumerate(rows):
        MATCH_URL = row[0]
        result = fetch_result(MATCH_URL)
        result[MATCH_NUMBER] = row[1]
        arr_result.append(result)
        print(f'Completed fetching {index + 1}/{len(rows)}...')
        # print(result)
        time.sleep(5)

    print('Extraction Complete!')
    time.sleep(2)
    return arr_result


@task(name='etl_result_transform')
def transform(arr_result: []) -> []:
    arr_rows = []
    for index, result in enumerate(arr_result):
        dictionary = {}

        score_team1 = clean_scores(result['score_team1'])
        dictionary[TEAM1] = score_team1['team']
        dictionary[TEAM1_RUNS] = score_team1['runs']
        dictionary[TEAM1_WICKETS] = score_team1['wickets']
        dictionary[TEAM1_OVERS] = score_team1['overs']

        score_team2 = clean_scores(result['score_team2'])
        dictionary[TEAM2] = score_team2['team']
        dictionary[TEAM2_RUNS] = score_team2['runs']
        dictionary[TEAM2_WICKETS] = score_team2['wickets']
        dictionary[TEAM2_OVERS] = score_team2['overs']

        dictionary[WINNER] = DICTIONARY_TEAMS.get(" ".join(result['result'].split()[0:-4]))

        dictionary[WON_BY_NUMBER] = int(result['result'].split()[-2])
        dictionary[WON_BY_METRIC] = result['result'].split()[-1]

        dictionary[POTM] = result['potm']

        dictionary[MATCH_NUMBER] = result[MATCH_NUMBER]

        arr_rows.append(dictionary)

    print('Transformation Complete!')
    time.sleep(2)
    return arr_rows


@task(name='etl_result_load')
def load(arr_result: []) -> bool:
    # print(arr_result)
    db, cursor = init_db()
    for match in arr_result:
        sql = f"INSERT INTO tbl_result({MATCH_NUMBER}, {TEAM1}, {TEAM1_RUNS}, {TEAM1_WICKETS}, {TEAM1_OVERS}, " \
              f"{TEAM2}, {TEAM2_RUNS}, {TEAM2_WICKETS}, {TEAM2_OVERS}, {WINNER}, {WON_BY_NUMBER}, {WON_BY_METRIC}, " \
              f"{POTM})" \
              f"VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (match[MATCH_NUMBER],
               match[TEAM1], match[TEAM1_RUNS], match[TEAM1_WICKETS], match[TEAM1_OVERS],
               match[TEAM2], match[TEAM2_RUNS], match[TEAM2_WICKETS], match[TEAM2_OVERS],
               match[WINNER], match[WON_BY_NUMBER], match[WON_BY_METRIC], match[POTM])
        cursor.execute(sql, val)

        sql = f"""
        UPDATE tbl_schedule
        SET {WINNER} = %s
        WHERE {MATCH_NUMBER} = %s
        """
        val = (match[WINNER], match[MATCH_NUMBER])
        cursor.execute(sql, val)
    db.commit()
    print('Loading Complete!')
    time.sleep(2)
    close_db(db, cursor)
    return False


def flow_run_etl_eod_result():
    pipeline = 'EOD'
    send_pipeline_start_message(pipeline)
    load(transform(extract()))
    send_pipeline_end_message(pipeline)


# flow_run_etl_eod()
if __name__ == "__main__":
    flow_run_etl_eod_result()
    # flow_run_etl_eod_result.serve(name="deployment_etl_eod_result", cron="0 19 * * *")