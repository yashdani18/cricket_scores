import time

import requests
from bs4 import BeautifulSoup

from helper.helper_db import init_db, close_db

from datetime import datetime, date
from prefect import flow, task, serve
import telebot
from config.telegram import API_KEY, CHAT_ID

from ipl2024.constants import TEAM, RUNS, WICKETS, OVERS, CRR, POWERPLAY_OVER_LIMIT, AGGRESSIVE_BATTING_CRR, RRR, \
    CHASE_LB, CHASE_UB, CHASE_WICKET_LIMIT, CHASE_MIN_OVER_VAL

from helper.helper_telegram import send_aggressive_batting_alert, send_explosive_powerplay_alert, \
    send_exciting_chase_alert


def init(match_start_time: int):
    db, cursor = init_db()
    cursor.execute(
        f'SELECT link from schedule_result where match_date="{date.today()}" '
        f'and match_time="{str(match_start_time).zfill(2)}:00:00"')
    rows = cursor.fetchall()
    print(rows)
    if len(rows) > 0:
        return rows[0][0]
    else:
        return ''


@task
def extract(link):
    print('Extract')
    html = requests.get(link).text
    soup = BeautifulSoup(html, 'lxml')
    dictionary = {}
    try:
        score = soup.find(class_='cb-min-bat-rw')
        ind_score = score.find_all(True, {'class': ['cb-font-20', 'text-bold', 'inline-block']})
        current_runs_wicket = ind_score[0].text.strip()
        dictionary['current_runs_wickets'] = current_runs_wicket
        # print(current_runs_wicket)

        ind_crr = score.find_all(True, {'class': ['cb-font-12', 'cb-text-gray']})
        current_run_rate = ind_crr[0].text.strip()
        dictionary['current_run_rate'] = current_run_rate
        # print(current_run_rate)

        if len(ind_crr) > 1:
            req_run_rate = ind_crr[1].text.strip()
            dictionary['req_run_rate'] = req_run_rate

        return dictionary
    except AttributeError as e:
        print('Exception:', str(e))
        print('Exiting program with status code 1.')
        # filename = f'error/log_{datetime.now().strftime("%m_%d_%Y_%H_%M_%S")}.txt'
        # with open(filename, "w", encoding="utf-8") as f:
        #     f.write(html)
        # f.close()
        exit(1)


@task
def transform(dict_stats: dict):
    print('Transform')
    team_score_overs = dict_stats['current_runs_wickets'].split()
    team = team_score_overs[0]
    runs_wickets = team_score_overs[1]
    runs = runs_wickets.split('/')[0]
    wickets = runs_wickets.split('/')[1]
    overs = team_score_overs[2][1:-1]
    crr_key_val = dict_stats['current_run_rate'].split()
    crr_key = crr_key_val[0]
    crr_val = crr_key_val[1]
    req_run_rate = dict_stats.get('req_run_rate')
    rrr_val = ''
    if req_run_rate is not None:
        rrr_key_val = req_run_rate.split()
        rrr_key = rrr_key_val[0]
        rrr_val = rrr_key_val[1]
        # print(req_run_rate)
    return {
        'team': team,
        'runs': int(runs),
        'wickets': int(wickets),
        'overs': float(overs),
        'crr': float(crr_val),
        'rrr': float(rrr_val) if rrr_val != '' else rrr_val
    }


def send_alert(dict_processed_data: dict):
    bot = telebot.TeleBot(API_KEY)
    bot.send_message(CHAT_ID,
                     f"{dict_processed_data['team']} is batting aggressively. "
                     f"{dict_processed_data['runs']}/{dict_processed_data['wickets']} in {dict_processed_data['overs']}")


@task
def process(dict_processed_data: dict):
    print('Process')
    print(dict_processed_data)

    team = dict_processed_data[TEAM]
    runs = dict_processed_data[RUNS]
    wickets = dict_processed_data[WICKETS]
    overs = dict_processed_data[OVERS]
    crr = dict_processed_data[CRR]
    rrr = dict_processed_data.get(RRR, 0)

    if overs == 20:
        return 1200

    condition_crr_overs = crr > AGGRESSIVE_BATTING_CRR and overs > 10
    condition_explosive_powerplay = crr > AGGRESSIVE_BATTING_CRR and overs < POWERPLAY_OVER_LIMIT
    condition_exciting_chase = (CHASE_LB <= rrr <= CHASE_UB) and (wickets <= CHASE_WICKET_LIMIT) and \
                               (overs >= CHASE_MIN_OVER_VAL)

    if condition_crr_overs:
        send_aggressive_batting_alert(team, runs, wickets, overs)

    if condition_explosive_powerplay:
        send_explosive_powerplay_alert(team, runs, wickets, overs)

    if condition_exciting_chase:
        send_exciting_chase_alert(team, runs, wickets, overs, rrr)

    return 240
