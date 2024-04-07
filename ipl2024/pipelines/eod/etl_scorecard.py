import time
from pprint import pprint

import requests
from bs4 import BeautifulSoup
from prefect import task, flow

from helper.helper_db import init_db
from datetime import date

from helper.helper_telegram import send_alert, send_pipeline_start_message, send_pipeline_end_message
from ipl2024.constants import LINK, MATCH_NUMBER, URL_ROOT, \
    BATTING_SUMMARY, BATSMEN, \
    BATSMAN_NAME, BATSMAN_LINK, \
    BATSMAN_DISMISSAL, BATSMAN_RUNS, BATSMAN_RUNS, BATSMAN_BALLS, BATSMAN_FOURS, BATSMAN_SIXES, BATSMAN_STRIKE_RATE, \
    DISMISSAL_TYPE, DISMISSAL_BOWLER, DISMISSAL_CAUGHT, DISMISSAL_STUMP, DISMISSAL_RUN_OUT, \
    EXTRAS_KEY, EXTRAS_TOTAL, EXTRAS_DESC, EXTRAS_B, EXTRAS_LB, EXTRAS_W, EXTRAS_NB, EXTRAS_P, \
    TOTAL_KEY, TOTAL_VALUE, TOTAL_DESC, \
    BOWLERS, \
    BOWLER_NAME, BOWLER_LINK, BOWLER_OVERS, BOWLER_MAIDEN, BOWLER_RUNS, BOWLER_WICKETS, BOWLER_NB, BOWLER_WB, \
    BOWLER_ECONOMY, INNING, PLAYER_ID, BATTING_POSITION, BOWLING_POSITION


def clean_dismissal_string(dismissal: str) -> dict:
    return_obj = {
        DISMISSAL_TYPE: None,
        DISMISSAL_BOWLER: None,
        DISMISSAL_CAUGHT: None,
        DISMISSAL_RUN_OUT: None,
        DISMISSAL_STUMP: None
    }
    first_character = dismissal[0]
    if first_character == 'b':
        return_obj[DISMISSAL_TYPE] = 'bowled'
        return_obj[DISMISSAL_BOWLER] = " ".join(dismissal.split()[1:-1])
    elif first_character == 'c':
        return_obj[DISMISSAL_TYPE] = 'caught'
        return_obj[DISMISSAL_CAUGHT] = dismissal.split(' b ')[0].split('c ')[1]
        return_obj[DISMISSAL_BOWLER] = dismissal.split(' b ')[1][:-1].strip()
    elif first_character == 'r':
        return_obj[DISMISSAL_TYPE] = 'run out'
        return_obj[DISMISSAL_RUN_OUT] = dismissal.split(' (')[1][:-1].strip()[:-1]
        pass
    elif first_character == 'n':
        return_obj[DISMISSAL_TYPE] = 'not out'
        pass
    elif first_character == 's':
        return_obj[DISMISSAL_TYPE] = 'stumping'
        return_obj[DISMISSAL_STUMP] = dismissal.split(' b ')[0].split('st ')[1]
        return_obj[DISMISSAL_BOWLER] = dismissal.split(' b ')[1][:-1].strip()
    elif first_character == 'l':
        return_obj[DISMISSAL_TYPE] = 'lbw'
        return_obj[DISMISSAL_BOWLER] = dismissal.split(' b ')[1].strip()
    else:
        print('Error while parsing dismissal string. Exiting program.')
        exit(1)
    return return_obj


def fetch_scorecard(MATCH_SCORECARD_URL, match_number) -> dict:
    html = requests.get(MATCH_SCORECARD_URL).text
    soup = BeautifulSoup(html, 'lxml')
    innings = ['innings_1', 'innings_2']

    scorecard = {}
    batsmen = []
    bowlers = []
    for index, inning in enumerate(innings):
        current_innings = soup.find(id=inning)

        # Batting figures
        innings_batting_box = current_innings.find(False, {'class': ['cb-col cb-col-100 cb-ltst-wgt-hdr']})

        # Innings batting extras_total: Kolkata Knight Riders Innings 272-7 (20 Ov)
        innings_batting_summary = innings_batting_box.find(False, {'class': ['cb-col cb-col-100 cb-scrd-hdr-rw']})
        # print(innings_batting_summary.text)
        scorecard[f'{inning}_batting_summary'] = innings_batting_summary.text.strip()

        batsmen_rows = innings_batting_box.find_all(False, {'class': ['cb-col cb-col-100 cb-scrd-itms']})
        # Iterating over all rows except the last 3 rows - extras, total, did not bat

        for batting_position, batsman_row in enumerate(batsmen_rows[0:-3]):
            try:
                batsman_name = batsman_row.find(False, {'class': ['cb-col cb-col-25']})
                batsman_link = batsman_name.find('a')['href']
                player_id = batsman_link.split('/')[2]
                dismissal = batsman_row.find(False, {'class': ['cb-col cb-col-33']})
                runs = batsman_row.find(False, {'class': ['cb-col cb-col-8 text-right text-bold']})
                stats = batsman_row.find_all(False, {'class': ['cb-col cb-col-8 text-right']})
                balls, fours, sixes, strike_rate = [stat.text.strip() for stat in stats]
                # print(f'{batsman_name.text}|{batsman_link}|{dismissal}|{runs}|{[stat.text for stat in stats]}')

                dismissal_object = clean_dismissal_string(dismissal.text.strip())

                batsman = {
                    MATCH_NUMBER: match_number,
                    INNING: index + 1,
                    PLAYER_ID: player_id,
                    BATSMAN_NAME: batsman_name.text.strip().split(' (')[0],
                    BATSMAN_LINK: URL_ROOT + batsman_link,
                    BATTING_POSITION: batting_position + 1,
                    DISMISSAL_TYPE: dismissal_object[DISMISSAL_TYPE],
                    DISMISSAL_BOWLER: dismissal_object[DISMISSAL_BOWLER],
                    DISMISSAL_CAUGHT: dismissal_object[DISMISSAL_CAUGHT],
                    DISMISSAL_STUMP: dismissal_object[DISMISSAL_STUMP],
                    DISMISSAL_RUN_OUT: dismissal_object[DISMISSAL_RUN_OUT],
                    BATSMAN_DISMISSAL: dismissal.text.strip(),
                    BATSMAN_RUNS: int(runs.text.strip()),
                    BATSMAN_BALLS: int(balls),
                    BATSMAN_FOURS: int(fours),
                    BATSMAN_SIXES: int(sixes),
                    BATSMAN_STRIKE_RATE: float(strike_rate)
                }

                batsmen.append(batsman)
            except Exception as e:
                print(f'Exception: {str(e)} for {batsman_row.text}')

        # scorecard[f'{inning}_{BATSMEN}'] = batsmen

        # Extracting 3rd row from the end - extras row
        extras_total = batsmen_rows[-3].find(False, {'class': ['cb-col cb-col-8 text-bold cb-text-black text-right']})
        extras_breakup = batsmen_rows[-3].find(False, {'class': ['cb-col-32 cb-col']}).text.strip()
        # (b 4, lb 2, w 15, nb 1, p 0)
        extras_b, extras_lb, extras_w, extras_nb, extras_p = \
            [int(val.split()[1]) for val in extras_breakup[1:-1].split(',')]

        scorecard[f'{inning}_{EXTRAS_KEY}'] = {
            MATCH_NUMBER: match_number,
            INNING: index + 1,
            EXTRAS_TOTAL: int(extras_total.text.strip()),
            EXTRAS_B: extras_b,
            EXTRAS_LB: extras_lb,
            EXTRAS_W: extras_w,
            EXTRAS_NB: extras_nb,
            EXTRAS_P: extras_p
        }

        # Extracting 2nd row from the end - Innings total
        # title = batsmen_rows[-2].find(False, {'class': ['cb-col cb-col-60']})
        # extras_total = batsmen_rows[-2].find(False, {'class': ['cb-col cb-col-8 text-bold text-black text-right']})
        # extras_breakup = batsmen_rows[-2].find(False, {'class': ['cb-col-32 cb-col']})
        # print(f'{title}|{extras_total}|{extras_breakup}')  # Total| 272 | (7 wkts, 20 Ov)

        # scorecard[f'{inning}_{TOTAL_VALUE}'] = int(extras_total.text.strip())
        # scorecard[f'{inning}_{TOTAL_DESC}'] = extras_breakup.text.strip()

        # Bowling figures

        # Bowling figures
        innings_bowling_box = current_innings.find_all(False, {'class': ['cb-col cb-col-100 cb-ltst-wgt-hdr']})[1]
        bowlers_rows = innings_bowling_box.find_all(False, {'class': ['cb-col cb-col-100 cb-scrd-itms']})

        for bowling_position, bowler in enumerate(bowlers_rows):
            bowler_name = bowler.find(False, {'class': ['cb-col cb-col-38']})
            bowler_link = bowler_name.find('a')['href']
            player_id = bowler_link.split('/')[2]
            overs_maiden_nb_wb = bowler.find_all(False, {'class': ['cb-col cb-col-8 text-right']})
            overs, maiden, nb, wb = [val.text.strip() for val in overs_maiden_nb_wb]
            wickets = bowler.find(False, {'class': ['cb-col cb-col-8 text-right text-bold']})
            runs_eco = bowler.find_all(False, {'class': ['cb-col cb-col-10 text-right']})
            runs, economy = [val.text.strip() for val in runs_eco]

            bowler = {
                MATCH_NUMBER: match_number,
                INNING: index + 1,
                PLAYER_ID: player_id,
                BOWLER_NAME: bowler_name.text.strip(),
                BOWLER_LINK: URL_ROOT + bowler_link,
                BOWLING_POSITION: bowling_position + 1,
                BOWLER_OVERS: float(overs),
                BOWLER_MAIDEN: int(maiden),
                BOWLER_RUNS: int(runs),
                BOWLER_WICKETS: int(wickets.text.strip()),
                BOWLER_NB: int(nb),
                BOWLER_WB: int(wb),
                BOWLER_ECONOMY: float(economy),
            }
            bowlers.append(bowler)
        # scorecard[f'{inning}_{BOWLERS}'] = bowlers

    scorecard[BATSMEN] = batsmen
    scorecard[BOWLERS] = bowlers
    return scorecard


@task(name='etl_scorecard_extract')
def extract():
    """
    get current date, and fetch records from db for that date
    visit the corresponding link and get the result
    :return: array of dictionaries
    """
    db, cursor = init_db()
    cursor.execute(f'SELECT {LINK}, {MATCH_NUMBER} FROM schedule_result WHERE match_date = "{date.today()}"')

    # cursor.execute(
        # f'SELECT {LINK}, {MATCH_NUMBER} FROM schedule_result WHERE match_date = "2024-04-03"')
    rows = cursor.fetchall()

    arr_result = []
    for index, row in enumerate(rows):
        MATCH_URL = row[0]
        parts = MATCH_URL.split('/')
        prefix = f'{parts[0]}//{parts[2]}/'
        suffix = f'/{parts[-2]}/{parts[-1]}'
        MATCH_SCORECARD_URL = prefix + 'live-cricket-scorecard' + suffix
        print(MATCH_SCORECARD_URL)
        # print(MATCH_SCOREBOARD_URL)
        result = fetch_scorecard(MATCH_SCORECARD_URL, match_number=row[1])
        # print(row[1], result['innings_1_extras'])
        # print(result)
        arr_result.append(result)
        print(f'Completed fetching {index + 1}/{len(rows)}')
        time.sleep(10)

    print('Extraction Complete!')
    # time.sleep(2)
    return arr_result


@task(name='etl_scorecard_load')
def load(arr_matches):
    db, cursor = init_db()

    for index, match in enumerate(arr_matches):
        batsmen = match[BATSMEN]
        bowlers = match[BOWLERS]

        for batsman in batsmen:
            sql = f"INSERT INTO tbl_batting({MATCH_NUMBER}, {INNING}, {PLAYER_ID}, " \
                  f"{BATSMAN_NAME}, {BATSMAN_LINK}, {BATTING_POSITION}, " \
                  f"{DISMISSAL_TYPE}, {DISMISSAL_BOWLER}, {DISMISSAL_CAUGHT}, {DISMISSAL_STUMP}, " \
                  f"{DISMISSAL_RUN_OUT}, " \
                  f"{BATSMAN_RUNS}, {BATSMAN_BALLS}, {BATSMAN_FOURS}, {BATSMAN_SIXES}, {BATSMAN_STRIKE_RATE}) " \
                  f"VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (batsman[MATCH_NUMBER], batsman[INNING], batsman[PLAYER_ID],
                   batsman[BATSMAN_NAME], batsman[BATSMAN_LINK], batsman[BATTING_POSITION],
                   batsman[DISMISSAL_TYPE], batsman[DISMISSAL_BOWLER], batsman[DISMISSAL_CAUGHT],
                   batsman[DISMISSAL_STUMP], batsman[DISMISSAL_RUN_OUT],
                   batsman[BATSMAN_RUNS], batsman[BATSMAN_BALLS],
                   batsman[BATSMAN_FOURS], batsman[BATSMAN_SIXES], batsman[BATSMAN_STRIKE_RATE])
            cursor.execute(sql, val)

        for bowler in bowlers:
            sql = f"INSERT INTO tbl_bowling({MATCH_NUMBER}, {INNING}, {PLAYER_ID}, " \
                  f"{BOWLER_NAME}, {BOWLER_LINK}, {BOWLING_POSITION}, " \
                  f"{BOWLER_OVERS}, {BOWLER_MAIDEN}, {BOWLER_RUNS}, {BOWLER_WICKETS}, " \
                  f"{BOWLER_NB}, {BOWLER_WB}, {BOWLER_ECONOMY}) " \
                  f"VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (bowler[MATCH_NUMBER], bowler[INNING], bowler[PLAYER_ID],
                   bowler[BOWLER_NAME], bowler[BOWLER_LINK], bowler[BOWLING_POSITION],
                   bowler[BOWLER_OVERS], bowler[BOWLER_MAIDEN], bowler[BOWLER_RUNS], bowler[BOWLER_WICKETS],
                   bowler[BOWLER_NB], bowler[BOWLER_WB], bowler[BOWLER_ECONOMY])
            cursor.execute(sql, val)

        for i in range(2):
            dict_key = f'innings_{i + 1}_extras'
            extras = match[dict_key]
            sql = f"INSERT INTO tbl_extras({MATCH_NUMBER}, {INNING},{EXTRAS_TOTAL}, " \
                  f"{EXTRAS_B}, {EXTRAS_LB}, {EXTRAS_W}, {EXTRAS_NB}, {EXTRAS_P}) " \
                  f"VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (extras[MATCH_NUMBER], extras[INNING], extras[EXTRAS_TOTAL],
                   extras[EXTRAS_B], extras[EXTRAS_LB], extras[EXTRAS_W], extras[EXTRAS_NB], extras[EXTRAS_P])
            cursor.execute(sql, val)
        db.commit()
        print(f'Loading complete {index + 1}/{len(arr_matches)}')
        time.sleep(1)


def flow_etl_scorecard():
    pipeline = 'Scorecard'
    send_pipeline_start_message(pipeline)
    load(extract())
    send_pipeline_end_message(pipeline)


if __name__ == "__main__":
    flow_etl_scorecard()
