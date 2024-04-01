import datetime
import time
from pprint import pprint

import requests
from bs4 import BeautifulSoup
import pandas as pd
# from tabulate import tabulate

import mysql.connector

# from prefect import flow, task

from config.mysql import HOST, USER, PASSWORD, DATABASE

from ipl2024.constants import TEAMS, LINK, LOCATION, SCHEDULE_RESULT, TIMESTAMP, GMT_LOCAL_TIME, \
    SCHEDULE_RESULT_ROOT_CLASSES, \
    TEAM1, TEAM2, MATCH_NUMBER, STADIUM, CITY, WINNER, LOCAL_TIME, \
    MATCH_DATE, MATCH_TIME, MATCH_DATETIME, DICTIONARY_TEAMS, \
    URL_ROOT, URL_SCHEDULE_RESULT


# def print_dataframe(p_dataframe):
#     print(tabulate(p_dataframe, headers="keys", tablefmt="psql"))


def extract():
    html = requests.get(URL_SCHEDULE_RESULT).text
    soup = BeautifulSoup(html, 'lxml')

    root_element = soup.find(id='series-matches')

    count = 0
    arr_matches = []
    for class_ in SCHEDULE_RESULT_ROOT_CLASSES:
        rows = root_element.find_all(False, {'class': [class_]})
        print(len(rows))
        for row in rows:
            dictionary = {}
            count += 1
            col_middle_right = row.find(False, {'class': ['cb-col-75 cb-col']})
            col_middle = col_middle_right.find(False, {'class': ['cb-col-60 cb-col cb-srs-mtchs-tm']})
            field_anchor = col_middle.find(class_='text-hvr-underline')
            field_location = col_middle.find(class_='text-gray')
            field_result = col_middle.find(class_='cb-text-complete')
            if field_anchor is not None:
                match_teams = field_anchor.text
                match_link = URL_ROOT + field_anchor['href']
                print(match_teams)
                print(match_link)
                dictionary[TEAMS] = match_teams
                dictionary[LINK] = match_link
            else:
                dictionary[TEAMS] = col_middle.find('span').text
                dictionary[LINK] = None
                print('tag anchor is none')
                continue

            print(field_location.text)
            dictionary[LOCATION] = field_location.text

            if field_result is None:
                field_result = col_middle.find(class_='cb-text-preview')
                if field_result is None:
                    field_result = col_middle.find(class_='cb-text-upcoming')
            dictionary[SCHEDULE_RESULT] = field_result.text
            print(field_result.text)

            col_right = col_middle_right.find(False, {'class': ['cb-col-40 cb-col cb-srs-mtchs-tm']})
            my_time = col_right.find('span')
            print(my_time['timestamp'], my_time['format'], my_time.text)
            dictionary[TIMESTAMP] = my_time['timestamp']
            gmt_local_time = col_right.find(False, {'class': ['cb-font-12 text-gray']})
            dictionary[GMT_LOCAL_TIME] = gmt_local_time.text
            print(gmt_local_time.text)

            arr_matches.append(dictionary)

    # print(arr_matches)
    df = pd.DataFrame(arr_matches)
    # print_dataframe(df)
    df.to_csv('data/extracted/schedule_result.csv', index=False)


def transform():
    df = pd.read_csv('data/extracted/schedule_result.csv')
    # print(df)
    arr_matches = df.to_dict('records')
    # pprint(arr_matches)
    for match in arr_matches:
        '''
        process teams - team 1, teams 2, match_number
        process location - stadium name, city
        process schedule_result - winner (result), ignore for schedule
        process timestamp - not sure if important
        process gmt_local_time - gmt_time, local_time, convert to datetime object
        '''
        # teams
        teams_match_number = match[TEAMS].split(', ')
        teams = teams_match_number[0]
        match[TEAM1] = teams.split(' vs ')[0]
        match[TEAM2] = teams.split(' vs ')[1]
        try:
            match[MATCH_NUMBER] = int(teams_match_number[1][0:-8])
        except:
            print('Exception:', match[TEAMS])
            exit(1)

        # location
        stadium_city = match[LOCATION].split(', ')
        match[STADIUM] = stadium_city[0]
        match[CITY] = stadium_city[-1]

        # schedule_result
        schedule_result_words = match[SCHEDULE_RESULT].split()
        if schedule_result_words[0] != 'Match':
            match[WINNER] = " ".join(match[SCHEDULE_RESULT].split(" ", -4)[:-4])

        # time
        dt_object = datetime.datetime.fromtimestamp(match[TIMESTAMP] / 1000)
        match[MATCH_DATETIME] = dt_object.strftime("%Y-%m-%d %H:%M:%S")
        match[MATCH_DATE] = dt_object.strftime("%Y-%m-%d")
        match[MATCH_TIME] = dt_object.strftime("%H:%M:%S")
        time_elements = match[GMT_LOCAL_TIME].split('\t')
        match[LOCAL_TIME] = time_elements
        pprint(match)

    df = pd.DataFrame(arr_matches)
    df.to_csv('data/transformed/schedule_result.csv', index=False)


def load():
    mydb = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )
    cursor = mydb.cursor()

    df = pd.read_csv('data/transformed/schedule_result.csv')
    arr_matches = df.to_dict('records')
    for index, match in enumerate(arr_matches):
        # print(type(match[WINNER]), match[WINNER])
        sql = f'INSERT INTO schedule_result({TEAM1}, {TEAM2}, {MATCH_NUMBER}, {STADIUM}, {CITY}, {WINNER}, {LINK}, ' \
              f'{MATCH_DATETIME}, {MATCH_DATE}, {MATCH_TIME}, {TIMESTAMP}) ' \
              f'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        val = (DICTIONARY_TEAMS.get(match[TEAM1]), DICTIONARY_TEAMS.get(match[TEAM2]), match[MATCH_NUMBER],
               match[STADIUM], match[CITY],
               DICTIONARY_TEAMS.get(match[WINNER]) if type(match[WINNER]) == str else None, match[LINK],
               match[MATCH_DATETIME], match[MATCH_DATE], match[MATCH_TIME], match[TIMESTAMP])
        cursor.execute(sql, val)
        print(f'Inserted {index + 1}/{len(arr_matches)} records.')
    mydb.commit()


if __name__ == "__main__":
    extract()
    transform()
    load()
