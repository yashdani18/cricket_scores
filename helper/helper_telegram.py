import telebot
from config.telegram import API_KEY, CHAT_ID

telebot = telebot.TeleBot(API_KEY)


def send_alert(text: str):
    telebot.send_message(CHAT_ID, text)


def send_pipeline_start_message(pipeline: str):
    send_alert(f'Starting {pipeline} pipeline execution...')


def send_pipeline_end_message(pipeline: str):
    send_alert(f'{pipeline} pipeline executed successfully!')


def send_aggressive_batting_alert(team, runs, wickets, overs):
    send_alert(f'{team} is batting aggressively. {runs}/{wickets} in {overs}')


def send_explosive_powerplay_alert(team, runs, wickets, overs):
    send_alert(f'{team} is killing it in powerplay. {runs}/{wickets} in {overs}')


def send_exciting_chase_alert(team, runs, wickets, overs, rrr):
    send_alert(f'{team} going after an exciting chase. {runs}/{wickets} in {overs}. '
               f'Target: {round(runs + ((20 - overs) * rrr))}')
