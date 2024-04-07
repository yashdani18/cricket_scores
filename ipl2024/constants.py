URL_ROOT = 'https://www.cricbuzz.com'
URL_SCHEDULE_RESULT = f'{URL_ROOT}/cricket-series/7607/indian-premier-league-2024/matches'

TEAMS = 'teams'
LINK = 'link'
LOCATION = 'location'
SCHEDULE_RESULT = 'schedule_result'
TIMESTAMP = 'timestamp'
GMT_LOCAL_TIME = 'gmt_local_time'

SCHEDULE_RESULT_ROOT_CLASSES = ['cb-col-100 cb-col cb-series-brdr cb-series-matches',
                                'cb-col-100 cb-col cb-series-matches']

TEAM1 = 'team1'
TEAM2 = 'team2'
MATCH_NUMBER = 'match_number'
STADIUM = 'stadium'
CITY = 'city'
WINNER = 'winner'
LOCAL_TIME = 'local_time'
MATCH_DATETIME = 'match_datetime'
MATCH_DATE = 'match_date'
MATCH_TIME = 'match_time'

INDEX_TEAM1 = 0
INDEX_TEAM2 = 1
INDEX_MATCH_NUMBER = 2
INDEX_STADIUM = 3
INDEX_CITY = 4
INDEX_WINNER = 5
INDEX_LINK = 6
INDEX_MATCH_DATETIME = 7
INDEX_MATCH_DATE = 8
INDEX_MATCH_TIME = 9
INDEX_TIMESTAMP = 10

TEAM1_RUNS = 'team1_runs'
TEAM1_WICKETS = 'team1_wickets'
TEAM1_OVERS = 'team1_overs'
TEAM2_RUNS = 'team2_runs'
TEAM2_WICKETS = 'team2_wickets'
TEAM2_OVERS = 'team2_overs'

WON_BY_NUMBER = 'won_by_number'
WON_BY_METRIC = 'won_by_metric'
POTM = 'potm'

DICTIONARY_TEAMS = {
    'Lucknow Super Giants': 'LSG',
    'Punjab Kings': 'PBKS',
    'Chennai Super Kings': 'CSK',
    'Sunrisers Hyderabad': 'SRH',
    'Mumbai Indians': 'MI',
    'Royal Challengers Bengaluru': 'RCB',
    'Gujarat Titans': 'GT',
    'Delhi Capitals': 'DC',
    'Kolkata Knight Riders': 'KKR',
    'Rajasthan Royals': 'RR'
}

# dictionary_teams = {
#     "LSG": "Lucknow Super Giants",
#     "PBKS": "Punjab Kings",
#     "CSK": "Chennai Super Kings",
#     "SRH": "Sunrisers Hyderabad",
#     "MI": "Mumbai Indians",
#     "RCB": "Royal Challengers Bengaluru",
#     "GT": "Gujarat Titans",
#     "DC": "Delhi Capitals",
#     "KKR": "Kolkata Knight Riders",
#     "RR": "Rajasthan Royals"
# }

IPL_START_DATE = '2024-03-22'

INNING = 'inning'
PLAYER_ID = 'player_id'

BATTING_SUMMARY = 'batting_summary'
BATSMEN = 'batsmen'
BATSMAN_NAME = 'batsman_name'
BATSMAN_LINK = 'batsman_link'
BATTING_POSITION = 'batting_position'
BATSMAN_DISMISSAL = 'batsman_dismissal'
BATSMAN_RUNS = 'batsman_runs'
BATSMAN_BALLS = 'batsman_balls'
BATSMAN_FOURS = 'batsman_fours'
BATSMAN_SIXES = 'batsman_sixes'
BATSMAN_STRIKE_RATE = 'batsman_strike_rate'

DISMISSAL_TYPE = 'dismissal_type'
DISMISSAL_BOWLER = 'dismissal_bowler'
DISMISSAL_CAUGHT = 'dismissal_caught'
DISMISSAL_STUMP = 'dismissal_stump'
DISMISSAL_RUN_OUT = 'dismissal_run_out'

EXTRAS_KEY = 'extras'
EXTRAS_TOTAL = 'extras_total'
EXTRAS_DESC = 'extras_desc'
EXTRAS_B = 'extras_b'
EXTRAS_LB = 'extras_lb'
EXTRAS_W = 'extras_w'
EXTRAS_NB = 'extras_nb'
EXTRAS_P = 'extras_p'

TOTAL_KEY = 'total'
TOTAL_VALUE = 'total_value'
TOTAL_DESC = 'total_desc'

BOWLERS = 'bowlers'
BOWLER_NAME = 'bowler_name'
BOWLER_LINK = 'bowler_link'
BOWLING_POSITION = 'bowling_position'
BOWLER_OVERS = 'bowler_overs'
BOWLER_MAIDEN = 'bowler_maiden'
BOWLER_RUNS = 'bowler_runs'
BOWLER_WICKETS = 'bowler_wickets'
BOWLER_NB = 'bowler_nb'
BOWLER_WB = 'bowler_wb'
BOWLER_ECONOMY = 'bowler_economy'

# etl_score_alert
TEAM = 'team'
RUNS = 'runs'
WICKETS = 'wickets'
OVERS = 'overs'
CRR = 'crr'
RRR = 'rrr'

POWERPLAY_OVER_LIMIT = 6
AGGRESSIVE_BATTING_CRR = 10
CHASE_LB = 10
CHASE_UB = 16
CHASE_WICKET_LIMIT = 5
CHASE_MIN_OVER_VAL = 15
