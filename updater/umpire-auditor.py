#!/usr/bin/env python3
# coding: utf-8

#%% Import Libraries
import statsapi
import logging
import requests
import pandas as pd
import math
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
import os
import psycopg
from pypika import PostgreSQLQuery, Table
import dataclasses
import sys
import argparse
import hashlib

from pitch import Pitch
from game import Game
from umpire import Umpire
from player import Player
from team import Team
from ejection import Ejection

# Config logging
logger = logging.getLogger('umpireauditor')
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

#%% Conn String

conn_string = os.environ['DB_URL']

#%% Upsert Query

def dataclass_upsert_query(table_name, rows, dc):
    dc_fields = [field.name for field in dataclasses.fields(dc)]
    dc_values = [row.get_values() for row in rows]
    
    db_table = Table(table_name)
      
    q = PostgreSQLQuery.into(db_table)\
        .columns(*dc_fields)\
        .insert(*dc_values)\
        .on_conflict('id')
        
    for i, field in enumerate(dc_fields):
        q = q.do_update(field, dc_values[0][i])
    
      
    return str(q)

#%% Constants

PLATE_WIDTH = 17.0 / 12 / 2
BALL_RADIUS = 2.94 / 12 /2
HALF_STRIKE_ZONE = PLATE_WIDTH + BALL_RADIUS

TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIME_FORMAT_MS = "%Y-%m-%dT%H:%M:%S.%fZ"

#%% Pitch location helpers

def width_strike(pitch):
    return abs(pitch['px']) < HALF_STRIKE_ZONE

def height_strike(pitch):
    return (pitch['pz'] < (pitch['sz_top'] + BALL_RADIUS) and pitch['pz'] > pitch['sz_bottom'] - BALL_RADIUS)

def strike(pitch):
    return width_strike(pitch) and height_strike(pitch)



#%% Convert Timedelta

def convert_timedelta(duration):
    total_seconds = duration.total_seconds()
    
    # Occasional bugs in the API where the date is wrong
    if (total_seconds > 86400 or total_seconds < 0):
        return None
    
    milliseconds = int(total_seconds % 1 * 100)
    seconds = f"{int(total_seconds % 60):02d}"
    minutes = f"{int((total_seconds % 3600) // 60):02d}"
    hours = f"{int(total_seconds // 3600):02d}"
        
    return '{}:{}:{}.{}'.format(hours, minutes, seconds, milliseconds)
    
#%% Game Start Time

def game_start_time(content_id):
    url = 'http://search-api-mlbtv.mlb.com/svc/search/v2/graphql'
    query = """query getStartTime($contentId: ID) {
        Airings(contentId: $contentId) {
            milestones(milestoneType: "BROADCAST_START") {
              milestoneTime {
                startDatetime
              },
            }
          }

    }"""
    
    variables = {'contentId': content_id}
    res = requests.post(url, json={'query': query, 'variables': variables})
    airings = res.json()["data"]["Airings"]
    
    try:
        milestoneTimes = airings[0]["milestones"][0]["milestoneTime"]
        start_time = [t for t in milestoneTimes if t["startDatetime"] != None][0]["startDatetime"]
        return datetime.strptime(start_time, TIME_FORMAT)
    except:
        return None

#%% Add Pitches Function

def add_pitches(game_data):
    game_pitches = []
    game_ejections = []
    
    all_plays = game_data['allPlays']
    start_time_home = game_data['start_time_home']
    start_time_away = game_data['start_time_away']

    for play in all_plays:
        if not 'description' in play['result']:
            continue
        
        description = play['result']['description']
        inning = play['about']['inning']
        inning_half = play['about']['halfInning']
        outs = play['count']['outs']
        play_events = play['playEvents']
        ## WHAT HAPPENS WHEN THERE'S A MID ATBAT PITCHER CHANGE?
        batter_id = play['matchup']['batter']['id']
        pitcher_id = play['matchup']['pitcher']['id']
        pitches = [event for event in play_events if event['isPitch']]
        counts = [{'balls': 0, 'strikes': 0}] + [pitch['count'] for pitch in pitches]
        codes = [pitch['details']['code'] for pitch in pitches]
        ids = [pitch['playId'] for pitch in pitches]
        start_times = [datetime.strptime(pitch['startTime'], TIME_FORMAT_MS) for pitch in pitches]
        end_times = [datetime.strptime(pitch['endTime'], TIME_FORMAT_MS) if "endTime" in pitch else None for pitch in pitches]
        pitches_data = [pitch['pitchData'] for pitch in pitches]
        for i, row in enumerate(pitches_data):
#             if codes[i] == 'X' or codes[i] == 'V' or codes[i] == '*B':
#                 continue
                
            if codes[i] != 'B' and codes[i] != 'C':
                continue
            
            if not 'pX' in row['coordinates']:
                continue
                            
            pitch = {
                "id": ids[i],
                "play_description": description,
                "inning": inning,
                "inning_half": inning_half,
                "outs": outs,
                "sz_top": row['strikeZoneTop'],
                "sz_bottom": row['strikeZoneBottom'],
                "px": row['coordinates']['pX'],
                "pz": row['coordinates']['pZ'],
                "code": codes[i],
                "strikes": counts[i]['strikes'],
                'balls': counts[i]['balls'],
                'timestamp_start_home': convert_timedelta(start_times[i] - start_time_home) if start_time_home else None,  
                'timestamp_start_away': convert_timedelta(start_times[i] - start_time_away) if start_time_away else None,
                'start_seconds_home': (start_times[i] - start_time_home).seconds if start_time_home else None,
                'start_seconds_away': (start_times[i] - start_time_away).seconds if start_time_away else None,
                'batter_id': batter_id,
                'pitcher_id': pitcher_id
            }
            
            if end_times[i]:
                pitch['timestamp_end_home'] = convert_timedelta(end_times[i] - start_time_home) if start_time_home else None
                pitch['timestamp_end_away'] = convert_timedelta(end_times[i] - start_time_away) if start_time_away else None
                
            
            abs_px = abs(pitch['px'])
            
            if pitch['code'] == 'C':
                pitch['correct_call'] = strike(pitch)
                
                if width_strike(pitch):
                    pitch['x_miss'] = 0
                else:
                    pitch['x_miss'] = abs_px - HALF_STRIKE_ZONE
                    
                if height_strike(pitch):
                    pitch['y_miss'] = 0
                elif pitch['pz'] > (pitch['sz_top'] + BALL_RADIUS):
                    pitch['y_miss'] = pitch['pz'] - pitch['sz_top'] - BALL_RADIUS
                else:
                    pitch['y_miss'] = pitch['sz_bottom'] - BALL_RADIUS - pitch['pz']
                    
                if pitch['correct_call'] == False:
                    pitch['total_miss'] = math.sqrt(pitch['x_miss'] ** 2 + pitch['y_miss'] ** 2)
                    pitch['total_miss_in'] = round(pitch['total_miss'] * 12, 2)
                                   
            if pitch['code'] == 'B':
                pitch['correct_call'] = not strike(pitch)
                
            if pitch['correct_call'] == False:
                    
                if pitch['code'] == 'B':
                    pitch['home_away_benefit'] = "away" if inning_half == "top" else "home"
                    pitch['player_type_benefit'] = 'batter'
                    pitch['blown_walk'] = True if pitch['balls'] == 3 else False 
                
                if pitch['code'] == 'C':
                    pitch['home_away_benefit'] = "home" if inning_half == "top" else "away"
                    pitch['player_type_benefit'] = 'pitcher'
                    pitch['blown_strikeout'] = True if pitch['strikes'] == 2 else False 
                    pitch['possible_bad_data'] = pitch['total_miss_in'] > 7
            
            game_pitches.append(pitch)
            
            for event in play_events:
                try:
                    if event['details']['eventType'] == 'ejection':
                        start_time = datetime.strptime(event['startTime'], TIME_FORMAT_MS)
                        
                        ejection = {
                            'description': event['details']['description'],
                            'timestamp_start_home': convert_timedelta(start_time - start_time_home) if start_time_home else None,  
                            'timestamp_start_away': convert_timedelta(start_time - start_time_away) if start_time_away else None,
                            'start_seconds_home': (start_time - start_time_home).seconds if start_time_home else None,
                            'start_seconds_away': (start_time - start_time_away).seconds if start_time_away else None,
                            'player_id': event['player']['id'],
                            'umpire_id': event['umpire']['id']
                        }
                        
                        game_ejections.append(ejection)
                        
                except KeyError:
                    pass
    
    game_pitch_data = {'game_pitches': game_pitches, 'game_ejections': game_ejections}
    return game_pitch_data

#%% Get HP Umpire

def get_hp_umpire(official):
    if (official['officialType'] == 'Home Plate'):
        return True
    else: 
        return False

#%% Parse Player Data

def parse_player_data(player_data):
    if (player_data['isPlayer'] == False):
        return None
    else:    
        return Player(id = player_data['id'], name = player_data['fullName'])

#%% Add Game Data

def add_game_data(pitch, game_data):
    pitch['umpire_id'] = game_data['umpire_id']
    pitch['umpire_name'] = game_data['umpire_name']
    pitch['game_id'] = game_data['game_id']
    pitch['home_team'] = game_data['home_team']
    pitch['away_team'] = game_data['away_team']
    pitch['home_team_id'] = game_data['home_team_id']
    pitch['away_team_id'] = game_data['away_team_id']
    pitch['game_date'] = game_data['game_date']
    
    if pitch['correct_call'] != True:
    
        if pitch['home_away_benefit'] == 'home':
            pitch['team_benefit'] = game_data['home_team']
            pitch['team_benefit_id'] = game_data['home_team_id']
            pitch['team_hurt'] = game_data['away_team']
            pitch['team_hurt_id'] = game_data['away_team_id']
        elif pitch['home_away_benefit'] == 'away':
            pitch['team_benefit'] = game_data['away_team']
            pitch['team_benefit_id'] = game_data['away_team_id']
            pitch['team_hurt'] = game_data['home_team']
            pitch['team_hurt_id'] = game_data['home_team_id']
    
    return Pitch(**pitch)

#%% Add Ejection Data
def add_game_ejection_data(ejection, game_data):
    ejection['id'] = hashlib.sha256((str(game_data['game_id']) + str(game_data['umpire_id']) + str(ejection['player_id'])).encode('utf-8')).hexdigest()
    ejection['umpire_id'] = game_data['umpire_id']
    ejection['umpire_name'] = game_data['umpire_name']
    ejection['game_id'] = game_data['game_id']
    ejection['home_team'] = game_data['home_team']
    ejection['away_team'] = game_data['away_team']
    ejection['home_team_id'] = game_data['home_team_id']
    ejection['away_team_id'] = game_data['away_team_id']
    ejection['game_date'] = game_data['game_date']
    
    return Ejection(**ejection)
    
#%%
def add_game_to_db(game_id):
    
#%%%

    game_data = statsapi.get('game', {'gamePk': game_id})

#%%% CREATE UMPIRE

    # Regular, Wildcard, Divisional, League, WS
    if game_data['gameData']['game']['type'] not in ['R', 'F', 'D', 'L', 'W']:
        return
    
    officials = game_data['liveData']['boxscore']['officials']
    
    # This happens on rainouts
    if len(officials) == 0:
        return
    
    hp_umpire = next(filter(get_hp_umpire, officials))['official']
    hp_umpire_name = hp_umpire['fullName']
    hp_umpire_id = hp_umpire['id']
    
    umpire_obj = Umpire(id = hp_umpire_id, name = hp_umpire_name)
    
    db_umpire_query = dataclass_upsert_query('umpires', [umpire_obj], Umpire)
    
    with psycopg.connect(conn_string, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(db_umpire_query)


#%%% CREATE TEAM

    team_data = game_data['gameData']['teams']
    
    away_team = team_data['away']
    away_team_abbreviation = away_team['abbreviation']
    away_team_name = away_team['name']
    away_team_id = away_team['id']
    
    away_team_obj = Team(
        id = away_team_id,
        name = away_team_name,
        abbreviation = away_team_abbreviation)
    
    home_team = team_data['home']
    home_team_abbreviation = home_team['abbreviation']
    home_team_name = home_team['name']
    home_team_id = home_team['id']
    
    home_team_obj = Team(
        id = home_team_id,
        name = home_team_name,
        abbreviation = home_team_abbreviation)
    
    # db_team_query = dataclass_upsert_query('teams', [away_team_obj, home_team_obj], Team)
    
    db_home_query = dataclass_upsert_query('teams', [home_team_obj], Team)
    db_away_query = dataclass_upsert_query('teams', [away_team_obj], Team)
    
    with psycopg.connect(conn_string, autocommit=True) as conn:
        cur = conn.cursor()
        # cur.execute(db_team_query)
        cur.execute(db_home_query)
        cur.execute(db_away_query)

       
#%%% Game

    game_date = game_data['gameData']['datetime']['officialDate']
    game_type = game_data['gameData']['game']['type']

#%%% ADD PLAYERS

    game_players = game_data['gameData']['players']
    player_rows = list(map(parse_player_data, [*game_players.values()]))
    
    player_queries = []
    
    for player_obj in player_rows:
        player_queries.append(dataclass_upsert_query('players', [player_obj], Player))
    
    db_player_query = ';'.join(player_queries)
    
    with psycopg.connect(conn_string, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(db_player_query)


#%%% ADD PITCHES

    play_data = game_data['liveData']['plays']
    
    ## GATHER MLB.TV BROADCAST DATA XXX THIS SHOULD MAYBE GO INTO GAME TABLE AS WELL
    content = statsapi.get('game_content', {'gamePk': game_id})
    content_items = content['media']['epg'][0]['items']
        
    if (len(content_items) > 1):
        first_item = content_items[0]
        second_item = content_items[1]
        home_feed_id = first_item["contentId"] if first_item["mediaFeedType"] == "HOME" else second_item["contentId"]
        away_feed_id = first_item["contentId"] if first_item["mediaFeedType"] == "AWAY" else second_item["contentId"]
    elif (len(content_items) == 1):
        home_feed_id = content_items[0]["contentId"]
        away_feed_id = content_items[0]["contentId"]
    # else:
    #    continue
    
    play_data['start_time_away'] = game_start_time(away_feed_id)
    play_data['start_time_home'] = game_start_time(home_feed_id)
    
    pitches_data = add_pitches(play_data)
    
    pitch_rows = pitches_data['game_pitches']
    
    pitch_game_data = {
        'umpire_id': hp_umpire_id,
        'umpire_name': hp_umpire_name,
        'game_id': game_id,
        'home_team': home_team_abbreviation,
        'away_team': away_team_abbreviation,
        'home_team_id': home_team_id,
        'away_team_id': away_team_id,
        'game_date': game_date,
    }
    
    pitch_list = list(map(lambda p: add_game_data(p, pitch_game_data), pitch_rows))
    
    pitch_queries = []
    
    for pitch_obj in pitch_list:
        pitch_queries.append(dataclass_upsert_query('pitches', [pitch_obj], Pitch))
    
    db_pitch_query = ';'.join(pitch_queries)
    
    ejection_rows = pitches_data['game_ejections']
    ejection_list = list(map(lambda p: add_game_ejection_data(p, pitch_game_data), ejection_rows))
       
    ejection_queries = []
    
    for ejection_obj in ejection_list:
        ejection_queries.append(dataclass_upsert_query('ejections', [ejection_obj], Ejection))
    
    db_ejection_query = ';'.join(ejection_queries)
    

#%%# Create Game

    df_pitches = pd.DataFrame(pitch_list)   
    
    # Games like the one at Tokyo Dome are regular season but have no pitch tracking
    if len(df_pitches) == 0:
        game_object = Game(
            id = game_id,
            home_team = home_team_abbreviation,
            away_team = away_team_abbreviation,
            game_date = game_date,
            game_type = game_type,
            correct_calls = None,
            incorrect_calls = None,
            total_calls = None,
            calls_benefit_home = None,
            calls_benefit_away = None,
            correct_call_rate = None,
            umpire_name = hp_umpire_name,
            umpire_id = hp_umpire_id,
            home_team_id = home_team_id,
            away_team_id = away_team_id
        )
    else:
        incorrect_calls = df_pitches.loc[df_pitches['correct_call'] == False].sort_values(by='total_miss', ascending=False)
        correct_calls = df_pitches.loc[df_pitches['correct_call'] == True]
        total_calls = df_pitches.loc[df_pitches['correct_call'].isin([True, False])]
        correct_call_rate = (len(correct_calls) / len(total_calls)) * 100
        
        calls_benefit_home = df_pitches.loc[df_pitches['home_away_benefit'] == 'home']
        calls_benefit_away = df_pitches.loc[df_pitches['home_away_benefit'] == 'away']
        
        game_object = Game(
            id = game_id,
            home_team = home_team_abbreviation,
            away_team = away_team_abbreviation,
            game_date = game_date,
            game_type = game_type,
            correct_calls = len(correct_calls),
            incorrect_calls = len(incorrect_calls),
            total_calls = len(total_calls),
            calls_benefit_home = len(calls_benefit_home),
            calls_benefit_away = len(calls_benefit_away),
            correct_call_rate = correct_call_rate,
            umpire_name = hp_umpire_name,
            umpire_id = hp_umpire_id,
            home_team_id = home_team_id,
            away_team_id = away_team_id
        )
    

    db_game_query = dataclass_upsert_query('games', [game_object], Game)
        
    with psycopg.connect(conn_string, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute(db_game_query)
        
        if len(df_pitches) != 0:
            logger.debug("Upserting %s pitches", len(df_pitches))
            cur.execute(db_pitch_query)
        
        if len(ejection_list) != 0:
            cur.execute(db_ejection_query)


#%% Umpire Auditor

def umpire_auditor(sdate, edate):
    delta = edate - sdate

    dates = []

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        dates.append(day)

    game_ids = []

    for d in dates:
        logger.debug('Finding games from date: %s', d)
        sched = statsapi.schedule(date=d)
        date_game_ids = [game['game_id'] for game in sched]
        game_ids = game_ids + date_game_ids

    for gid in game_ids:
        logger.debug('Processing game id: %s', gid)
        add_game_to_db(gid)

parser = argparse.ArgumentParser()

parser.add_argument("-sdate", "--start-date", help="Start of date range to update", type=date.fromisoformat, default=date.today())
parser.add_argument("-edate", "--end-date", help="End of date range to update", type=date.fromisoformat, default=date.today())

args = parser.parse_args()

today = datetime.utcnow().replace(tzinfo=ZoneInfo("America/Los_Angeles")).date()
sdate = today
edate = today

if args.start_date:
    sdate = args.start_date

if args.end_date:
    edate = args.end_date

umpire_auditor(sdate, edate)
