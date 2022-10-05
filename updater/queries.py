# -*- coding: utf-8 -*-
"""
Created on Sat Jul  2 18:55:08 2022

@author: dydesk
"""

#%% Import libraries

import pandas as pd
import numpy as np
import math
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#%% Connection string

conn = create_engine('postgresql://postgres:postgres@localhost/umpire_auditor')

#%% Load Data

df_pitches_full = pd.read_sql_table('pitches', con=conn)
df_umpires_full = pd.read_sql_table('umpires', con=conn)
df_games_full = pd.read_sql_table('games', con=conn)
df_players_full = pd.read_sql_table('players', con=conn)
df_teams_full = pd.read_sql_table('teams', con=conn)
df_ejections_full = pd.read_sql_table('ejections', con=conn)

df_pitches = df_pitches_full
df_umpires = df_umpires_full
df_games = df_games_full
df_players = df_players_full
df_teams = df_teams_full
df_ejections = df_ejections_full


#%% Subset by date
# subset_date = datetime(2022, 6, 18)

# df_pitches = df_pitches_full.query('game_date == @subset_date')
# df_games = df_games_full.query('game_date == @subset_date')

#%% Subset by date range
subset_date_start = datetime(2022, 1, 1)
subset_date_end = datetime(2023, 1, 1)

df_pitches = df_pitches_full.query('game_date >= @subset_date_start & game_date <= @subset_date_end')
df_games = df_games_full.query('game_date >= @subset_date_start & game_date <= @subset_date_end')
df_ejections = df_ejections.query('game_date >= @subset_date_start & game_date <= @subset_date_end')

#%% Bad calls

blown_calls = df_pitches.query('correct_call == False').sort_values(by='total_miss', ascending=False)

#%% Bad strikeouts

blown_strikeouts_full = df_pitches.query('blown_strikeout == True').sort_values(by='total_miss', ascending=False)
blown_strikeouts_simple = blown_strikeouts_full[["game_date", "play_description", 'home_team', 'away_team', 'inning', 'inning_half', 'outs', 'sz_top', 'sz_bottom', 'px', 'pz', 'strikes', 'balls', 'umpire_name', 'start_seconds_home', 'start_seconds_away', 'team_benefit', 'x_miss', 'y_miss', 'total_miss_in']]

#%% Bad Walks

blown_walks_full = df_pitches.query('blown_walk == True').sort_values(by='pz', ascending=False)
blown_walks_simple = blown_walks_full[["game_date", "play_description", 'home_team', 'away_team', 'inning', 'inning_half', 'outs', 'sz_top', 'sz_bottom', 'px', 'pz', 'strikes', 'balls', 'umpire_name', 'start_seconds_home', 'start_seconds_away', 'team_benefit', 'x_miss', 'y_miss', 'total_miss_in']]

#%% Umpire Report

umpire_report = df_games.groupby('umpire_id')\
    .agg({
        'umpire_name': 'first', 
        'incorrect_calls': 'sum',
        'correct_calls': 'sum',
        'total_calls': 'sum'
    })\
    .assign(correct_call_rate = lambda dataframe: dataframe['correct_calls'] / dataframe['total_calls'])\
    .sort_values(by="correct_call_rate", ascending=False)\
    .query("total_calls > 1")\
    .reset_index()

#%% Season aggregate

df_season = df_games_full\
    .assign(season = lambda df: df['game_date'].dt.year)\
    .pivot_table(index = ['season'], values=['correct_calls', 'incorrect_calls', 'total_calls'], aggfunc=np.sum)\
    .assign(correct_call_rate = lambda df: df['correct_calls'] / df['total_calls'])

#%% Player report

df_batter_pitches = df_players.merge(blown_calls, left_on='id', right_on='batter_id')\
    .assign(is_x_miss = lambda df: df['x_miss'] > 0,
            is_y_miss = lambda df: df['y_miss'] > 0,
            is_blown_strike = lambda df: df['code'] == 'C',
            is_blown_ball = lambda df: df['code'] == 'B')
    
df_batter_ranking = df_batter_pitches\
    .pivot_table(index = ['batter_id', 'name'], values=['is_blown_strike', 'is_blown_ball', 'is_x_miss', 'is_y_miss', 'blown_strikeout', 'blown_walk'], aggfunc=np.count_nonzero)

df_pitcher_pitches = df_players.merge(blown_calls, left_on='id', right_on='pitcher_id')\
    .assign(is_x_miss = lambda df: df['x_miss'] > 0,
            is_y_miss = lambda df: df['y_miss'] > 0,
            is_blown_strike = lambda df: df['code'] == 'C',
            is_blown_ball = lambda df: df['code'] == 'B')

df_pitcher_ranking = df_pitcher_pitches\
    .pivot_table(index = ['pitcher_id', 'name'], values=['is_blown_strike', 'is_blown_ball', 'is_x_miss', 'is_y_miss', 'blown_strikeout', 'blown_walk'], aggfunc=np.count_nonzero)

#%% Team report

df_team_report_benefit = df_teams.merge(blown_calls, left_on='id', right_on='team_benefit_id')\
    .pivot_table(index = ['id_x', 'name'], values=['id_y'], aggfunc=np.count_nonzero)
    
df_team_report_hurt = df_teams.merge(blown_calls, left_on='id', right_on='team_hurt_id')\
    .pivot_table(index = ['id_x', 'name'], values=['id_y'], aggfunc=np.count_nonzero)



#%% Generate streamglob cmd


def download_pitch(pitch):
       
    offset_time = pitch.start_seconds_away if pitch.home_away_benefit == 'home' else pitch.start_seconds_home
    cmd = f'streamglob download://mlb/{pitch.game_date.strftime("%m-%d-%Y")}.{pitch.team_hurt}.1:offset={int(offset_time)}' 
    print(cmd)

#%%



# blown_calls\
#     .head(n=20)\
#     .apply(download_pitch, axis=1)
    
# tmp = blown_calls[['umpire_name', 'total_miss_in', 'home_team', 'away_team', 'game_date']].head(n=11)

blown_strikeouts_full\
    .head(n=20)\
    .apply(download_pitch, axis=1)
    
tmp = blown_strikeouts_full[['umpire_name', 'total_miss_in', 'home_team', 'away_team', 'game_date']].head(n=11)
    
# Could do extremes like disproportionatlity between home/away benefit
# High leverage
# 

#%%
# tmp = blown_calls\
#     .query("total_miss_in > 5")\
#     .groupby('game_id')['id']\
#     .count()

