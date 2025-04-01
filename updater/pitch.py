# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 11:30:44 2022

@author: dydesk
"""
#%%
from dataclasses import dataclass
from datetime import datetime

#%%
@dataclass
class Pitch:
    id: str
    game_date: str
    play_description: str
    home_team: str
    away_team: str
    home_team_id: int
    away_team_id: int
    inning: int
    inning_half: str
    outs: int
    bat_side: str
    sz_top: float
    sz_bottom: float
    px: float
    pz: float
    code:  str
    strikes: int
    balls: int
    umpire_name: str
    game_id: int
    datetime_start: datetime
    timestamp_start_home: str
    timestamp_start_away: str
    start_seconds_home: int
    start_seconds_away: int
    timestamp_end_home: str
    timestamp_end_away: str
    home_media_id: str
    away_media_id: str
    home_media_call_letters: str
    away_media_call_letters: str
    home_media_state: str
    away_media_state: str
    correct_call: bool
    batter_id: int
    pitcher_id: int 
    umpire_id: int
    catcher_id: int = None
    player_type_benefit: str = None
    team_benefit: str = None
    team_benefit_id: int = None
    team_hurt: str = None
    team_hurt_id: int = None
    home_away_benefit: str = None
    x_miss: float = None
    y_miss: float = None
    total_miss: float = None
    total_miss_in: float = None
    blown_strikeout: bool = False
    blown_walk: bool = False
    possible_bad_data: bool = False
    
    def get_values(self):
        return tuple(vars(self).values())
