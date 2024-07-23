# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 18:37:36 2022

@author: dydesk
"""

#%%
from dataclasses import dataclass

#%%
@dataclass
class Ejection:
    id: str
    game_date: str
    description: str
    home_team: str
    away_team: str
    home_team_id: int
    away_team_id: int
    umpire_name: str
    game_id: int
    timestamp_start_home: str
    timestamp_start_away: str
    home_media_id: str
    away_media_id: str
    start_seconds_home: int
    start_seconds_away: int
    player_id: int
    umpire_id: int
    
    def get_values(self):
        return tuple(vars(self).values())
