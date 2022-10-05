# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 12:28:24 2022

@author: dydesk
"""

#%%
from dataclasses import dataclass

#%%
@dataclass
class Game:
   id: int
   home_team: str
   away_team: str
   game_date: str
   game_type: str
   correct_calls: int
   incorrect_calls: int
   total_calls: int
   calls_benefit_home: int
   calls_benefit_away: int
   correct_call_rate: float
   umpire_name: str
   umpire_id: int
   home_team_id: int
   away_team_id: int
   
   def get_values(self):
       return tuple(vars(self).values())