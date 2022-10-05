# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 23:35:35 2022

@author: dydesk
"""

#%%
from dataclasses import dataclass

#%%
@dataclass
class Team:
    id: int
    name: str
    abbreviation: str
    
    def get_values(self):
        return tuple(vars(self).values())