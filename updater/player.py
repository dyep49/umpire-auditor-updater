# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 23:39:08 2022

@author: dydesk
"""

#%%
from dataclasses import dataclass

#%%
@dataclass
class Player:
    id: int
    name: str
    
    def get_values(self):
        return tuple(vars(self).values())