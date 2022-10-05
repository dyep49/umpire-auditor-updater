# -*- coding: utf-8 -*-
"""
Created on Fri Jul  1 12:28:24 2022

@author: dydesk
"""

#%%
from dataclasses import dataclass

#%%
@dataclass
class Umpire:
    id: int
    name: str
    
    def get_values(self):
        return tuple(vars(self).values())