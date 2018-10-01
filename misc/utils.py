# -*- coding: utf-8 -*-
"""
Created on Thu May 10 15:34:14 2018

@author: Vanja Smailovic
"""

# import a local module from a different path 
# abysmal code due to lack of modularization support for Notebooks 
import os, sys
from os.path import dirname 
module_path = os.path.abspath(os.path.join('..'))
parent = dirname(module_path)
if module_path not in sys.path:
    sys.path.append(module_path)
if parent not in sys.path:
    sys.path.append(parent)

from furl import furl


def cast_int(s) -> int:
    """
    Extracts int from price string. 
    """
    return int(''.join(char for char in s if char.isdigit()))
	

def clean_owner_urls(jobs) -> list:
    """
    Removes "&sort" param in order to default to "newest first". 
    """
    return [(owner,                             
             furl(url).remove(['sort']).url,   
             pmin,                              
             pmax)                              
             for (owner,url,pmin,pmax) in jobs]


def parse_page(url) -> str:
    """
    Parses and retrieves a page from URL parameter. 
    """
    page = ''
    for p in url.split('&'): 
        if 'o=' in p:
            page = p.lstrip('o=')
    return page 


