# -*- coding: utf-8 -*-
"""
Created on Thu May 10 15:34:14 2018

@author: Vanja Smailovic
"""

def extract_from_css(response, index, query) -> str:
    """ 
    Extracts using CSS selector. 
    """ 
    resp = response.css(query)
    if resp:
        ext = resp.extract()[index].lstrip().rstrip()
        if len(ext) == 0:
            return ''
        else:
            return ext
    else:
        return ''
            

def transform_locations(input) -> list:
    """ 
    Edits and returns a correct list of car locations. 
    Blocket has a messed-up CSS selector for location, 
    e.g. if the locations are 'Bromma' and 'Butik Nacka', 
    it will return ['Bromma', 'Butik', 'Nacka']. 
    """ 
    out = [ ]
    for i, item in enumerate(input):
        # first 
        if i==0:        
            next = input[i+1] 
            next = next.lstrip().rstrip()
            if item=='Butik':
                out.append(item + ' ' + next) 
            else:
                out.append(item)
        # last
        elif i==len(input)-1:   
            prev = input[i-1]
            prev = prev.lstrip().rstrip()
            if prev=='Butik':
                continue
            else:
                out.append(item)
        # middle
        else:
            prev = input[i-1]
            prev = prev.lstrip().rstrip()
            next = input[i+1] 
            next = next.lstrip().rstrip()
            if prev=='Butik':
                continue
            elif item=='Butik':
                out.append(item + ' ' + next)
            else:
                out.append(item)
    return out 
    
