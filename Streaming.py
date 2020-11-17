# -*- coding: utf-8 -*-
import pandas as pd
from lib.ddeclient import DDEClient
from datetime import datetime, timedelta
import time
from RakutenRSS import RakutenRSS
from PriceDatabase import PriceDatabase, TickTable
from TickBuffer import TickBuffer

NK225F = 'N225.FUT01.OS'
TABLE_NAME = 'NK225F'

class Streaming(object):
    
    def __init__(self, code, interval_sec):
        self.old_volume = None
        self.interval = interval_sec
        try:
            self.rss = RakutenRSS(code)
            self.active = True
        except:
            print('Error... Cannot connect RSS.exe')
            self.active = False
        
    def __iter__(self):
        yield self
        
    def tick(self):
        data = (None, None, None)
        while self.active:
            result = self.rss.current()
            #print(result)
            if result is None:
                continue
            [t, p, v] = result
            #print('Tick:', t, p, v)
            if self.old_volume is not None:
                if v > self.old_volume:
                    data = (t, p, float(v - self.old_volume)) 
                    self.old_volume = v
                    break
            else:
                self.old_volume = v
            time.sleep(self.interval)
    
        #print(data)
        return data
    
    def stop(self):
        self.active = False
# -----
     
def beginTime():
    now = datetime.now()
    tbegin = datetime(now.year, now.month, now.day, 9, 0)
    if now > tbegin:
        return now
    else:
        return tbegin

def dayEnd():
    now = datetime.now()
    tend = datetime(now.year, now.month, now.day, 15, 31)
    return tend

def nightEnd():
    now = datetime.now()
    tend = datetime(now.year, now.month, now.day, 5, 31)
    tend += timedelta(days=1)
    return tend
    
    
def ticks():
    db = PriceDatabase()
    now = datetime.now()
    tbegin = beginTime()
    table = TickTable(TABLE_NAME, now.year, now.month)
    if not db.isTable(table.name):
        db.create(table)
    
    buffer = TickBuffer()
    streaming = Streaming(NK225F, 0.05)
    tsave = dayEnd()
    tend = nightEnd()
    while now <= tend:
        data = streaming.tick()
        (t, price, volume) = data
        if t is not None:
            if t > tbegin:
                print('Tick: ', t, price, volume)
                buffer.add([t, price, price, price, volume])
                if buffer.length() > 20:
                    tick_list = buffer.flush()
                    #print(tick_list)
                    db.insert(table, tick_list)
        now = datetime.now()
        if tsave is not None:
            if now > tsave:
                tick_list = buffer.flush()
                #print(tick_list)
                db.insert(table, tick_list)
                tsave = None

    # loop end            
    tick_list = buffer.flush()
    if len(tick_list) > 0:
        db.insert(table, tick_list)
    if buffer.length() > 0:
        db.insert(table, buffer.buffer)
        
        
def test():
    db = PriceDatabase()    
    now = datetime.now()
    table = TickTable(TABLE_NAME, now.year, now.month)
    if not db.isTable(table.name):
        db.create(table)
    
    t = datetime(now.year, now.month, now.day, now.hour, now.minute)
    buffer = TickBuffer()
    for i in range(3):
        t += timedelta(seconds=1)
        buffer.add([t, 10 + i, 100, 100, 10])
    
    for i in range(4):
        buffer.add([t, 100 + i * 200, 200, 200, 30])
        
    for i in range(3):
        t += timedelta(seconds=i)
        buffer.add([t, 1000 + i, 100, 100, 10])
        
    print('1', buffer.buffer)
    l = buffer.flush()
    print('2', l)
    print('3', buffer.buffer)
    
            
if __name__ == '__main__':
    ticks()
    #test()