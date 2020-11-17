import pandas as pd
from lib.ddeclient import DDEClient
from datetime import datetime
import time


NK225F = 'N225.FUT01.OS'


def optionCode(year, month, call_or_put, price_range, should_weekly_option):

    df = pd.read_csv('./rb20201116.csv', encoding='sjis')
    all_codes = list(df['銘柄コード'].values)
    all_contract = list(df['限月'].values)
    all_names = list(df['銘柄名称'].values)
    
    codes = []
    for code, name , contract in zip(all_codes, all_names, all_contract):
        values = name.split('_')
        if len(values) != 4:
            continue
        
        if values[1] != 'NK225':
            continue
        
        if values[0] == 'CAL':
            kind = 'call'
        elif values[0] == 'PUT':
            kind = 'put'
            
        if kind != call_or_put:
            continue
        
        date = values[2]
        if date.find('W') >= 0:
            is_weekly = True
            date = date[0:-1]
        else:
            is_weekly = False
            
        if should_weekly_option and is_weekly == False:
            continue
        
        if should_weekly_option == False and is_weekly:
            continue
        
        y = 2000 + int(date[0:2])
        m = int(date[2:4])
        day = int(date[4:6])
        
        if y != year or m != month:
            continue
        
        price = float(values[3])
        if price_range is not None:
            p0 = price_range[0]
            p1 = price_range[1]
            if p0 is not None:
                if price < p0:
                    continue
            if p1 is not None:
                if price > p1:
                    continue
        codes.append([code, contract, year, month, day, is_weekly, kind, price])
    
    return codes
    
    

class RakutenRSS():
    def __init__(self, code):
        self.dde = None
        self.dde = DDEClient("RSS", code)
        
    def __del__(self):
        self.close()

    def data(self, item):	
        try:
            res = self.dde.request(item).decode('sjis').strip()
        except:
            res = None
        return res

    def close(self):
        if self.dde is not None:
            self.dde.__del__()

    def current(self):
        price = self.data('現在値')
        t =  self.data('現在値詳細時刻')
        volume = self.data('出来高')
        if (t is not None) and (price is not None) and (volume is not None):
            t0 = datetime.now()
            values = t.split(':')
            if len(values) != 3:
                return None
            try:
                h = int(values[0])
                m = int(values[1])
                s = int(values[2])
                tim= datetime(t0.year, t0.month, t0.day, h, m, s)
                return (tim, float(price), int(volume))
            except:
                return None
        else:
            return None

