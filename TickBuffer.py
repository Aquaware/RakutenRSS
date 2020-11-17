# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

class TickBuffer(object):

    def __init__(self):
        self.buffer = []
        
    # tick : (time, bid, ask, mid, volume)
    def add(self, tick):
        if tick is None:
            print('error in add method')
        self.buffer.append(tick)

    def length(self):
        return len(self.buffer)
        
    def flush(self):
        n = len(self.buffer)
        if n < 2:
            return []
        
        t_old = self.buffer[0][0]
        buf = [self.buffer[0]]
        out = []
        for i in range(1, n):
            tick = self.buffer[i]
            t = tick[0]
            if t == t_old:
                buf.append(tick)
            else:
                m = len(buf)
                if m == 1:
                    out.append(buf[0])
                    buf = [tick]
                elif m > 1:
                    msec = int(1000.0 / m)
                    tnew = datetime(t_old.year, t_old.month, t_old.day, t_old.hour, t_old.minute, t_old.second)
                    for i in range(m):
                        (tt, bid, ask, mid, volume) = buf[i]
                        out.append([tnew, bid, ask, mid, volume])
                        tnew += timedelta(milliseconds=msec)
                    buf = [tick]
                else:
                    print('Error')
                t_old = t        
        self.buffer = buf.copy()
        return out