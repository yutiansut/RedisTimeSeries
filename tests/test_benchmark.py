import os
import redis
import pytest
import time
import __builtin__
import math
import random
import statistics 
from rmtest import ModuleTestCase

class RedisTimeseriesTests(ModuleTestCase(os.path.dirname(os.path.abspath(__file__)) + '/../bin/redistimeseries.so')):
  def test_benchmark(self):
    start_ts = 10L
    series_count =  6
  
    samples_count = 1500
    name = 0
    start_time = time.time()

    with self.redis() as r:
      for i in range(series_count):
        for j in range(series_count):
          for k in range(series_count):
            for l in range(series_count):
              for m in range(series_count):
#                for n in range(series_count):
                  r.execute_command('TS.CREATE', name, 'LABELS', 'a', i, 'b', j, 'c', k, 'd', l, 'e', m)#, 'f', n)
                  name += 1      
    print
    print("--- %s seconds ---" % (time.time() - start_time))
    start_time = time.time()

    result = r.execute_command('TS.QUERYINDEX', 'a=0,1,2,3,4,5')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'a=0,1,2,3,4,5', 'b!=(1,2,3,4)')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'a=8')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'c=5')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'b=(1,4)', 'c=(2,3)', 'd!=(4,5)', 'e=(1,2,3)')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'a=1', 'b=3', 'f=5', 'd=2')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=4', 'e!=5')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c!=(1,2,6)', 'e!=5')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))
    start_time = time.time()
    result = r.execute_command('TS.QUERYINDEX', 'b=3', 'c=(1,2,5)', 'e!=5')
    print len(result)
    print("--- %s milliseconds ---" % ((time.time() - start_time) * 1000))