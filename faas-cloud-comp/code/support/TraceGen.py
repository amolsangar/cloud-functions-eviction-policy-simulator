import sys
sys.path.insert(1, '../sim')

from Container import *
from LambdaData import *
import pandas as pd
import numpy as np
from random import choice
import  os
from math import ceil
import random, pickle

class LambdaTrace:
    kinds = ['a','b','c','d']
    mem_sizes = [10, 50, 100, 1000] #MB 
    run_times = [10, 500, 500, 3000] #ms
    warm_times = [5, 100, 400, 2000] #ms 

    workload_mix = [1, 1, 1, 1] #MUST BE integers 
    iat = [50, 50, 50, 50] #ms 
    # iat = list(np.array(warm_times)*10) # IAT is 1-2 orders of magnitude higher than execution time

    def __init__(self):
        self.most_recent_invocation = dict() #time of most recent invocation for each lambda
        self.proportional_lambdas = []
        self.lam_datas = []
        self.lambdas = dict() #terrible name, but older convention ugh
        for i,k in enumerate(self.kinds):
            self.lambdas[k] = (self.mem_sizes[i], 
                                             self.run_times[i], self.warm_times[i])
            self.lam_datas.append(LambdaData(k, self.mem_sizes[i], 
                                             self.run_times[i], self.warm_times[i]))
        self.frac_iat = self.get_frac_iat()
    
    def get_frac_iat(self):
        iats = np.array(self.iat)
        s = sum(self.iat)
        frac_iat = iats/s 
        reciprocal_iat = 1.0/frac_iat 
        recipsum = sum(reciprocal_iat)
        return reciprocal_iat/recipsum
    
    def gen_full_trace(self, n, sample_seed=0):
        #n: number of entries to generate
        trace = []
        self.curr_time = 0 
        
        for i,d in enumerate(self.lam_datas):
            t = 0 
            for _ in range(int(self.frac_iat[i]*n)):
                next_iat_ms = int(np.random.exponential(self.iat[i]))
                t = t + next_iat_ms
                trace.append((d, t))
        
        out_trace = sorted(trace, key=lambda x:x[1]) #(lamdata, t)
        return self.lambdas, out_trace 

class PlannedTrace (LambdaTrace):
    kinds = ['smol','lorge']
    mem_sizes = [200, 2000] #MB 
    run_times = [400, 990] #ms
    warm_times = [300, 600] #ms 

    workload_mix = [1, 1] #MUST BE integers 
    iat = list(np.array(warm_times)*1000)

    def __init__(self):
        self.most_recent_invocation = dict() #time of most recent invocation for each lambda
        self.proportional_lambdas = []
        self.lam_datas = []
        self.lambdas = dict() #terrible name, but older convention ugh
        for i,k in enumerate(self.kinds):
            self.lambdas[k] = (self.mem_sizes[i], 
                                             self.run_times[i], self.warm_times[i])
            self.lam_datas.append(LambdaData(k, self.mem_sizes[i], 
                                             self.run_times[i], self.warm_times[i]))
        self.frac_iat = self.get_frac_iat()
        
        return 
    
    def get_frac_iat(self):
        iats = np.array(self.iat)
        s = sum(self.iat)
        frac_iat = iats/s 
        reciprocal_iat = 1.0/frac_iat 
        recipsum = sum(reciprocal_iat)
        return reciprocal_iat/recipsum
    
    def gen_trace_entry(self):
        pass
    
    def gen_full_trace(self, n, sample_seed=0) :
        #n: number of entries to generate
        trace = []
        self.curr_time = 0 
        
        for i,d in enumerate(self.lam_datas):
            t = 0 
            for _ in range(int(self.frac_iat[i]*n)):
                next_iat_ms = int(np.random.exponential(self.iat[i]))
                t = t + next_iat_ms
                trace.append((d, t))
        out_trace = sorted(trace, key=lambda x:x[1]) #(lamdata, t)
        return self.lambdas, out_trace 
