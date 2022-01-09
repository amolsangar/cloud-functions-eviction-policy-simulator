import numpy as np
import random
from collections import defaultdict
from LambdaData import *
from Container import *
import os
import operator
import math

class LambdaScheduler:

    def __init__(self, policy:str="RAND", mem_capacity:int=32000, num_funcs:int=10, run:str="a", log_dir=""):
        fname = "{}-{}-{}-{}-".format(policy, num_funcs, mem_capacity, run)

        self.mem_capacity = mem_capacity
        self.mem_used = 0
        self.eviction_policy = policy
        
        self.wall_time = 0              # Current system time
        self.RunningC = dict()          # Container : (launch_time, launch_time+processing_time)
        self.ContainerPool = []         # simple list of `Container`s
        self.FunctionHistoryList = []   # list of tuplies (`LambdaData`, invocation_time)
        self.freq = {}

        self.PerfLogFName = os.path.join(log_dir, fname+"performancelog.csv")
        self.PerformanceLog = open(self.PerfLogFName, "w")
        self.PerformanceLog.write("lambda,time,meta\n")

        self.evdict = defaultdict(int)
        self.capacity_misses = defaultdict(int)

        self.provider_overhead_base = 3000 # 3 seconds
        self.provider_overhead_pct = 0.2 # 20% of function runtime added to cold start
        
        self.TTL = (self.mem_capacity // 400) * 60 * 1000  # Dynamic Memory TTL ms
        self.TTL = max(self.TTL, 6 * 60 * 1000)

        if self.eviction_policy == "RAND":
            # Function to be called pick containers to evict
            self.EvictionFunc = self.RandomEvictionPicker
        elif self.eviction_policy == "CLOUD21":
            # Function to be called pick containers to evict
            self.EvictionFunc = self.TTLEvictionPicker
        elif self.eviction_policy == "LRU":
            # Function to be called pick containers to evict
            self.EvictionFunc = self.LRUEvictionPicker
        elif self.eviction_policy == "LFU":
            # Function to be called pick containers to evict
            self.EvictionFunc = self.LFUEvictionPicker
        elif self.eviction_policy == "RUNTIME_DIFF":
            # Function to be called pick containers to evict
            self.EvictionFunc = self.RunTimeDiffEvictionPicker
        elif self.eviction_policy == "COLD_COST":
            # Function to be called pick containers to evict
            self.EvictionFunc = self.ColdProcEvictionPicker
        else:
            raise NotImplementedError("Unkonwn eviction policy: {}".format(self.eviction_policy))

    ##############################################################

    def WritePerfLog(self, d:LambdaData, time, meta):
        msg = "{},{},{}\n".format(d.kind, time, meta)
        self.PerformanceLog.write(msg)

    ##############################################################

    def AssertMemory(self):
      """ Raise an exception if the memory assumptions of the simulation have been violated """
      used_mem = sum([c.metadata.mem_size for c in self.ContainerPool])
      if used_mem != self.mem_used:
        raise Exception("Container pool mem '{}' does not match tracked usage '{}'".format(used_mem, self.mem_used))
      if used_mem > self.mem_capacity:
        raise Exception("Container pool mem '{}' exceeds capacity '{}'".format(used_mem, self.mem_capacity))

    ##############################################################

    def ColdHitProcTime(self, d:LambdaData) -> float:
      """
      Total processing time for a cold hit on the given lambda
      """
      return self.provider_overhead_base + d.run_time + (self.provider_overhead_pct * d.run_time)

    ##############################################################

    def find_container(self, d: LambdaData):
        """ 
        Search through the containerpool for a non-running container with the sane metadata as `d`
        Return None if one cannot be found
        """
        if len(self.ContainerPool) == 0 :
            return None
        containers_for_the_lambda = [x for x in self.ContainerPool if (x.metadata == d and
                                                     x not in self.RunningC)]

        if containers_for_the_lambda == []:
            return None
        else:
            return containers_for_the_lambda[0]
        # Just return the first element.

    ##############################################################
    
    def find_container_by_id(self, kind):
        if len(self.ContainerPool) == 0 :
            return None
        
        containers_for_the_kind = [x for x in self.ContainerPool if (x.metadata.kind == kind)]

        if containers_for_the_kind == []:
            return None
        else:
            return containers_for_the_kind[0]

    #############################################################

    def container_clones(self, c: Container):
        """ Return all the conatienrs have the same function data as `c` """
        return [x for x in self.ContainerPool if x.metadata == c.metadata]

    ##############################################################

    def CheckFree(self, c):
      """
      Check
      """
      mem_size = c.metadata.mem_size
      return mem_size + self.mem_used <= self.mem_capacity

    ##############################################################

    def AddToPool(self, c: Container):
        """ Add contaienr to the ContainerPool, maintaining bookkeeping """
        mem_size = c.metadata.mem_size
        if mem_size + self.mem_used <= self.mem_capacity:
            #Have free space
            self.mem_used = self.mem_used + mem_size

            self.ContainerPool.append(c)
            return True
        else:
            # print ("Not enough space for memsize, used, capacity.", mem_size, self.mem_used, self.mem_capacity)
            return False

    ##############################################################

    def RemoveFromPool(self, c: Container):
      if c in self.RunningC:
        raise Exception("Cannot remove a running container")
      self.ContainerPool.remove(c)
      self.mem_used -= c.metadata.mem_size

    ############################################################

    def RandomEvictionPicker(self, to_free):
        """ 
        Return victim lists
        Simple eviction that randomly chooses from non-running containers
        """
        eviction_list = []
        # Can't evict running containers!
        # Even with infinite concurrency, container will still exist in running_c
        available = [c for c in self.ContainerPool if c not in self.RunningC]

        while to_free > 0 and len(available) > 0:
            victim = random.choice(available)
            available.remove(victim)
            eviction_list.append(victim)
            to_free -= victim.metadata.mem_size

        return eviction_list  

    #############################################################
    
    # TTL = Time-to-live (5 mins)
    # Evicts expired containers
    def TTLEvictionPicker(self, to_free):
        """ Returns victim list """
        eviction_list = []
        available = [c for c in self.ContainerPool if c not in self.RunningC]

        last_invo = [(c,c.last_invo_time) for c in available]        
        last_invo_sorted = self.Sort_Tuple(last_invo,False)
        eviction_candidates = [c for c,_ in last_invo_sorted if c.last_invo_time + self.TTL < self.wall_time]

        for k in eviction_candidates:
            if(to_free <= 0):
                break
            
            if(to_free > 0 and len(available) > 0):
                victim = k
                available.remove(victim)
                eviction_list.append(victim)
                to_free -= victim.metadata.mem_size
        
        return eviction_list

    #############################################################

    # Evicts Least Recently Used containers
    def LRUEvictionPicker(self, to_free):
        """ 
        Return victim lists
        """
        eviction_list = []
        available = [c for c in self.ContainerPool if c not in self.RunningC]
        eviction_candidates = [c for c in available if c.metadata.kind in self.freq]
        prio_eviction_list = []
        
        for k in eviction_candidates:
            prio = k.last_invo_time
            prio_eviction_list.append((k,prio))
        
        prio_eviction_list = self.Sort_Tuple(prio_eviction_list,False)

        for c in prio_eviction_list:
            if(to_free <= 0):
                break

            if(to_free > 0 and len(available) > 0):
                victim = c[0]
                available.remove(victim)
                eviction_list.append(victim)
                to_free -= victim.metadata.mem_size
        
        return eviction_list

    #############################################################

    # Evicts Least Frequently Used containers
    def LFUEvictionPicker(self, to_free):
        """ 
        Return victim lists
        """
        eviction_list = []
        available = [c for c in self.ContainerPool if c not in self.RunningC]
        eviction_candidates = [c for c in available if c.metadata.kind in self.freq]
        prio_eviction_list = []
        
        for k in eviction_candidates:
            prio = self.freq[k.metadata.kind]
            prio_eviction_list.append((k,prio))
        
        prio_eviction_list = self.Sort_Tuple(prio_eviction_list,False)

        for c in prio_eviction_list:
            if(to_free <= 0):
                break

            if(to_free > 0 and len(available) > 0):
                victim = c[0]
                available.remove(victim)
                eviction_list.append(victim)
                to_free -= victim.metadata.mem_size
        
        return eviction_list

    #############################################################

    # Evicts containers with the lowest cost
    # WARM TIME / COLD TIME
    def RunTimeDiffEvictionPicker(self, to_free):
        """ 
        Return victim lists
        """
        eviction_list = []
        available = [c for c in self.ContainerPool if c not in self.RunningC]
        eviction_candidates = [c for c in available if c.metadata.kind in self.freq]
        prio_eviction_list = []
        
        for k in eviction_candidates:
            run_time = k.metadata.run_time
            if(k.metadata.run_time == 0):
                run_time = 1
            cost = k.metadata.warm_time / run_time
            prio_eviction_list.append((k,cost))
        
        prio_eviction_list = self.Sort_Tuple(prio_eviction_list,False)

        for c in prio_eviction_list:
            if(to_free <= 0):
                break

            if(to_free > 0 and len(available) > 0):
                victim = c[0]
                available.remove(victim)
                eviction_list.append(victim)
                to_free -= victim.metadata.mem_size
        
        return eviction_list

    #############################################################

    # Evicts containers with Highest Cold Processing Time
    def ColdProcEvictionPicker(self, to_free):
        """ 
        Return victim lists
        """
        eviction_list = []
        available = [c for c in self.ContainerPool if c not in self.RunningC]
        eviction_candidates = [c for c in available if c.metadata.kind in self.freq]
        prio_eviction_list = []
        
        for k in eviction_candidates:
            cost = self.ColdHitProcTime(k.metadata)
            prio_eviction_list.append((k,cost))
        
        prio_eviction_list = self.Sort_Tuple(prio_eviction_list,True)

        for c in prio_eviction_list:
            if(to_free <= 0):
                break

            if(to_free > 0 and len(available) > 0):
                victim = c[0]
                available.remove(victim)
                eviction_list.append(victim)
                to_free -= victim.metadata.mem_size
        
        return eviction_list

    #############################################################

    def Eviction(self, d: LambdaData):
        """ Return a list of containers that have been evicted """
        if len(self.RunningC) == len(self.ContainerPool):
            # all containers busy
            return []

        eviction_list = self.EvictionFunc(to_free=d.mem_size)

        for v in eviction_list:
          self.RemoveFromPool(v)
          # self.mem_used -= v.metadata.mem_size
          k = v.metadata.kind
          self.evdict[k] += 1

        return eviction_list

    #############################################################

    # Function to sort the list by second item of tuple
    def Sort_Tuple(self,tup,rev=False): 
        # reverse = True/False (Sorts in Descending/Ascending order) 
        # key is set to sort using second element of sublist 
        return(sorted(tup, key = lambda x: x[1], reverse=rev))
    
    ##############################################################

    def cache_miss(self, d:LambdaData):
        """ 
        A cache miss for the function.
        Create a new Container that has been added to the Container Pool and return it
        Return None if one could not be created

        Evicts non-running containers in an attempt to make room
        """
        c = Container(d)
        if not self.CheckFree(c) : #due to space constraints
          evicted = self.Eviction(d) #Is a list. containers already terminated

        added = self.AddToPool(c)
        if not added:
          # unable to add a new container due to memory constraints
          return None

        return c

    ##############################################################

    def cleanup_finished(self):
        """ Go through running containers, remove those that have finished """
        t = self.wall_time
        finished = []
        for c in self.RunningC:
            (start_t, fin_t) = self.RunningC[c]
            if t >= fin_t:
                finished.append(c)

        for c in finished:
            del self.RunningC[c]

        return len(finished)

    ##############################################################

    def runInvocation(self, d: LambdaData, t = 0):
        """ Entrypoint for the simulation """
        self.wall_time = t
        self.cleanup_finished()

        c = self.find_container(d)
        if c is None:
            #Launch a new container since we didnt find one for the metadata ...
            c = self.cache_miss(d)
            if c is None:
                # insufficient memory
                self.capacity_misses[d.kind] += 1
                return
            c.run()
            processing_time = self.ColdHitProcTime(d)
            self.RunningC[c] = (t, t+processing_time)
            self.WritePerfLog(d, t, "miss")

        else:
            c.run()
            processing_time = d.warm_time
            self.RunningC[c] = (t, t+processing_time)
            self.WritePerfLog(d, t, "hit")

        c.last_invo_time = self.wall_time

        if(d.kind in self.freq):
            self.freq[d.kind] += 1
        else:
            self.freq[d.kind] = 1

        self.FunctionHistoryList.append((d,t))
        self.AssertMemory()

    ##############################################################

    def miss_stats(self):
        """ Go through the performance log."""
        rdict = dict() #For each activation
        with open(self.PerfLogFName, "r") as f:
            line = f.readline() # throw away header
            for line in f:
                line = line.rstrip()
                d, ptime, evtype = line.split(",")
                k = d
                if k not in rdict:
                    mdict = dict()
                    mdict['misses'] = 0
                    mdict['hits'] = 0
                    rdict[k] = mdict

                if evtype == "miss":
                    rdict[k]['misses'] = rdict[k]['misses'] + 1
                elif evtype == "hit":
                    rdict[k]['hits'] = rdict[k]['hits'] + 1
                else:
                    pass

        #Also some kind of response time data?
        return rdict

    ##############################################################
    ##############################################################
    ##############################################################

if __name__ == "__main__":
    from pprint import pprint
    import pickle
    ls = LambdaScheduler(policy="CLOUD21", mem_capacity=2048, num_funcs=20, run="a")

    pth = "../../traces/20-a.pckl"
    with open(pth, "r+b") as f:
        lambdas, input_trace = pickle.load(f)
    print(len(input_trace))

    for d, t in input_trace:
        ls.runInvocation(d, t)

    print("\n\nDONE\n")

    pprint(ls.evdict)
    pprint(ls.miss_stats())
    print("cap", ls.capacity_misses)

