class LambdaData:
    def __init__ (self, kind, mem_size, run_time, warm_time):
        """
        kind - unique identifier for function
        mem_size - amount of memory this function will use, in MB
        run_time - length of time function will run when run cold
        warm_time - length of time function will run when run warm
        """
        self.kind = kind 
        self.mem_size = mem_size
        self.run_time = run_time 
        self.warm_time = warm_time
        
    def __eq__(self, other):
        if isinstance(other, LambdaData):
            return self.kind == other.kind 
        
    def __repr__(self):
        return str((self.kind, self.mem_size))
