from LambdaData import LambdaData

class Container:
    
    state = "COLD"

    def __init__(self, lamdata: LambdaData):
        self.metadata = lamdata
        
    def prewarm(self):
        self.state = "WARM"
    
    def cfree(self):
        return self.state == "WARM" or self.state == "COLD"
    
    def run(self):
        #returns the time when finished? 
        self.state = "RUNNING"
        
    def terminate(self):
        self.state = "TERM"
    
    def __repr__(self):
        return str(self.metadata.kind)
