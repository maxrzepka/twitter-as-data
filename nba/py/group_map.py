import json
from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from dispatch_map import PipeProtocol

class StatOutProtocol(object):
    """  
    """
    def read(self, line):
        #return (None, json.loads(line))
        #print "read in PipProtocol %s" % line
        k_str, v_str = line.split('\t', 1)        
        #values = v_str.split('|')
        #return (k,values)
        return (k, v_str.split('|'))

    def write(self,key,value):
        return ("%s|%s" % ("|".join(key),value))

#TODO implement group from RAW files
class GroupMapJob(MRJob):
    """ From DispatchMapJop output Map/Reduce Job to compute various stats
    """
    #INPUT_PROTOCOL = PipeProtocol
    INPUT_PROTOCOL = StatOutProtocol
    OUTPUT_PROTOCOL = StatOutProtocol
    #INPUT_PROTOCOL = JSONProtocol
    #OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self,k,values):
        #yield k,1
        if k == "time":
            yield ("hour",values[-3]),1
            yield ("minute",values[-2] + values[-3]*100),1
        elif k == "user":
            yield ("user",values[2]),values
            yield ("user_count",values[2]),1
        elif k == "text":
            yield ("lang",values[1]),1
        elif k == "entity":
            size = len(values)
            if size > 1 and values[1]:
                yield ("hastag",values[1]),1
            if size > 2 and values[2]:
                yield ("mention",values[2]),1

    def combiner(self,k,values):
        if k and k[0] == "user":
            yield k,values.next()
        else:
            yield k,sum(values)

    def reducer(self,k,values):
        if k and k[0] == "user":
            yield k,values.next()
        else:
            yield k,sum(values)    
if __name__ == '__main__':
    GroupMapJob.run()
