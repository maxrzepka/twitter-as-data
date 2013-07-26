from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol,JSONProtocol


# Output Format    
    # tweetid , full time , hour , minute , second
    # tweetid , region , long ,lat
    # tweetid , lang , [computed lang ,] text
    # tweetid , userid ,name, some stats
    # tweetid , mentions,hasttags, keywords

#Returns YYYYMMDD HH24 MI
def format_date(d):
    """ datetime object --> [YYYYMMDD,HH,MM,SS] """
    return ["%d%02d%02d" % (d.year,d.month,d.day),d.hour,d.minute,d.second]
    #return "%d%02d%02d" % (d.year,d.month,d.day) + " %02d%02d" % (d.hour,d.minute)

import  time
from datetime import datetime
import locale
def parse_datetime(dt):
    """ Convert a time in string to a more accessbile structure (cf format_date)"""
    locale.setlocale(locale.LC_TIME, 'C')

    # We must parse datetime this way to work in python 2.4
    date = datetime(*(time.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')[0:6]))

    # Reset locale back to the default setting
    locale.setlocale(locale.LC_TIME, '')
    return format_date(date)

def region_map(t):
    """ From tweet Extract geo informations if exists """
    if t["coordinates"]: 
        return [t["id_str"],"-"] + t["coordinates"]["coordinates"]

#TODO add keywords
def extract_entities(t):
    """ From tweet extract hashtag and mention if exists"""
    hashtag = ' '.join(map(lambda h: h["text"],t["entities"]["hashtags"]))
    mention = ' '.join(map(lambda h: h["id_str"] + ' ' + h["screen_name"],t["entities"]["user_mentions"]))
    if hashtag or mention:
        return [t["id_str"],hashtag,mention]

MAPPERS = {}
MAPPERS["time"] = lambda t: [t[i] for i in ("id_str","created_at")] + parse_datetime(t["created_at"])
MAPPERS["region"] =  region_map
MAPPERS["user"] = lambda t: [t["id_str"]] + [t["user"][i] for i in ["name","screen_name","followers_count","friends_count"]]
MAPPERS["entity"] = extract_entities
MAPPERS["text"] = lambda t: [t.get(i,"") for i in ("id_str","lang","text")]

class PipeOutProtocol(object):
    """ Custom Protocol to output a custom pipe-separated file 
    """

    def write(self, key, value):
        #print "write in PipProtocol " + type(value)
        return ('%s\t%s' % (key, '|'.join(map(lambda s: s.replace("\n"," "),value))))
        #return ('%s\t%s' % (key, '|'.join(value)))

class DispatchMapJob(MRJob):
    """ Only Map job to dispatch a tweet into several parts as defined in MAPPERS
    """

    INPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, tweet):
        if isinstance(tweet,dict):
            for k,f in MAPPERS.items():
                v = f(tweet)
                if v:
                    yield k,v
        else:
            yield "Error",tweet

    def mapper_localized(self,_,t):
        """ Returns only localized tweet with minimal informations 
        """
        if isinstance(t,dict) and t["coordinates"]:            
            day,hour,minute,second = parse_datetime(t["created_at"])
            yield None,{"id": t["id_str"],"name": t["user"]["screen_name"]
                   ,"when": t["created_at"],"time": hour*100 + minute
                   ,"position": t["coordinates"]["coordinates"]
                   ,"text": t["text"]}

    def configure_options(self):
        super(DispatchMapJob, self).configure_options()
        self.add_passthrough_option(
            '--output-format', default='json', choices=['pipe', 'json','jvalue'])
        self.add_passthrough_option(
            '--my-mapper', default='full', choices=['full','loc'])
        
    def output_protocol(self):
        if self.options.output_format == 'json':
            return JSONProtocol()
        elif self.options.output_format == 'jvalue':
            return JSONValueProtocol()
        elif self.options.output_format == 'pipe':
            return PipeOutProtocol()    

    def steps(self):
        if self.options.my_mapper == 'loc':
            return [self.mr(mapper=self.mapper_localized)]
        else:
            return super(DispatchMapJob, self).steps()

if __name__ == '__main__':
    DispatchMapJob.run()

