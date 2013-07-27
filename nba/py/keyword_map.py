from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from contextlib import closing

import re, codecs

# Analysis of tweet content :
#   - set up :
#       - input files : id , lang,hashtags, text
#   - relation between search keyword and text
#       - Detect keywords in text
#       - Hastags are also part of the search ?
#       - basic stats : keywords frequency, split by given languages
#   - clean text 
#       - language detection : only select english text
#       - remove stop words for english
#   - Build matrix of terms (features) for all tweets : 
#       - Do some counting to find some other relevant terms (plain text or hashtag)
#       - Most "relevant" terms with td-idf measure
#       - Find topics (clustering) : unsupervised
#       - Classification : manually classify terms (possible classes : about spurs ,about heat , nbafinals ,both )
#       - remove unrelated tweets : No relevant terms found or too generic (for example only one relevant term found like spurs , basketball , nba found)

# Time Analysis 
#   - count tweet by periods : every 30mins , every 15mins, every 5mins, every minutes --> bar charts
#
# Geo Analysis :
#   - find country, US states from geo loc 
#   - count by country/US state
#
# Networks Analysis :
#   - Nodes are hastags and other keywords
#   - hastags/relevant keywords are linked when in same tweet
#   - Community detection


def load_lines(path):
    res = []
    with closing(codecs.open(path, "r", "utf-8")) as f:
        res = [l.strip() for l in f]
    return res

STOP_WORDS = load_lines("en.stop")

def find_terms(text):
    """ only keep important words of text : 
            - greater than 2 characters 
            - exclude words in STOP_WORDS 
            - TODO remove hyperlinks
    """
    terms = filter(lambda s : len(s) > 2 and s not in STOP_WORDS
                   ,re.split('[\[\(\{\)\]\}#@.,;:?! ]+',text.lower()))
    return terms

def detect_keywords(terms,keywords):
    if keywords:
        return sorted(list(set([w for w in terms if w in keywords])))

##TODO build extractor : keywords as set
def extract(text,keywords):
    """ returns t-uple (terms,keywords found) """
    terms = find_terms(text)
    return (' '.join(terms),' '.join(detect_keywords(terms,keywords)))


class PipeOutProtocol(object):
    """ Custom Protocol to output a custom pipe-separated file 
    """

    #TODO add read part to use this protocol as input
    def read(self,line):
        return None,line.split("|")

    def write(self, key, value):
        print "write in PipProtocol " + type(value)
        s = value
        if isinstance(value, (list, tuple)):
            s = ('%s' % ('|'.join(map(lambda s: s.replace("\n"," ").replace("|",":"),value))))
        if key:
            return "|".join((key,s))
        else:
            return s

#
# Only Mapper Job : Process JSON tweet to extract infos and enhance it
#   - extract id , lang , text (cleanup : replace \n and | to other characters), hashtags ()
#   - add job option : set of kewords
#   - find keywords in text --> new field keywords
#   - language detection --> new field reallang
#   - (only for english) remove stopwords, punctuation --> new field terms
#   - output in PipeSV file
class TwitterKeywordJob(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = PipeOutProtocol

    #TODO : how to parametrize it ? as options ?
    KEYWORDS = set(map(lambda s: s.lower(),['MiamiiHeatGang', 'miamiheat', 'spurs', 'nbafinals', 'nba', 'basketball', 'ginobili', 'DwyaneWade', 'tonyparker', 'kingjames', 'nbastats', 'game7']))

    def mapper(self, _, tweet):
        if isinstance(tweet,dict):
            hashtag = ' '.join(map(lambda h: h["text"],tweet["entities"]["hashtags"]))
            mention = ' '.join(map(lambda h: h["screen_name"],tweet["entities"]["user_mentions"]))
            (terms,searchkeys) = extract(tweet.get("text"),self.KEYWORDS)
            yield None,[tweet.get(i,"") for i in ("id_str","lang","text")] + [hashtag,mention,terms,searchkeys]
            #yield None,extract(tweet.get("text"),self.keywords)

    def configure_options(self):
        super(TwitterKeywordJob, self).configure_options()
        self.add_passthrough_option( '--keywords', default='')
        #self.keywords = set(re.split("[,;: ]",self.options.keywords.lower()))
        

#
# Map/Reduce job : Produce stats based on TwitterKeywordJob output
#   - terms frequency --> find relevant terms in this context (game, win, heat)
#   - most found search keywords : which 
#   - most used hashtags
#   - most used entities
class TermStatsJob(MRJob):

    INPUT_PROTOCOL = PipeOutProtocol
    OUTPUT_PROTOCOL = PipeOutProtocol

    def mapper(self,_,row):
        #terms frequencies
        id,lang,text,hastag,mention,terms,search = row
        for term in terms.split(' '):
            yield term,1

    def reducer(self, key, values):
        yield key,sum(values)
    
#  python py/keyword_map.py /home/max/tmp/testnba.txt --output-dir=test1                
if __name__ == '__main__':
    #TwitterKeywordJob.run()
    TermStatsJob.run()
