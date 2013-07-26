from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

import re


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


def find_terms(text):
    terms = filter(lambda s : len(s) > 0,re.split('[\[\(\{\)\]\}#@.,;:?! ]+',text.lower()))
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

    def write(self, key, value):
        #print "write in PipProtocol " + type(value)
        #return ('%s\t%s' % (key, '|'.join(map(lambda s: s.replace("\n"," "),value))))
        return ('%s' % ('|'.join(map(lambda s: s.replace("\n"," ").replace("|",":"),value))))
        #return ('%s\t%s' % (key, '|'.join(value)))

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
            (terms,searchkeys) = extract(tweet.get("text"),self.KEYWORDS)
            yield None,[tweet.get(i,"") for i in ("id_str","lang","text")] + [hashtag,terms,searchkeys]
            #yield None,extract(tweet.get("text"),self.keywords)

    def configure_options(self):
        super(TwitterKeywordJob, self).configure_options()
        self.add_passthrough_option( '--keywords', default='')
        #self.keywords = set(re.split("[,;: ]",self.options.keywords.lower()))
        

#  python py/keyword_map.py /home/max/tmp/testnba.txt --output-dir=test1                
if __name__ == '__main__':
    TwitterKeywordJob.run()
