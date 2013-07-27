import tweepy
import string
import networkx as nx

CONSUMER_KEY = 'ZkpEmsYvjrRmQK0iODTzw'
CONSUMER_SECRET = 'myWcVjgaQkLpZab0fWj0KVaiayZzbnyCHp3RzS8HnI'
ACCESS_TOKEN = '67731745-zPkJ7hwCZXaLgb7nDQECjX6KoWjHoIC16G5a54jQZ'
ACCESS_TOKEN_SECRET = 'HCSnMexLYqeypbWox2p89nFOgldDx6rf5Aj6F4NKA'


TWITTER_ATTRS = ["screen_name","friends_count","followers_count","favourites_count","statuses_count","name"]

class TwitterCollector(object):
    def __init__(self):
	#self.consumer_key = 
	#self.consumer_secret = consumer_secret
	#self.oauth_token = oauth_token
	#self.oauth_token_secret = oauth_token_secret
	self.auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	self.auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
	self.api = tweepy.API(self.auth)

    def followers(self,user):
	return self.api.followers_ids(screen_name=user)

    def followings(self,user):
	return self.api.friends_ids(screen_name=user)

    #structure example ['friends']['/friends/ids']
    def rate_limit(self):
	return self.api.rate_limit_status()['resources']

    def users(self, user_id_list):
        # max number of user_ids is 100
	for i in range(0, len(user_id_list), 100):
	    users = self.api.lookup_users(user_ids=user_id_list[i:i+100])
	    print "Fetch from %d to %d : get %d " % (i, (i+100), len(users))
	    for twitter_user in users:
		yield twitter_user

    def network(self,name,db={}):
	user = self.api.get_user(screen_name=name)
	if user.id not in db:
	   db[user.id] = user 
	followings = self.api.friends_ids(screen_name=name)
	followers = self.api.followers_ids(screen_name=name)
	print "%s has %d following %d followers " % (name, len(followings), len(followers))
	# Get other users not already in db
	ids = list(set(f for f in followers + followings if f not in db))
	print "%d ids to fetch" % len(ids)
	ousers = self.users(ids)
	for ouser in ousers:
	    if ouser.id not in db:
	        db[ouser.id] = ouser
        return ([(user.id,f) for f in followings] + [(f,user.id) for f in followers],db)

    def add_users(self,ids,db):
	users = self.users([i for i in ids if i not in db])
	for u in users:
	    db[u.id] = u
	    
    def fetch_followings(self,ids):
	try:
            for i in ids:
	    	for f in self.api.friends_ids(i):
	            yield (i,f)
	except tweepy.TweepError:
	    print "TweepError found"
	    yield (-1,-1)
	
	
def build_graph(edges,nodes):
    g = nx.DiGraph()
    for (id,model) in nodes.items():
	infos = { k: model.__dict__[k] for k in TWITTER_ATTRS }
	g.add_node(id,infos)
    for (n,p) in edges:
	g.add_edge(n,p)
    return g


def dump_tuple(path,l):
    with open(path,"w") as dfile:
	for (i,j) in l:
	    dfile.write("%d,%d\n" % (i,j))

def dump_users(path,users):
    with open(path,"w") as dfile:
	for u in users:
	    dfile.write("%s\n" % str_user(u))

def str_user(user):
    if not isinstance(user,tweepy.models.User):
	return str(user)
    res = str(user.id)
    for k in TWITTER_ATTRS:
	res += "," 
	try:
	    res += str(user.__dict__[k])
        except UnicodeEncodeError:
 	    res += user.__dict__[k]
	#if isinstance(user.__dict__[k],str):
 	#    res += user.__dict__[k]
	#else:
	#    res += str(user.__dict__[k])
    return res

def find_by(name,nodes):
    l = (u for u in nodes if u.screen_name == name)
    return l.next()

def dump_lines(path,lines):
    with open(path,"w") as dfile:
	for l in lines:
	    u = l.encode('utf-8')
	    dfile.write(u)
	    dfile.write("\n")
	    #dfile.write("%s\n" % l)
	    
def followers2lines(uid,edges,nodes):
    headers = "#id," + string.join(TWITTER_ATTRS + ["Followed"],","); 
    following = {fw: "Yes" for (fl,fw) in edges if fl == uid}
    followers = ((id1,str_user(nodes.get(id1,str(id1) + ","))) for (id1,id2) in edges if id2 == uid)
    return [headers] + [fo + "," + following.get(idf,"") for (idf,fo) in followers] 

def followings2lines(uid,edges,nodes):
    headers = "#id," + string.join(TWITTER_ATTRS + ["Following"],","); 
    followers = {fl: "Yes" for (fl,fw) in edges if fw == uid}
    followings = ((id2,str_user(nodes.get(id2,str(id2) + ","))) for (id1,id2) in edges if id1 == uid)
    return [headers] + [fo + "," + followers.get(idf,"") for (idf,fo) in followings] 

def convertD3(edges,nodes):
    ids = {}
    n = []
    for (i,(k,v)) in enumerate(nodes.items()):
        n.append({s: v.__dict__[s] for s in ["id"] + TWITTER_ATTRS})
	ids[k] = i
    l = []
    for (i,o) in edges:
	l.append({"source": ids[i],"target": ids[o],"value": 1})
    return {"nodes": n,"links": l}

# f = open("force_maxrzepka.json","w")
# f.write(json.dumps(j,indent=4,  sort_keys=True,separators=(',', ': ')))
# f.close()

