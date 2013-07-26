from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

#TWEET_ATTRS = ["id","created"

class TwitterLocalizedJob(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, tweet):
        if isinstance(tweet,dict):
            #yield None,tweet["text"]
            if "coordinates" in tweet.keys() and tweet["coordinates"]:
                infos = {"coordinates": tweet["coordinates"]["coordinates"],"name": tweet["user"]["name"],"id": tweet["id"],"when": tweet["created_at"],"text": tweet["text"]}
                #yield "Total Coordinates",1
                yield tweet["id_str"],infos
        else:
            yield "Error",tweet

    #def combiner(self, key, values):
    #    yield key, sum(values)

    def reducer(self, key, values):
        if key != "Error":
            for v in values:
                yield None,v 

        #if key == "Error":
        #    yield key, ("Error Sum",len(list(values)))
        #else:
        #    for v in values:
        #        yield None,v 
        #     for v in values:
        #         yield key, (key,v)
        # elif key.startswith("Total"):
        #     yield "Total", (key,sum(values))
        # else:
        #     for v in values:
        #         yield None, (key,v)


if __name__ == '__main__':
    TwitterLocalizedJob.run()

