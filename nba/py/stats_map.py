from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol


class TwitterStatsJob(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, tweet):
        if isinstance(tweet,dict):
            #yield None,tweet["text"]
            if "coordinates" in tweet.keys() and tweet["coordinates"]:
                yield "Total Coordinates",1
                yield "Coordinates",tweet["coordinates"]
        else:
            yield "Error",tweet

    #def combiner(self, key, values):
    #    yield key, sum(values)

    def reducer(self, key, values):
        if key == "Error":
            yield key, ("Error Sum",len(list(values)))
            for v in values:
                yield key, (key,v)
        elif key.startswith("Total"):
            yield "Total", (key,sum(values))
        #else:
        #    for v in values:
        #        yield None, (key,v)


if __name__ == '__main__':
    TwitterStatsJob.run()

