import time, resource, multiprocessing, os, sys

def benchmark(runs, func, *args, **kw):
    def _benchmark(runs, func, args, kw):
        total_runs = runs
        start = time.clock()
        try:
            while runs:
                v = func(*args, **kw)
                runs -= 1
        finally:
            end = time.clock()
            maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            print '\t%s Total Time: %s (%s runs, ~%s, %s MB)' % (func.__name__, end-start, total_runs, (end-start)/total_runs, maxrss/1024.0)
        print v

    process = multiprocessing.Process(target=_benchmark, args=(runs, func, args, kw))
    process.start()
    process.join()

def replace
