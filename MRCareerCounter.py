from mrjob.job import MRJob

class MRJobCounter(MRJob):
    def mapper(self, key, line):
        (xID, jobID, sOccur, timestamp) = line.split('\t')
        yield sOccur, 1

    def reducer(self, sOccur, occurences):
        yield sOccur, sum(occurences)

if __name__ == '__main__':
    MRJobCounter.run()
