
from mrjob.job import MRJob

class WordsCount(MRJob):

        def mapper(self,_,line):
                words = line.decode('utf-8','ignore').split()
                for word in words:
                        yield word.lower(), 1

        def reducer(self, key, values):
                yield key, sum(values)

if __name__=='__main__':
