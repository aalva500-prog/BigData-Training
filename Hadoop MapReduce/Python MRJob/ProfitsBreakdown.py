from mrjob.job import MRJob
from mrjob.step import MRStep

class Profit(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.reducer_sort)]

        def mapper(self,_, line):
                (region, b, c, d, e, f, g, h, i, j, k, l, m, Tprofit) = line.split(',')
                if (region != 'Region'):
                        yield region, float(Tprofit)

        def reducer(self, key, values):
                yield None,(sum(values), key)

        def reducer_sort(self,_, values):
                for value, key in sorted(values, reverse=True):
                        yield key, value

if __name__ == '__main__':       
        Profit.run()
        
