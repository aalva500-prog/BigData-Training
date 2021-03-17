from mrjob.job import MRJob
from mrjob.step import MRStep

class RevenuePerRegion(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper, reducer=self.reducer)]

        def mapper(self,_, line):
                (region, b, c, d, e, f, g, h, i, j, k, revenue, m, n) = line.split(',')
                if (region != 'Region'):
                        yield region, float(revenue)

        def reducer(self, key, values):
                yield key, sum(values)

if __name__ == '__main__':
        RevenuePerRegion.run()
