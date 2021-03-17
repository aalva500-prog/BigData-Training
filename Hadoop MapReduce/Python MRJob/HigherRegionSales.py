from mrjob.job import MRJob
from mrjob.step import MRStep

class HigherRegionSales(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.sort)]

        def mapper(self,_, line):
                (region, b, c, d, e, f, g, h, USold, j, k, l, m, n) = line.split(',')
                if (region != 'Region'):
                        yield region, int(USold)

        def reducer(self, key, values):
                yield None,(sum(values), key)

        def sort(self,_, values):
                for value, key in sorted(values, reverse=True):
                        yield key, value

if __name__ == '__main__':
        HigherRegionSales.run()
