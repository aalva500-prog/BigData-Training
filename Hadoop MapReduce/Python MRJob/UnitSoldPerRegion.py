
from mrjob.job import MRJob
from mrjob.step import MRStep

class UnitsSoldPerRegion(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.sort)]

        def mapper(self,_, line):
                (region, b, Itype, d, e, f, g, h, uSold, j, k, l, m, n) = line.split(',')
                if (region != 'Region'):
                        yield Itype, int(uSold)

        def reducer(self, key, values):
                yield None, (sum(values), key)

        def sort(self,_,values):
                for value, key in sorted(values, reverse=True):
                        yield key, value

if __name__ == '__main__':
        UnitsSoldPerRegion.run()
