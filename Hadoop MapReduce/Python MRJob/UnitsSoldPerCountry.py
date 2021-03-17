from mrjob.job import MRJob
from mrjob.step import MRStep

class UnitsSoldPerCountry(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.sort)]

        def mapper(self,_, line):
                (region, country, c, d, e, f, g, h, USold, j, k, l, m, n) = line.split(',')
                if (region != 'Region'):
                        yield country, float(USold)

        def reducer(self, key, values):
                yield None,(sum(values), key)

        def sort(self,_, values):
                for value, key in sorted(values, reverse=False):
                        yield key, value


if __name__ == '__main__':
        UnitsSoldPerCountry.run()
