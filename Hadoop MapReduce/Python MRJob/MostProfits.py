
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostProfits(MRJob):

        def steps(self):
                return [
                        MRStep(mapper = self.mapper, reducer=self.reducer),
                        MRStep(reducer=self.postSort)]


        # Mapper function to get each line and split it, then get the field required
        def mapper(self,_,line):
                fulline = line.split(",")
                # verify that the first row is not taken
                if(fulline[0] != "Region"):
                        # Get the first and last field from each row
                        region,totalprofit = fulline[0],float(fulline[-1])
                        yield region, totalprofit

        # Reducer function to aggregate the data as (key, value) pair
        def reducer(self, key, values):
                yield None, (sum(values),key)

        # Second reducer to sort the data
        def postSort(self, key, values):
                for value,key in (sorted(values)):
                        yield key,value

if __name__=='__main__':
        MostProfits.run()
