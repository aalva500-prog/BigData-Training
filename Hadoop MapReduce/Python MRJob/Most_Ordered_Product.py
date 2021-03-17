from mrjob.job import MRJob
from mrjob.step import MRStep

class Product(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.sort)]

        # Mapper function to obtain the productID in each line
        def mapper(self,_,line):
                (serialNum, orderID, productID, quantity, totalCost, unitCost) = line.split(',')
                yield productID, 1

        # Reducer function to aggregate the data as (key, value) pair and sum all the quantities of the same productID
        def reducer(self, key, values):
                yield None, (sum(values), key)

        # Second reducer function to sort the data and obtain a sorted list with the product most ordered on the top
        def sort(self,_,values):
                for value, key in sorted(values, reverse=True):
                        yield key, value

# Main function
if __name__ == '__main__':
        Product.run()
