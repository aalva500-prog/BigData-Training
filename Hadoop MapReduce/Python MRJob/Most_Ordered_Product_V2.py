
from mrjob.job import MRJob
from mrjob.step import MRStep

class Product(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.sort)]

        # Mapper to obtain the fields neccesary to get the product most ordered, which are productID and quantity
        def mapper(self,_,line):
                (serialNum, orderID, productID, quantity, totalCost, unitCost) = line.split(',')
                yield productID, int(quantity)

        # Reducer to aggregate the data as key, value pair and summing all the quantities of the same productID
        def reducer(self, key, values):
                yield None, (sum(values), key)

        # Second reducer function to sort the data obtain in reference to the total sum of quantities
        def sort(self,_,values):
                for value, key in sorted(values, reverse=True):
                        yield key, value

if __name__ == '__main__':
        Product.run()
