
from mrjob.job import MRJob

class CustomerPurchases(MRJob):

        def mapper(self, _, line):
                (customerId, itemID, money) = line.split(',')
                yield customerId, float(money)

        def reducer(self, customerID, orders):
                yield customerID, sum(orders)

if __name__=='__main__':
        CustomerPurchases.run()