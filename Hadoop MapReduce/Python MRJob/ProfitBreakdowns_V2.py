from mrjob.job import MRJob

class Profits(MRJob):

        def mapper(self,_,line):
                (region, country, type, channel, priority, orderDate, orderId, shipDate, unitsSold, price, cost, TRevenue, TCost, TProfit) = line.split(',')
                yield region, float(Tprofit)

        def reducer(self, region, profit):
                yield region, sum(profit)

if __name__ == '__main__':
        Profits.run()