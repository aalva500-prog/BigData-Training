
from mrjob.job import MRJob

class MaxTemps(MRJob):
        def convertToFarenheit(self, celsius):
                cel = float(celsius)/10.0
                fahrenheit = cel * 1.8 + 32.0
                return fahrenheit

        def mapper(self,_,line):
                (location, date, type, data, x, y, z, w) = line.split(',')
                if(type == "TMIN"):
                        temperature = self.convertToFarenheit(data)
                        yield location, temperature

        def reducer(self, location, temps):
                yield location, max(temps)

if __name__=='__main__':
        MaxTemps.run()
