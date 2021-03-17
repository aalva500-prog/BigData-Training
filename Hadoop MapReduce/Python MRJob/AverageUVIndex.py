
from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageUVIndex(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper_get_fair_days, reducer=self.reducer_avg)]

        # Mapper function to obtain the data in each line
        def mapper_get_fair_days(self,_, line):
                (day_ind, temp, wx_icon, icon_ixtd, wx_phrase, dewPt, heat_index, rh, pressure, vis, wc, wdir_cardinal, wspd, uv_desc, feels_like, uv_index, clds, data) = line.split(',')
                # Get the days that are fair
                if (wx_phrase == 'Fair'):
                        yield wx_phrase, float(uv_index)

        # Reducer function to aggregate the data as (key, value) pair and get the average of uv_index in the days in which wx_phrase was fair
        def reducer_avg(self, key, values):
                count=0
                sum=0
                for value in values:
                        count += 1
                        sum += value
                avg = sum/count
                print("The average UV Index on Fair days is:")
                yield (key, avg)

if __name__ == '__main__':
        AverageUVIndex.run()
