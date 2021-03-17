
from mrjob.job import MRJob
from mrjob.step import MRStep

class Speed(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper_get_fair_days, reducer=self.reducer_count)]

        # Mapper function to obtain the data in each line
        def mapper_get_fair_days(self,_, line):
                (day_ind, temp, wx_icon, icon_ixtd, wx_phrase, dewPt, heat_index, rh, pressure,\
                 vis, wc, wdir_cardinal, wspd, uv_desc, feels_like, uv_index, clds, data) = line.split(',')
                # Get the days in which the speed is above 9
                if (wspd != 'wspd' and wspd != ''):
                        if (int(wspd)  > 9):
                                yield None, 1

        # Reducer function to aggregate the data as (key, value) pair and sum all the days in which the speed is above 9
        def reducer_count(self, key, values):
                print("The following number of days(rows) the wind speed was above 9:")
                yield sum(values), key


if __name__ == '__main__':
        Speed.run()
