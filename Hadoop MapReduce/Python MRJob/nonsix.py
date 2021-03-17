from mrjob.job import MRJob
from mrjob.step import MRStep

class Visibility(MRJob):

        def steps(self):
                return[MRStep(mapper=self.mapper_get_visibility, reducer=self.reducer_count)]

        # Mapper function to obtain the data in each line
        def mapper_get_visibility(self,_, line):
                (day_ind, temp, wx_icon, icon_ixtd, wx_phrase, dewPt, heat_index, rh, pressure, \
                vis, wc, wdir_cardinal, wspd, uv_desc, feels_like, uv_index, clds, data) = line.split(',')
                # Get the days in which visibility is not 6
                if (vis != 'vis' and vis != '6'):
                        yield None, 1

        # Reducer function to aggregate the data as (key, value) pair and sum all the days in which vis is not 6
        def reducer_count(self, key, values):
                print("The visibility was not a 6 in the following number of days (rows):")
                yield sum(values), key

if __name__ == '__main__':
        Visibility.run()
