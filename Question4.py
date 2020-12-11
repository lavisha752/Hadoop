from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
cols = 'iso_code,continent,location,date,total_cases,new_cases,total_deaths,new_deaths'.split(
    ',')

class MostCase(MRJob):
    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, [a.strip()
                              for a in csv.reader([line]).__next__()]))

        if row['date'] and row['new_deaths'] and row['location']:
            #take date and location as key and new cases as value
            yield (row['date'], row['location']), int(row['new_cases'])

    def reducer_count_death(self, key, values):
            # send all (num_occurrences, word) pairs to the same reducer.
            # num_occurrences is so we can easily use Python's max() function.
        yield None, ('%04d'%int(max(values)), key)



    def secondreducer(self,key,values):
        self.alist = []
        for value in values:
            self.alist.append(value)
        self.blist = []
        for i in range(10):
            self.blist.append(max(self.alist))
            self.alist.remove(max(self.alist))
        for i in range(10):
            yield self.blist[i]

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    #combiner=self.combine,
                    reducer=self.reducer_count_death),
            MRStep(reducer=self.secondreducer)
            ]

if __name__ == '__main__':
    MostCase.run()
