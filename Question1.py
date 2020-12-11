from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = cols = 'iso_code,continent,location,date,total_cases,new_cases,total_deaths,new_deaths'.split(
    ',')


class DeathAfrica(MRJob):
    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, [a.strip() for a in csv.reader([line]).__next__()]))

        if row['continent'] == 'Africa' and row['total_deaths']:
            yield row['continent'], int(row['total_deaths'])

    def reducer(self,key,values):
        yield key, max(values)


if __name__ == '__main__':
    DeathAfrica.run()


