from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
cols = 'iso_code,continent,location,date,total_cases,new_cases,total_deaths,new_deaths'.split(
   ',')
class Top10(MRJob):
   def mapper(self, _, line):
       # Convert each line into a dictionary
       row = dict(zip(cols, [a.strip()
                             for a in csv.reader([line]).__next__()]))
       if row['date'] >= "01\/09\/2020" and row['new_cases'] and row['location']:
           #take date and location as key and new cases as value 
            #condition is such that it will only take the previous 2 months starting at September to latest being November
           yield row['location'], int(row['new_cases'])

   def reducer_count_newcases(self, location, new_cases):
           # send all (num_occurrences, word) pairs to the same reducer.
           # num_occurrences is so we can easily use Python's max() function.
       yield None, (int(max(new_cases)), location)

   def secondreducer(self,key,max_cases):
       self.max_list = []
       for value in max_cases:
           self.max_list.append(value)
        
        #In the list where all the cases are append, it will the then sort it from max using the number of new cases.
        #Afterwards the list will be sorted for Top 10 Countries starting from the highest.
       for index in range(10):
           yield max(self.max_list)
           self.max_list.remove(max(self.max_list))

   def steps(self):
       return [
           MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_newcases),
           MRStep(reducer=self.secondreducer)
           ]
if __name__ == '__main__':
   Top10.run()