from mrjob.job import MRJob
from mrjob.step import MRStep

class MostScoringQuarter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_points,
                   reducer=self.reducer_aggregate_points),
            MRStep(mapper=self.mapper_find_max_quarter,
                   reducer=self.reducer_get_max_quarter)
        ]
    
    def mapper_get_points(self, _, line):
        
        fields = line.split(',')
        quarter = fields[5]
        team = fields[11]
        points = fields[27]
        points = int(points)  
        
        # Yield key as (team, quarter) and value as points scored
        yield (team, quarter), points

    def reducer_aggregate_points(self, team_quarter, points_list):
        # Sum all the points scored by a team in a particular quarter
        total_points = sum(points_list)
        yield team_quarter[0], (team_quarter[1], total_points)  
    
    def mapper_find_max_quarter(self, team, quarter_points):
        # Extract the quarter and total points scored for each team
        yield team, quarter_points
    
    def reducer_get_max_quarter(self, team, quarter_points_list):
        # Find the quarter with the maximum points for the team
        max_quarter = max(quarter_points_list, key=lambda x: x[1])  
        yield team, max_quarter  

if __name__ == '__main__':
    MostScoringQuarter.run()