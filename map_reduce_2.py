from mrjob.job import MRJob
from mrjob.step import MRStep

class MostScoringPlayer(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_player_points,
                   reducer=self.reducer_aggregate_player_points),
            MRStep(mapper=self.mapper_find_max_player,
                   reducer=self.reducer_get_max_player)
        ]
    
    def mapper_get_player_points(self, _, line):
        fields = line.split(',')
        player = fields[7]
        points = fields[27]
        points = int(points)  
        
        # Yield key as player and value as points scored
        yield player, points

    def reducer_aggregate_player_points(self, player, points_list):
        # Sum all the points scored by each player
        total_points = sum(points_list)
        yield player, total_points  

    def mapper_find_max_player(self, player, total_points):
        # Yield key as player and value as total_point
        yield 'max_player', (player, total_points)
    
    def reducer_get_max_player(self, _, player_points_list):
        # Find the player with the maximum total points
        max_player = max(player_points_list, key=lambda x: x[1])  
        yield max_player  

if __name__ == '__main__':
    MostScoringPlayer.run()
