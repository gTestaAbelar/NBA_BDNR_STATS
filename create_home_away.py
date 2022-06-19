
from pandas import read_csv,isnull
from neo4j import GraphDatabase

uri             = "" # url of your neo4j proyect, for example "bolt://localhost:7687"

userName        = "" # username 

password        = "" # user password

# Connect to the neo4j database server

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

# CQL to query all the universities present in the graph



file_noc = 'games.csv' # change it to the name (or path) of your excel file



with graphDB_Driver.session() as graphDB_Session:
    df = read_csv(file_noc,low_memory=False)
    largo = len(df.index) - 1
    for idx in range(0,largo): 
        # get the information of each row
        wanted_df_slice = df.iloc[[idx]]

        game_id = str(wanted_df_slice.GAME_ID.values[0])
        home_team_wins = bool(wanted_df_slice.HOME_TEAM_WINS.values[0])
        home_team = str(wanted_df_slice.HOME_TEAM_ID.values[0])
        away_team = str(wanted_df_slice.TEAM_ID_away.values[0])
        winner_team = home_team if home_team_wins else away_team

        #home team data

        home_pts = "0" if isnull(wanted_df_slice.PTS_home.values[0]) else str(wanted_df_slice.PTS_home.values[0])
        home_fg_pct = "0" if isnull(wanted_df_slice.FG_PCT_home.values[0]) else str(wanted_df_slice.FG_PCT_home.values[0])
        home_ft_pct = "0" if isnull(wanted_df_slice.FT_PCT_home.values[0]) else str(wanted_df_slice.FT_PCT_home.values[0])
        home_fg3m_pct = "0" if isnull(wanted_df_slice.FG3_PCT_home.values[0]) else str(wanted_df_slice.FG3_PCT_home.values[0])
        home_reb = "0" if isnull(wanted_df_slice.REB_home.values[0]) else str(wanted_df_slice.REB_home.values[0])
        home_ast = "0" if isnull(wanted_df_slice.AST_home.values[0]) else str(wanted_df_slice.AST_home.values[0])

        #away team data

        away_pts = "0" if isnull(wanted_df_slice.PTS_away.values[0]) else str(wanted_df_slice.PTS_away.values[0])
        away_fg_pct = "0" if isnull(wanted_df_slice.FG_PCT_away.values[0]) else str(wanted_df_slice.FG_PCT_away.values[0])
        away_ft_pct = "0" if isnull(wanted_df_slice.FT_PCT_away.values[0]) else str(wanted_df_slice.FT_PCT_away.values[0])
        away_fg3m_pct = "0" if isnull(wanted_df_slice.FG3_PCT_away.values[0]) else str(wanted_df_slice.FG3_PCT_away.values[0])
        away_reb = "0" if isnull(wanted_df_slice.REB_away.values[0]) else str(wanted_df_slice.REB_away.values[0])
        away_ast = "0" if isnull(wanted_df_slice.AST_away.values[0]) else str(wanted_df_slice.AST_away.values[0])
       
        # cypher query to create the relationship "HOME_TEAM" that matches the local team stats of each game
        home_team_query  = 'MERGE (g: Game {id: \'' + game_id +'\'}) MERGE (h: Team {id: \''+ home_team +'\'}) MERGE (g)-[r:HOME_TEAM {PTS: '+ home_pts +', FG_PCT: '+ home_fg_pct +', FT_PCT: '+home_ft_pct+', AST: '+home_ast+', REB: '+home_reb+' }]->(h)'
        # cypher query to create the relationship "AWAY_TEAM" that matches the away team stats of each game
        away_team_query  = 'MERGE (g: Game {id: \'' + game_id +'\'}) MERGE (a: Team {id: \''+ away_team +'\'}) MERGE (g)-[o:AWAY_TEAM {PTS: '+ away_pts +', FG_PCT: '+ away_fg_pct +', FT_PCT: '+away_ft_pct+', AST: '+away_ast+', REB: '+away_reb+' }]->(a)'
        # cypher query to create the relationship "WINNER" that matches the team winner of each game
        winner_team_query = 'MERGE (g: Game {id: \'' + game_id +'\'}) MERGE (t: Team {id: \''+ winner_team +'\'}) MERGE (g)-[w:WINNER]-(t)'
        # Query the graph    
        graphDB_Session.run(home_team_query)
        graphDB_Session.run(away_team_query)
        graphDB_Session.run(winner_team_query)
