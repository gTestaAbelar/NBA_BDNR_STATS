
from pandas import read_csv,isnull
from neo4j import GraphDatabase

uri             = "" # url of your neo4j proyect, for example "bolt://localhost:7687"

userName        = "" # username 

password        = "" # user password

# Connect to the neo4j database server

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))



file_noc = 'games_details.csv' # change it to the name (or path) of your excel file



with graphDB_Driver.session() as graphDB_Session:
    df = read_csv(file_noc,low_memory=False)
    largo = len(df.index) - 1
    for idx in range(0,largo): 
        # get the information of each row
        wanted_df_slice = df.iloc[[idx]]
        game_id = str(wanted_df_slice.GAME_ID.values[0])
        name = '\"'+str(wanted_df_slice.PLAYER_NAME.values[0])+'\"'
        player_id = str(wanted_df_slice.PLAYER_ID.values[0])
        team_id = str(wanted_df_slice.TEAM_ID.values[0])
        secs = '\"DNP\"' # defalut value that represents "Do not play"
        # if Min column has some value, we need to calculate the seconds and store it
        if not isnull(wanted_df_slice.MIN.values[0]) :
            # we split in minutes and seconds
            mins_slice = str(wanted_df_slice.MIN.values[0]).split(":")
            # if play at least 1 minute, we need to calculate that time in seconds
            if len(mins_slice) > 1 :
                secs = str(int(mins_slice[0])*60+int(mins_slice[1]))
            # and then, add the seconds of the time played
            else :
                secs = str(int(mins_slice[0])*60)
        fgm = "0" if isnull(wanted_df_slice.FGM.values[0]) else str(wanted_df_slice.FGM.values[0])
        fga = "0" if isnull(wanted_df_slice.FGA.values[0]) else str(wanted_df_slice.FGA.values[0])
        fg3a = "0" if isnull(wanted_df_slice.FG3A.values[0]) else str(wanted_df_slice.FG3A.values[0])
        fg3m = "0" if isnull(wanted_df_slice.FG3M.values[0]) else str(wanted_df_slice.FG3M.values[0])
        ftm = "0" if isnull(wanted_df_slice.FTM.values[0]) else str(wanted_df_slice.FTM.values[0])
        fta = "0" if isnull(wanted_df_slice.FTA.values[0]) else str(wanted_df_slice.FTA.values[0])
        oreb = "0" if isnull(wanted_df_slice.OREB.values[0]) else str(wanted_df_slice.OREB.values[0])
        dreb = "0" if isnull(wanted_df_slice.DREB.values[0]) else str(wanted_df_slice.DREB.values[0])
        reb = "0" if isnull(wanted_df_slice.REB.values[0]) else str(wanted_df_slice.REB.values[0])
        ast = "0" if isnull(wanted_df_slice.AST.values[0]) else str(wanted_df_slice.AST.values[0])
        stl = "0" if isnull(wanted_df_slice.STL.values[0]) else str(wanted_df_slice.STL.values[0])
        blk = "0" if isnull(wanted_df_slice.BLK.values[0]) else str(wanted_df_slice.BLK.values[0])
        to = "0" if isnull(wanted_df_slice.TO.values[0]) else str(wanted_df_slice.TO.values[0])
        pf = "0" if isnull(wanted_df_slice.PF.values[0]) else str(wanted_df_slice.PF.values[0])
        pts = "0" if isnull(wanted_df_slice.PTS.values[0]) else str(wanted_df_slice.PTS.values[0])
        # cypher query to create the players nodes and their relationship "PLAYED" that matches each player with the games they played and their stats 
        cqlNodeQuery  = 'MERGE (g: Game {id: \'' + game_id +'\'}) MERGE (p: Player {id: \''+ player_id +'\', name: '+ name +'}) MERGE (p)-[r:PLAYED {team: \''+ team_id +'\', SECS: '+ secs +', FGM: '+fgm+', FGA: '+fga+', FG3A: '+fg3a+',FG3M: '+fg3m+',FTM: '+ftm+', FTA: '+fta+', OREB: '+oreb+',DREB: '+dreb+', REB: '+reb+', AST: '+ast+',STL: '+stl+', BLK: '+blk+', TO: '+to+', PF: '+pf+', PTS: '+pts+' }]->(g)'
        
        # Query the graph    
        nodes = graphDB_Session.run(cqlNodeQuery)
