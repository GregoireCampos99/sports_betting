# pip install duckdb pandas
import duckdb, pandas as pd

db_path = r"C:/Users/campo/Desktop/sports betting/warehouse.duckdb"
con = duckdb.connect(db_path, read_only=False) 

# Quick sanity check
print(con.execute("select version(), current_database()").fetchdf())

# Get all tables
query = f"""select * from information_schema.tables"""
df = con.execute(query).fetchdf()
print(df)

# Get table with matches 
query = f"""select * from warehouse.main.matches order by 1"""
df_matches = con.execute(query).fetchdf()
list_matches_ts = df_matches.sort_values('kickoff_ts').kickoff_ts.unique()
print(df_matches.T)

# Get table with odds 
query = f"""select * from warehouse.main.odds order by 1"""
df_odds = con.execute(query).fetchdf()
print(df_odds.T) # decent
con.close()

def get_table(table_name):
    query = f"""select * from {table_name} order by 1"""
    df_matches = con.execute(query).fetchdf()
    return df_matches

get_table('warehouse.main.fact_odds').describe()

