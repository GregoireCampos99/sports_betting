# pip install duckdb pandas
import duckdb, pandas as pd

db_path = r"C:/Users/campo/Desktop/sports betting/warehouse.duckdb"
con = duckdb.connect(db_path, read_only=False)  # set True if you only read

# Quick sanity check
print(con.execute("select version(), current_database()").fetchdf())

# Get all tables
query = f"""select * from information_schema.tables"""
df = con.execute(query).fetchdf()
print(df.T)

# Get table with matches 
query = f"""select * from warehouse.main.matches order by 1"""
df_matches = con.execute(query).fetchdf()
df_matches = df_matches[(df_matches.kickoff_ts >= '2025-08-01') & (df_matches.kickoff_ts <= '2025-11-01')]
list_matches_ts = df_matches.sort_values('kickoff_ts').kickoff_ts.unique()
print(df_matches.T)

# Get table with odds 
query = f"""select * from warehouse.main.odds order by 1"""
df_odds = con.execute(query).fetchdf()
print(df_odds.T) # decent


con.close()
