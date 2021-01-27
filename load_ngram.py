import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite:///ngram.db', echo=False)
sqlite_connection = engine.connect()
df = pd.read_csv('ngram_2016_2019.csv')
df.to_sql('ngram_2016_2019', sqlite_connection, if_exists='replace', index=False, chunksize=10000, method='multi')