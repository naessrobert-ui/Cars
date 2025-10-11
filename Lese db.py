import pyarrow.parquet as pq
import pandas as pd

PATH = "database_biler.parquet"
import pyarrow.parquet as pq

pf = pq.ParquetFile("database_biler.parquet")
print("Antall rader:", pf.metadata.num_rows)

# 1) Kun skjema (raskt, leser ikke hele data)
pf = pq.ParquetFile(PATH)
print(pf.schema)              # kolonnenavn + typer
print("Rows:", pf.metadata.num_rows, "Row groups:", pf.num_row_groups)

# 2) Les et lite utsnitt for å se innhold
df = pd.read_parquet(PATH, columns=None)  # evt. columns=[...noen få...]
print(df.head())
print(df.dtypes)
print(df.memory_usage(deep=True).sort_values(ascending=False).head(15))
