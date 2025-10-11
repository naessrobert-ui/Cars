import pandas as pd

# Filen ligger i samme mappe som skriptet
df = pd.read_parquet('database_biler.parquet', engine='pyarrow')

pd.set_option('display.max_columns', None)

# Vis flere tegn per linje (unngå linjebryting)
pd.set_option('display.width', 200)

# Vis de første 5 radene
print(df.head(10))