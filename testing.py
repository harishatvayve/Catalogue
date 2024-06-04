import pandas as pd
df=pd.read_csv('Console.csv')
for row in df.itertuples(index=False):
    print(row._0,)