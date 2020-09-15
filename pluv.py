import pandas as pd
import psycopg2


df = pd.read_csv("CFSV2-01-2020-Diária-IA-ENA.csv", delimiter=";", skiprows=59)
print(df)