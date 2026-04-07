import sqlite3
import pandas as pd
from sqlalchemy import create_engine

# 1. Lê do SQLite (seu arquivo atual)
conn_sqlite = sqlite3.connect('weather.db')
df = pd.read_sql_query("SELECT * FROM temperature", conn_sqlite)

# 2. Conecta ao SQL Server
# O SQLAlchemy criará a tabela automaticamente se ela não existir!
engine_sqlserver = create_engine("mssql+pyodbc://USUARIO:SENHA@SERVIDOR/BANCO?driver=ODBC+Driver+17+for+SQL+Server")

# 3. Envia de forma dinâmica
# Se a tabela 'Port_Temperature' não existir no SQL Server, o Python a cria AGORA.
df.to_sql('Port_Temperature', con=engine_sqlserver, if_exists='replace', index=False)