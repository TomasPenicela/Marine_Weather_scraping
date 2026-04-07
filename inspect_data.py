import pandas as pd
import sqlite3

# Verificar estrutura do Excel
xl = pd.ExcelFile('Opsdata_MarineHistory.xlsx')
print('Sheets no Excel:', xl.sheet_names)
for sheet in xl.sheet_names[:3]:
    df = pd.read_excel('Opsdata_MarineHistory.xlsx', sheet_name=sheet)
    print(f'\n=== {sheet} ===')
    print(f'Dimensões: {df.shape}')
    print(f'Colunas: {list(df.columns)}')
    print('\nPrimeiras 2 linhas:')
    print(df.head(2).to_string())
    print(f'\nTipos de dados:')
    print(df.dtypes)
