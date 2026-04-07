import pandas as pd

# Carregar histórico
excel = pd.read_excel('Opsdata_MarineHistory.xlsx')

print('Colunas:', excel.columns.tolist())
print(f'\nTotal registros: {len(excel)}')
print(f'Data range: {excel["Date"].min()} a {excel["Date"].max()}')
print(f'\nRazões de atraso (principais 15):')
print(excel['Reason description'].value_counts().head(15))
print(f'\nCategoria (principais):')
print(excel['Category description'].value_counts().head(10))
