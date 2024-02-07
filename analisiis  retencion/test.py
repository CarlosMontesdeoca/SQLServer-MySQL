import pandas as pd

_2022  = pd.read_excel('doc.xlsx', sheet_name='Hoja1')
_2023  = pd.read_excel('doc.xlsx', sheet_name='Hoja2')

_2022['ruc'] = _2022['ruc'].astype(str)
_2023['ruc'] = _2023['ruc'].astype(str)


merged_data = pd.merge(_2022, _2023, on=['ruc', 'cliente'], how='outer', suffixes=('_2022', '_2023'))

# Guardar los resultados en un nuevo archivo Excel
merged_data.to_excel('resultado.xlsx', index=False)