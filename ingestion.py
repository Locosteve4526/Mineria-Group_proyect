import pandas as pd
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from pandas_gbq import to_gbq

# Cargar variables de entorno
load_dotenv()

# Ruta a la carpeta JSON
json_folder = Path(__file__).parent / "JSON"

# Listas para almacenar los datos
filenames = []
documents = []

# Iterar sobre todos los archivos .json en la carpeta
for json_file in json_folder.glob("*.json"):
    # Nombre del archivo (sin la ruta completa)
    filename = json_file.name
    
    # Leer el contenido del archivo JSON
    with open(json_file, 'r', encoding='utf-8') as f:
        document = json.load(f)
    
    # Agregar a las listas
    filenames.append(filename)
    # Convertir el documento a string JSON para guardarlo en BigQuery
    documents.append(json.dumps(document))

# Crear el DataFrame
df = pd.DataFrame({
    'activityid': filenames,
    'raw_json': documents
})

# Quitar la extensión .json de los nombres de archivo
df['activityid'] = df['activityid'].str.replace('.json', '', regex=False)

# Mostrar el DataFrame
print(df)
print(f"\nTotal de archivos procesados: {len(df)}")

# Enviar a BigQuery
project_id = 'mineria-de-datos-472620'
dataset_table = 'Json_DataBase.datos'

print(f"\nEnviando datos a BigQuery...")
to_gbq(dataframe=df,
       destination_table=dataset_table,
       project_id=project_id,
       if_exists='append')  # Opciones: 'fail', 'replace', 'append'

print(f"✓ Datos enviados exitosamente a {dataset_table}")
