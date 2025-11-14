import os
import json
import pandas as pd

folder_path = r"C:\Users\locos\Documents\Trabajos_U\VSC\Mineria_Datos\Trabajo_en_grupo_clase_14_11\JSON"

rows = []

for filename in os.listdir(folder_path):
    if filename.endswith(".json"):
        file_path = os.path.join(folder_path, filename)

        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)

        row = {}

        # 1. activityid
        row["activityid"] = content.get("activityid")

        # 2. fields (aplanado)
        fields = content.get("fields", {})
        for key, value in fields.items():
            row[key] = value

        # 3. Metadatos adicionales
        row["facultyid"] = content.get("facultyid")
        row["userid"] = content.get("userid")

        # 4. status (lo guardamos como JSON string o lista)
        row["status"] = json.dumps(content.get("status"))

        # 5. attachments
        row["attachments"] = json.dumps(content.get("attachments"))

        # 6. Ignorar coauthors (solo guardamos facultyid que ya está fuera)
        # no incluimos nada de coauthors

        rows.append(row)

# Crear DataFrame flexible
df2 = pd.DataFrame(rows)

print("Columnas detectadas:", len(df2.columns))
print(df2.head())