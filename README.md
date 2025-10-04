Este proyecto asume que ya tiene descargado el CSV directamente desde NASA POWER,
con PostgreSQL crea la tabla de lecturas.
Para cargar los datos a Postgres correr:
python cargar_csv_postgres.py
Para analizar los datos y obtener las gráficas:
python analisis_utf8.py
