import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# Cargar .env desde el cwd
load_dotenv()

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "") #Ingresar contraseña real
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")  # usar IPv4 por defecto
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "clima_agro")

url = URL.create(
    drivername="postgresql+psycopg2",
    username=PG_USER,
    password=PG_PASS,
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DB,
)

# Forzar UTF-8 y tiempo de espera de conexión
engine = create_engine(
    url,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 10, "options": "-c client_encoding=UTF8"},
)

# 1) Leer datos
df = pd.read_sql("SELECT fecha, t2m, rh2m FROM public.lecturas ORDER BY fecha", con=engine)

# 2) Exploración
print(df.head())
# Asegura tipos numéricos (por si vienen como texto)
df["t2m"] = pd.to_numeric(df["t2m"], errors="coerce")
df["rh2m"] = pd.to_numeric(df["rh2m"], errors="coerce")

# Describe compatible con pandas antiguos
print(df[["t2m","rh2m"]].describe())

# 3) Gráfico líneas
plt.figure()
plt.plot(df['fecha'], df['t2m'], label='T2M (°C)')
plt.plot(df['fecha'], df['rh2m'], label='RH2M (%)')
plt.title('Guayas - T2M y RH2M (Jun 2024)')
plt.xlabel('Fecha'); plt.ylabel('Valor')
plt.legend(); plt.grid(True); plt.tight_layout()
plt.show()

# 4) Top humedad
top_n = 5
top_h = df.nlargest(top_n, 'rh2m')[['fecha','rh2m']]
print("\nTop días con mayor humedad:")
print(top_h.to_string(index=False))

# 5) Correlación
corr = df[['t2m','rh2m']].corr(method='pearson').loc['t2m','rh2m']
print(f"\nCorrelación (Pearson) T2M vs RH2M: {corr:.3f}")
if corr <= -0.5:
    print("Interpretación: correlación negativa marcada (a mayor T, menor RH).")
elif corr >= 0.5:
    print("Interpretación: correlación positiva marcada (suben juntas).")
else:
    print("Interpretación: relación débil o poco marcada.")

# 6) Scatter opcional
plt.figure()
plt.scatter(df['t2m'], df['rh2m'])
plt.title('Dispersión: T2M vs RH2M')
plt.xlabel('T2M (°C)'); plt.ylabel('RH2M (%)')
plt.grid(True); plt.tight_layout()
plt.show()
