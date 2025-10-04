import os, io, pandas as pd, re
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Float
from dotenv import load_dotenv

CSV_PATH = "datos_guayas.csv"

def read_power_csv(csv_path: str) -> pd.DataFrame:
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    header_idx = None
    for i, ln in enumerate(lines):
        l = ln.strip().upper()
        if l.startswith(("DATE,","YEAR,","YYYYMMDD,","LOCAL_DATE,")):
            header_idx = i
            break
    if header_idx is None:
        header_idx = next((i for i, ln in enumerate(lines) if ln.strip().upper().startswith("YEAR,")), None)
    if header_idx is None:
        raise RuntimeError("No se encontró encabezado de datos.")

    raw = "".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(raw))
    colsU = {c.upper(): c for c in df.columns}

    # fecha
    if "DATE" in colsU:
        sample = str(df[colsU["DATE"]].iloc[0])
        if re.fullmatch(r"\d{8}", sample):
            df["fecha"] = pd.to_datetime(df[colsU["DATE"]].astype(str), format="%Y%m%d", errors="coerce")
        else:
            df["fecha"] = pd.to_datetime(df[colsU["DATE"]], errors="coerce")
    elif "YYYYMMDD" in colsU:
        df["fecha"] = pd.to_datetime(df[colsU["YYYYMMDD"]].astype(str), format="%Y%m%d", errors="coerce")
    elif "YEAR" in colsU and "DOY" in colsU:
        y = pd.to_numeric(df[colsU["YEAR"]], errors="coerce").astype("Int64")
        d = pd.to_numeric(df[colsU["DOY"]], errors="coerce").astype("Int64")
        base = pd.to_datetime(y.astype(str) + "-01-01", errors="coerce")
        df["fecha"] = base + pd.to_timedelta((d - 1).astype("float"), unit="D")
    elif "YEAR" in colsU:
        mo_key = "MO" if "MO" in colsU else ("MONTH" if "MONTH" in colsU else None)
        dy_key = "DY" if "DY" in colsU else ("DAY" if "DAY" in colsU else None)
        if mo_key and dy_key:
            df["fecha"] = pd.to_datetime(dict(year=df[colsU["YEAR"]], month=df[colsU[mo_key]], day=df[colsU[dy_key]]), errors="coerce")
        else:
            raise RuntimeError("No se pudo construir la fecha (hay YEAR pero faltan DOY o MO/DY).")
    else:
        raise RuntimeError("No se pudo construir 'fecha'.")

    # vars
    t2m_key = colsU.get("T2M"); rh2m_key = colsU.get("RH2M")
    if not t2m_key or not rh2m_key:
        raise RuntimeError(f"Faltan T2M/RH2M. Columnas: {list(df.columns)}")

    out = df[["fecha", t2m_key, rh2m_key]].rename(columns={t2m_key:"t2m", rh2m_key:"rh2m"})
    out["t2m"]  = pd.to_numeric(out["t2m"], errors="coerce").round(2)
    out["rh2m"] = pd.to_numeric(out["rh2m"], errors="coerce").round(2)
    out = out.dropna(subset=["fecha"]).drop_duplicates(subset=["fecha"]).sort_values("fecha").reset_index(drop=True)
    return out

def main():
    load_dotenv()
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_DB   = os.getenv("PG_DB", "clima_agro")
    PG_USER = os.getenv("PG_USER", "postgres")
    PG_PASS = os.getenv("PG_PASS", "") #Introducir contraseña real

    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

    df = read_power_csv(CSV_PATH)
    df.to_sql(
        "lecturas",
        engine,
        if_exists="append",
        index=False,
        dtype={"fecha": Date(), "t2m": Float(), "rh2m": Float()}
    )
    print(f"Cargadas {len(df)} filas en public.lecturas")

if __name__ == "__main__":
    main()
