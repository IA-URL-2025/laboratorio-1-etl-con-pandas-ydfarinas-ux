import pandas as pd
from pathlib import Path

def run_etl():
    input_path = Path("data/citas_clinica.csv")
    output_path = Path("data/output.csv")

    df = pd.read_csv(input_path)

    for col in ["paciente", "especialidad", "estado", "telefono"]:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    if "paciente" in df.columns:
        df["paciente"] = df["paciente"].str.title()

    if "especialidad" in df.columns:
        df["especialidad"] = df["especialidad"].str.upper()

    if "fecha_cita" in df.columns:
        fechas = pd.to_datetime(df["fecha_cita"], errors="coerce")
        df["fecha_cita"] = fechas.dt.date
        df = df[~fechas.isna()]

    if "estado" in df.columns:
        df = df[df["estado"] == "CONFIRMADA"]

    if "costo" in df.columns:
        df["costo"] = pd.to_numeric(df["costo"], errors="coerce")
        df = df[df["costo"] > 0]

    if "telefono" in df.columns:
        df["telefono"] = df["telefono"].fillna("NO REGISTRA")
        df.loc[
            df["telefono"].astype("string").str.strip().isin(["", "nan", "<na>"]),
            "telefono"
        ] = "NO REGISTRA"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    return df

if __name__ == "__main__":
    run_etl()
