# pages/01_PyPSA_DataPrep.py

import os
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="PyPSA DataPrep", layout="wide")

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

st.title("PyPSA DataPrep (Türkiye Tek-Node)")

# ------------------------------------------------
# Helpers
# ------------------------------------------------
def _to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    dt = pd.to_datetime(df["Tarih"], errors="coerce")

    if dt.isna().mean() > 0.2 and "Saat" in df.columns:
        date_part = pd.to_datetime(df["Tarih"], errors="coerce").dt.strftime("%Y-%m-%d")
        s = df["Saat"].astype(str).str.strip()
        s2 = s.where(s.str.contains(":"), s.str.zfill(2) + ":00")
        dt = pd.to_datetime(date_part + " " + s2, errors="coerce")

    df["timestamp"] = dt
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df.set_index("timestamp")

def drop_feb29(df):
    return df.loc[~((df.index.month == 2) & (df.index.day == 29))]

def normalize_shape_max(s):
    s = pd.to_numeric(s, errors="coerce").fillna(0).clip(lower=0)
    mx = float(s.max()) if len(s) else 0
    if mx <= 0:
        return s * 0
    return (s / mx).clip(0, 1)

# ------------------------------------------------
# UI
# ------------------------------------------------
cons_file = st.file_uploader("Tüketim Excel", type=["xlsx", "xls"])
lic_file = st.file_uploader("Lisanslı Üretim Excel", type=["xlsx", "xls"])
unlic_file = st.file_uploader("Lisanssız Üretim Excel", type=["xlsx", "xls"])

base_year = st.selectbox("Baz yıl", [2021, 2022, 2023, 2024, 2025], index=3)

produce_all_years = st.checkbox(
    "Tüm yıllar için profiles_YYYY.parquet üret",
    value=False
)

run_btn = st.button("Hazırla", type="primary")

# ------------------------------------------------
# MAIN
# ------------------------------------------------
if run_btn:

    if not cons_file or not lic_file or not unlic_file:
        st.error("3 dosyayı da yüklemelisin.")
        st.stop()

    df_cons = pd.read_excel(cons_file)
    df_lic = pd.read_excel(lic_file)
    df_unlic = pd.read_excel(unlic_file)

    cons = drop_feb29(_to_datetime(df_cons))
    lic = drop_feb29(_to_datetime(df_lic))
    unlic = drop_feb29(_to_datetime(df_unlic))

    idx = cons.index.intersection(lic.index).intersection(unlic.index)

    out = pd.DataFrame(index=idx)
    cons_col = [c for c in cons.columns if "tüketim" in c.lower()][0]

    out["consumption_mwh"] = pd.to_numeric(cons.loc[idx, cons_col], errors="coerce").fillna(0)
    out["solar_total_mwh"] = pd.to_numeric(lic.get("Güneş", 0), errors="coerce").fillna(0)
    out["wind_total_mwh"] = pd.to_numeric(lic.get("Rüzgar", 0), errors="coerce").fillna(0)
    out["hydro_res_mwh"] = pd.to_numeric(lic.get("Barajlı", 0), errors="coerce").fillna(0)
    out["hydro_ror_mwh"] = pd.to_numeric(lic.get("Akarsu", 0), errors="coerce").fillna(0)

    out["hydro_mwh"] = out["hydro_res_mwh"] + out["hydro_ror_mwh"]
    out["net_load_mwh"] = (out["consumption_mwh"] - out["solar_total_mwh"]).clip(lower=0)

    # ------------------------------------------------
    # SAVE
    # ------------------------------------------------
    history_path = OUT_DIR / "history_hourly.parquet"
    out.reset_index().to_parquet(history_path, index=False)

    written_profiles = []
    years_available = sorted(out.index.year.unique())

    years_to_build = years_available if produce_all_years else [base_year]

    for yr in years_to_build:
        df_y = out[out.index.year == yr].copy()

        if len(df_y) != 8760:
            st.warning(f"{yr} yılı 8760 değil ({len(df_y)}) → atlandı.")
            continue

        prof = pd.DataFrame(index=df_y.index)
        prof["load_base"] = df_y["consumption_mwh"]
        prof["net_load_base"] = df_y["net_load_mwh"]
        prof["solar_shape"] = normalize_shape_max(df_y["solar_total_mwh"])
        prof["wind_shape"] = normalize_shape_max(df_y["wind_total_mwh"])
        prof["hydro_res_shape"] = normalize_shape_max(df_y["hydro_res_mwh"])
        prof["hydro_ror_shape"] = normalize_shape_max(df_y["hydro_ror_mwh"])
        prof["hydro_shape"] = normalize_shape_max(df_y["hydro_mwh"])

        pth = OUT_DIR / f"profiles_{yr}.parquet"
        prof.reset_index().to_parquet(pth, index=False)
        written_profiles.append(pth)

    st.success("Parquet üretildi.")
    st.write("✅", history_path)

    for p in written_profiles:
        st.write("✅", p)

    st.subheader("Kontrol (Baz Yıl)")
    df_check = out[out.index.year == base_year]
    st.metric("Saat", len(df_check))
    st.dataframe(df_check.head(24))
