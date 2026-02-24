# Viz
import streamlit as st
import polars as pl

lf = pl.scan_parquet("out.parquet")

df = (
    lf.select("decimallatitude", "decimallongitude")
    .rename({"decimallatitude": "latitude", "decimallongitude": "longitude"})
    .collect()
)
st.title("Avesnap")
st.map(df)
st.write( df)
