import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Big Data Dashboard", layout="wide")

st.title("📊 Big Data Analytics Dashboard")
st.markdown("Monitor your e-commerce KPIs in real-time.")

st.sidebar.header("Configuration")
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()

def fetch_data(endpoint):
    start_time = time.time()
    try:
        response = requests.get(f"{API_URL}/{endpoint}")
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        st.error(f"Error fetching {endpoint}: {e}")
        return [], 0
    end_time = time.time()
    return data, (end_time - start_time)

col1, col2 = st.columns(2)

with col1:
    st.subheader("🌍 CA par Pays")
    data_pays, latency = fetch_data("stats/ca_pays")
    if data_pays:
        df_pays = pd.DataFrame(data_pays)
        st.caption(f"Latency: {latency:.4f}s")
        fig = px.bar(df_pays.head(10), x='pays', y='montant', color='montant', 
                     title="Top 10 Pays par CA")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📈 Top Produits")
    data_prod, latency = fetch_data("stats/top_produits")
    if data_prod:
        df_prod = pd.DataFrame(data_prod)
        st.caption(f"Latency: {latency:.4f}s")
        fig = px.pie(df_prod, values='ca_total', names='produit', title="Répartition CA par Produit")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

col3, col4 = st.columns(2)

with col3:
    st.subheader("📆 Volume Mensuel des Ventes")
    data_vol, latency = fetch_data("stats/vol_mensuel")
    if data_vol:
        df_vol = pd.DataFrame(data_vol)
        st.caption(f"Latency: {latency:.4f}s")
        fig = px.line(df_vol, x='mois', y='volume_ventes', markers=True, title="Evolution Volume Ventes")
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("💰 Croissance Mensuelle CA")
    data_ca, latency = fetch_data("stats/ca_mensuel")
    if data_ca:
        df_ca = pd.DataFrame(data_ca)
        df_ca['croissance_mom'] = df_ca['croissance_mom'].fillna(0)
        st.caption(f"Latency: {latency:.4f}s")
        fig = px.bar(df_ca, x='mois', y='croissance_mom', 
                     color='croissance_mom', 
                     title="Croissance MoM (%)",
                     color_continuous_scale=px.colors.diverging.RdYlGn)
        st.plotly_chart(fig, use_container_width=True)

