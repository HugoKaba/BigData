import streamlit as st
import pandas as pd
import plotly.express as px
from io import BytesIO
import os
import sys

sys.path.append(os.path.join(os.getcwd(), "flows"))
from config import BUCKET_GOLD, get_minio_client

st.set_page_config(
    page_title="Steamlit BigData Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .main {
        padding: 2rem;
    }
    .stMetric {
        background-color: rgba(255, 255, 255, 0.05);
        padding: 20px;
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }
    [data-testid="stMetricValue"] {
        color: #0088ff !important;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_data_from_minio(object_name):
    """Fetch parquet file from gold bucket in MinIO."""
    try:
        client = get_minio_client()
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        return pd.read_parquet(BytesIO(data))
    except Exception as e:
        st.error(f"Error loading {object_name}: {e}")
        return pd.DataFrame()
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

def main():
    st.title("🚀 Dashboard Analytique Big Data")
    st.markdown("---")

    with st.spinner("Chargement des données depuis MinIO..."):
        df_ca_pays = load_data_from_minio("kpi_ca_pays.parquet")
        df_vol_mensuel = load_data_from_minio("kpi_vol_mensuel.parquet")
        df_ca_mensuel = load_data_from_minio("kpi_ca_mensuel_croissance.parquet")
        df_stats_clients = load_data_from_minio("kpi_stats_clients.parquet")
        df_top_produits = load_data_from_minio("kpi_top_produits.parquet")

    if df_ca_mensuel.empty:
        st.warning("⚠️ Aucune donnée disponible dans le bucket Gold. Assurez-vous que le pipeline a tourné.")
        return

    col1, col2, col3, col4 = st.columns(4)
    
    total_ca = df_ca_mensuel["montant"].sum()
    last_month_growth = df_ca_mensuel["croissance_mom"].iloc[-1] if not df_ca_mensuel.empty else 0
    avg_basket = df_stats_clients["panier_moyen"].mean() if not df_stats_clients.empty else 0
    total_customers = len(df_stats_clients)

    with col1:
        st.metric("Chiffre d'Affaires Total", f"{total_ca:,.0f} €", f"{last_month_growth:+.1f}% MoM")
    with col2:
        st.metric("Panier Moyen", f"{avg_basket:,.2f} €")
    with col3:
        st.metric("Nombre de Clients", f"{total_customers:,}")
    with col4:
        total_sales = df_vol_mensuel["volume_ventes"].sum()
        st.metric("Volume Total des Ventes", f"{total_sales:,}")

    st.markdown("---")

    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("📈 Évolution du Chiffre d'Affaires")
        fig_ca = px.area(df_ca_mensuel, x="date_achat", y="montant", 
                         labels={"montant": "CA (€)", "date_achat": "Date"},
                         template="plotly_white",
                         color_discrete_sequence=["#0088ff"])
        st.plotly_chart(fig_ca, use_container_width=True)

    with row1_col2:
        st.subheader("🌍 CA par Pays")
        fig_pays = px.pie(df_ca_pays, values="montant", names="pays",
                          hole=0.4,
                          template="plotly_white",
                          color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_pays, use_container_width=True)

    st.markdown("---")

    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        st.subheader("🥇 Top Produits par Revenue")
        fig_prod = px.bar(df_top_produits.sort_values("ca_total", ascending=False).head(10), 
                          x="ca_total", y="produit", 
                          orientation='h',
                          labels={"ca_total": "CA Total (€)", "produit": "Produit"},
                          template="plotly_white",
                          color="ca_total",
                          color_continuous_scale="Blues")
        st.plotly_chart(fig_prod, use_container_width=True)

    with row2_col2:
        st.subheader("👥 Distribution des Dépenses Clients")
        fig_dist = px.histogram(df_stats_clients, x="total_depense", 
                                nbins=30,
                                labels={"total_depense": "Dépense Totale (€)"},
                                template="plotly_white",
                                color_discrete_sequence=["#FFA15A"])
        st.plotly_chart(fig_dist, use_container_width=True)

    st.markdown("---")
    st.subheader("📋 Détails Clients (Top 50)")
    st.dataframe(df_stats_clients.sort_values("total_depense", ascending=False).head(50), 
                 use_container_width=True)

if __name__ == "__main__":
    main()
