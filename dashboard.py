import streamlit as st
import pandas as pd
import plotly.express as px
from io import BytesIO
import os
import sys

sys.path.append(os.path.join(os.getcwd(), "flows"))
from config import BUCKET_GOLD, get_minio_client

st.set_page_config(
    page_title="BigData Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
    }
    .main {
        padding: 1rem;
    }
    .stMetric {
        background-color: #262730;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #41424b;
        box-shadow: 0 2px 6px rgba(0,0,0,0.2);
    }
    [data-testid="stMetricValue"] {
        color: #64b5f6 !important;
        font-weight: 600;
        font-size: 28px !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 14px !important;
        color: #b0bec5 !important;
    }
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
        color: #eceff1;
    }
    .stDataFrame {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_data_from_minio(object_name):
    try:
        client = get_minio_client()
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        return pd.read_parquet(BytesIO(data))
    except Exception:
        return pd.DataFrame()
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

def main():
    st.title("🚀 Big Data Analytics & Prediction")
    
    with st.spinner("Chargement..."):
        df_ca_pays = load_data_from_minio("kpi_ca_pays.parquet")
        df_vol_mensuel = load_data_from_minio("kpi_vol_mensuel.parquet")
        df_ca_mensuel = load_data_from_minio("kpi_ca_mensuel_croissance.parquet")
        df_stats_clients = load_data_from_minio("kpi_stats_clients.parquet")
        df_top_produits = load_data_from_minio("kpi_top_produits.parquet")
        df_churn_pred = load_data_from_minio("ml_churn_predictions.parquet")

    if df_ca_mensuel.empty:
        st.error("Données indisponibles.")
        return

    tab_overview, tab_clients, tab_ml = st.tabs(["📊 Vue d'Ensemble", "👥 Clients & Produits", "🔮 Prédictions IA"])

    with tab_overview:
        col1, col2, col3, col4 = st.columns(4)
        
        total_ca = df_ca_mensuel["montant"].sum()
        avg_basket = df_stats_clients["panier_moyen"].mean() if not df_stats_clients.empty else 0
        total_customers = len(df_stats_clients)
        total_sales = df_vol_mensuel["volume_ventes"].sum()
        
        col1.metric("Chiffre d'Affaires", f"{total_ca:,.0f} €")
        col2.metric("Panier Moyen", f"{avg_basket:,.2f} €")
        col3.metric("Clients Actifs", f"{total_customers:,}")
        col4.metric("Volume Ventes", f"{total_sales:,}")

        st.markdown("### 📈 Performance Temporelle")
        row1_col1, row1_col2 = st.columns([2, 1])

        with row1_col1:
            fig_ca = px.area(df_ca_mensuel, x="date_achat", y="montant", 
                             template="plotly_dark",
                             color_discrete_sequence=["#64b5f6"])
            fig_ca.update_layout(xaxis_title=None, yaxis_title=None, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_ca, width="stretch")

        with row1_col2:
            fig_pays = px.pie(df_ca_pays, values="montant", names="pays",
                              hole=0.6,
                              template="plotly_dark",
                              color_discrete_sequence=px.colors.qualitative.Prism)
            fig_pays.update_layout(showlegend=False, margin=dict(l=0, r=0, t=0, b=0))
            fig_pays.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_pays, width="stretch")

    with tab_clients:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🥇 Top Produits")
            fig_prod = px.bar(df_top_produits.sort_values("ca_total", ascending=False).head(10), 
                              x="ca_total", y="produit", 
                              orientation='h',
                              template="plotly_dark",
                              color="ca_total",
                              color_continuous_scale="Teal")
            fig_prod.update_layout(yaxis={'categoryorder':'total ascending'}, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_prod, width="stretch")
            
        with c2:
            st.subheader("💰 Répartition Dépenses")
            fig_dist = px.histogram(df_stats_clients, x="total_depense", 
                                    nbins=30,
                                    template="plotly_dark",
                                    color_discrete_sequence=["#ffca28"])
            fig_dist.update_layout(bargap=0.1, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_dist, width="stretch")

    with tab_ml:
        if not df_churn_pred.empty:
            st.markdown("### 🚨 Analyse des Risques (Churn)")
            
            c_ml1, c_ml2, c_ml3 = st.columns(3)
            
            n_high = len(df_churn_pred[df_churn_pred["risk_segment"] == "High Risk"])
            n_med = len(df_churn_pred[df_churn_pred["risk_segment"] == "Medium Risk"])
            n_low = len(df_churn_pred[df_churn_pred["risk_segment"] == "Low Risk"])
            
            c_ml1.metric("Risque Élevé", f"{n_high}", delta="Priorité Absolue", delta_color="inverse")
            c_ml2.metric("Risque Moyen", f"{n_med}")
            c_ml3.metric("Risque Faible", f"{n_low}")

            row_ml1, row_ml2 = st.columns([1, 2])
            
            with row_ml1:
                fig_risk = px.pie(df_churn_pred, names="risk_segment", 
                                  color="risk_segment",
                                  color_discrete_map={"High Risk": "#ff5252", "Medium Risk": "#ffa726", "Low Risk": "#66bb6a"},
                                  template="plotly_dark", hole=0.7)
                fig_risk.update_layout(showlegend=False, margin=dict(l=20, r=20, t=20, b=20))
                fig_risk.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_risk, width="stretch")
                
            with row_ml2:
                fig_scatter = px.scatter(df_churn_pred, x="avg_basket", y="churn_probability",
                                         color="risk_segment",
                                         color_discrete_map={"High Risk": "#ff5252", "Medium Risk": "#ffa726", "Low Risk": "#66bb6a"},
                                         title="Probabilité de départ vs Panier Moyen",
                                         template="plotly_dark")
                fig_scatter.update_layout(margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig_scatter, width="stretch")

            st.markdown("#### 📋 Liste des Clients à Risque Élevé")
            
            df_high_risk = df_churn_pred[df_churn_pred["risk_segment"] == "High Risk"].copy()
            df_high_risk["churn_probability"] = df_high_risk["churn_probability"] * 100
            
            st.dataframe(
                df_high_risk.sort_values("churn_probability", ascending=False)
                .head(50)[["id_client", "churn_probability", "frequency", "avg_basket", "pays"]],
                column_config={
                    "id_client": st.column_config.NumberColumn("ID Client", format="%d"),
                    "churn_probability": st.column_config.ProgressColumn(
                        "Probabilité de Départ",
                        format="%.1f%%",
                        min_value=0,
                        max_value=100,
                    ),
                    "frequency": st.column_config.NumberColumn("Achats", format="%d"),
                    "avg_basket": st.column_config.NumberColumn("Panier Moyen", format="%.2f €"),
                    "pays": "Pays",
                },
                width="stretch",
                hide_index=True
            )
        else:
            st.info("Aucune prédiction disponible.")

if __name__ == "__main__":
    main()
