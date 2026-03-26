import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest

# 1. Setup Page Configuration
st.set_page_config(page_title="Virtual Library Intelligence", page_icon="📚", layout="wide")

# 2. Securely Connect to BigQuery via Streamlit Secrets
@st.cache_resource
def get_bq_client():
    # This pulls your GCP credentials directly from Streamlit's secure vault
    gcp_creds = dict(st.secrets["gcp_service_account"])
    credentials = service_account.Credentials.from_service_account_info(gcp_creds)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

@st.cache_data(ttl=3600) # Cache the data for 1 hour
def load_master_catalog():
    bq_client = get_bq_client()
    PROJECT_ID = bq_client.project
    
    # CHANGED: Replaced extracted_at with our new demand proxy metrics!
    query = f"""
        SELECT 
            title, author_name, publisher_name, category, 
            publish_date, current_price_vnd, rating_score, review_count, is_bestseller
        FROM `{PROJECT_ID}.book_scraping.v_library_master_catalog`
        ORDER BY is_bestseller DESC, review_count DESC NULLS LAST
        LIMIT 1000
    """
    
    try:
        return bq_client.query(query).to_dataframe()
    except BadRequest as e:
        st.error(f"🚨 BigQuery SQL Error: {e.message}")
        st.stop()
    except Exception as e:
        st.error(f"🚨 General Error: {e}")
        st.stop()

# 3. Build the UI
st.title("📚 Virtual Library Intelligence Hub")
st.markdown("Welcome to the automated market analytics dashboard.")

# Load the data
with st.spinner("Fetching latest market data from BigQuery..."):
    df_catalog = load_master_catalog()

# Display high-level metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Books Tracked", len(df_catalog))
col2.metric("Bestsellers Identified", int(df_catalog['is_bestseller'].sum()))
col3.metric("Average Market Rating", round(df_catalog['rating_score'].mean(), 1) if not df_catalog.empty else 0)

# Display the raw data table
st.subheader("Master Market Catalog")
st.dataframe(df_catalog, use_container_width=True)
