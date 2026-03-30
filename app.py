import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest
import plotly.express as px # For interactive charts

# 1. Setup Page Configuration
st.set_page_config(page_title="Virtual Library Intelligence", page_icon="📚", layout="wide")

# 2. Connect to BigQuery
@st.cache_resource
def get_bq_client():
    gcp_creds = dict(st.secrets["gcp_service_account"])
    credentials = service_account.Credentials.from_service_account_info(gcp_creds)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

@st.cache_data(ttl=3600)
def load_master_catalog():
    bq_client = get_bq_client()
    PROJECT_ID = bq_client.project
    
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
        st.error(f" BigQuery SQL Error: {e.message}")
        st.stop()

@st.cache_data(ttl=3600)
def load_price_history(selected_titles):
    """Fetches historical scrapes from the raw database for the line chart."""
    if not selected_titles:
        return pd.DataFrame()
        
    bq_client = get_bq_client()
    PROJECT_ID = bq_client.project
    
    # Use standard SQL parameters instead of string hacking
    query = f"""
        SELECT title, current_price_vnd, extracted_at
        FROM `{PROJECT_ID}.book_scraping.library_database`
        WHERE title IN UNNEST(@selected_titles)
        ORDER BY extracted_at ASC
    """
    
    # Configure the query to safely pass the Python list directly into BigQuery
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("selected_titles", "STRING", selected_titles)
        ]
    )
    
    try:
        return bq_client.query(query, job_config=job_config).to_dataframe()
    except BadRequest as e:
        st.error(f"🚨 Line Chart SQL Error: {e.message}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_ai_insights():
    bq_client = get_bq_client()
    PROJECT_ID = bq_client.project
    
    query = f"""
        SELECT analyzed_at, trending_categories, business_recommendation
        FROM `{PROJECT_ID}.book_scraping.ai_market_insights`
        ORDER BY analyzed_at DESC
        LIMIT 1
    """
    try:
        return bq_client.query(query).to_dataframe()
    except Exception:
        # Fails silently and returns empty if the table doesn't exist yet
        return pd.DataFrame()

# ---------------------------------------------------------
# UI & DATA LOADING
# ---------------------------------------------------------
st.title("📚 Virtual Library Intelligence Hub")
st.markdown("Welcome to the automated market analytics dashboard.")

with st.spinner("Fetching latest market data from BigQuery..."):
    df_catalog = load_master_catalog()
    df_insights = load_ai_insights()

# ---------------------------------------------------------
# SIDEBAR FILTERS (Granular Search)
# ---------------------------------------------------------
st.sidebar.header("🎯 Filter Options")

# Category Filter
all_categories = df_catalog['category'].dropna().unique()
selected_categories = st.sidebar.multiselect("Category", options=all_categories)

# Author Filter
all_authors = df_catalog['author_name'].dropna().unique()
selected_authors = st.sidebar.multiselect("Author", options=all_authors)

# Publisher Filter
all_publishers = df_catalog['publisher_name'].dropna().unique()
selected_publishers = st.sidebar.multiselect("Publisher", options=all_publishers)

# Price Range Slider
min_price = int(df_catalog['current_price_vnd'].min()) if not df_catalog['current_price_vnd'].isnull().all() else 0
max_price = int(df_catalog['current_price_vnd'].max()) if not df_catalog['current_price_vnd'].isnull().all() else 1000000
selected_price_range = st.sidebar.slider("Price Range (VND)", min_value=min_price, max_value=max_price, value=(min_price, max_price), step=10000)

# Bestseller Toggle
only_bestsellers = st.sidebar.checkbox("⭐ Show Only Bestsellers")

# APPLY FILTERS
filtered_df = df_catalog.copy()
if selected_categories:
    filtered_df = filtered_df[filtered_df['category'].isin(selected_categories)]
if selected_authors:
    filtered_df = filtered_df[filtered_df['author_name'].isin(selected_authors)]
if selected_publishers:
    filtered_df = filtered_df[filtered_df['publisher_name'].isin(selected_publishers)]

# Apply Price and Bestseller Logic
filtered_df = filtered_df[
    (filtered_df['current_price_vnd'] >= selected_price_range[0]) & 
    (filtered_df['current_price_vnd'] <= selected_price_range[1])
]
if only_bestsellers:
    filtered_df = filtered_df[filtered_df['is_bestseller'] == True]

# ---------------------------------------------------------
# TOP METRICS (Dynamic based on filters)
# ---------------------------------------------------------
valid_ratings = filtered_df[filtered_df['rating_score'] > 0]['rating_score']
rating_display = f"{round(valid_ratings.max(), 1)} | {round(valid_ratings.median(), 1)} | {round(valid_ratings.min(), 1)}" if not valid_ratings.empty else "N/A"

col1, col2, col3 = st.columns(3)
col1.metric("Books in Current View", len(filtered_df))
col2.metric("Bestsellers in View", int(filtered_df['is_bestseller'].sum()))
col3.metric("Rating (Max | Med | Min)", rating_display)

st.divider()

# ---------------------------------------------------------
# AI STRATEGY REPORT SECTION
# ---------------------------------------------------------
st.subheader("🤖 Daily AI Acquisition Strategy")
if not df_insights.empty:
    latest_report = df_insights.iloc[0]
    report_date = pd.to_datetime(latest_report['analyzed_at']).strftime('%B %d, %Y - %H:%M UTC')
    
    st.caption(f"📅 Last Analyzed by Llama-3.3-70b-versatile: {report_date}")
    
    # Display Trends
    st.info(f"**Identified Micro-Trends:**\n\n{latest_report['trending_categories']}")
    
    # Display Recommendations cleanly
    rec_text = latest_report['business_recommendation']
    if "REASON:" in rec_text:
        buy_targets, reason = rec_text.split("REASON:")
        st.success(f"🎯 **Action:** {buy_targets.replace('BUY:', '').strip()}")
        st.write(f"**Strategic Justification:** {reason.strip()}")
    else:
        st.success(f"🎯 **Action:** {rec_text}")
else:
    st.warning("No AI insights generated yet. Ensure `trend_analyzer.py` has run successfully.")

st.divider()

# ---------------------------------------------------------
# VISUALIZATIONS
# ---------------------------------------------------------
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("📊 Market Proportion by Category")
    if not filtered_df.empty:
        # Group by category and count
        category_counts = filtered_df['category'].value_counts().reset_index()
        category_counts.columns = ['Category', 'Book Count']
        
        # Build Plotly Pie Chart
        fig_pie = px.pie(category_counts, names='Category', values='Book Count', hole=0.4)
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No data available for the selected filters.")

with col_chart2:
    st.subheader("📈 Historical Price Tracker")
    st.markdown("Select books below to see their price changes over time.")
    
    # User selects which books to graph (Max 5 for now)
    books_to_graph = st.multiselect(
        "Select up to 5 books to compare:", 
        options=filtered_df['title'].dropna().unique(),
        max_selections=5
    )
    
    if books_to_graph:
        with st.spinner("Fetching historical data..."):
            history_df = load_price_history(books_to_graph)
            
            if not history_df.empty:
                # Convert extracted_at to datetime for proper timeline graphing
                history_df['extracted_at'] = pd.to_datetime(history_df['extracted_at'])
                
                # Build Plotly Line Chart
                fig_line = px.line(
                    history_df, x='extracted_at', y='current_price_vnd', color='title',
                    markers=True, labels={"extracted_at": "Extraction Date", "current_price_vnd": "Price (VND)"}
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.warning("No historical data found for these titles yet.")
    else:
        st.info("👈 Select a book from the dropdown to generate the timeline.")

# ---------------------------------------------------------
# RAW DATA TABLE
# ---------------------------------------------------------
st.subheader("📋 Filtered Catalog Data")
st.dataframe(filtered_df, use_container_width=True)
