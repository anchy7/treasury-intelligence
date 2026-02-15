import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(page_title="DACH Treasury Intelligence", page_icon="🏦", layout="wide")

st.markdown("""
    <style>
    .main { padding: 0rem 1rem; }
    h1 { color: #1f77b4; }
    .insight-box {
        background-color: #e8f4f8;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_data():
    url = "https://raw.githubusercontent.com/anchy7/treasury-intelligence/main/treasury_jobs.csv"
    
    try:
        df = pd.read_csv(url)
        df['date_scraped'] = pd.to_datetime(df['date_scraped'])
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("⏳ Waiting for data... Run the scraper first!")
    st.info("Go to GitHub → Actions → Daily Job Scraper → Run workflow")
    st.stop()

# Sidebar
st.sidebar.markdown("### 📊 Filters")
date_range = st.sidebar.selectbox("Time Period", ["Last 7 days", "Last 30 days", "Last 90 days", "All time"])

date_map = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90, "All time": 999}
days = date_map[date_range]
cutoff = datetime.now() - timedelta(days=days)
df_filtered = df[df['date_scraped'] >= cutoff]

st.sidebar.markdown("---")
st.sidebar.metric("Total Jobs", len(df))
st.sidebar.metric("Companies", df['company'].nunique())
st.sidebar.metric("Updated", df['date_scraped'].max().strftime("%Y-%m-%d"))

# Main
st.title("🏦 DACH Treasury Market Intelligence")
st.markdown(f"*Automated tracking • {date_range}*")

col1, col2, col3, col4 = st.columns(4)
col1.metric("📊 Jobs", len(df_filtered))
col2.metric("🏢 Companies", df_filtered['company'].nunique())
last_week = df_filtered[df_filtered['date_scraped'] >= datetime.now() - timedelta(days=7)]
col3.metric("📈 This Week", len(last_week))

# Source breakdown
linkedin_count = len(df_filtered[df_filtered['source'] == 'LinkedIn'])
stepstone_count = len(df_filtered[df_filtered['source'] == 'StepStone.de'])
jobsch_count = len(df_filtered[df_filtered['source'] == 'Jobs.ch'])
col4.metric("💼 LinkedIn", linkedin_count)

st.markdown("---")

# Source breakdown box
st.markdown(f"""
    <div class="insight-box">
    <strong>📊 Data Sources:</strong> 
    StepStone.de: {stepstone_count} jobs | 
    Jobs.ch: {jobsch_count} jobs | 
    LinkedIn (email alerts): {linkedin_count} jobs
    </div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["📊 Overview", "🏢 Companies", "📋 Jobs"])

with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Companies")
        company_counts = df_filtered['company'].value_counts().head(10)
        fig = px.bar(x=company_counts.values, y=company_counts.index, orientation='h',
                     color=company_counts.values, color_continuous_scale='Blues')
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Data Sources")
        source_counts = df_filtered['source'].value_counts()
        fig = px.pie(values=source_counts.values, names=source_counts.index)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("📍 Locations")
    loc_counts = df_filtered['location'].value_counts().head(10)
    fig = go.Figure([go.Bar(x=loc_counts.index, y=loc_counts.values, marker_color='lightblue')])
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header("Company Analysis")
    companies = sorted(df_filtered['company'].unique())
    
    if len(companies) > 0:
        company = st.selectbox("Select Company", companies)
        company_data = df_filtered[df_filtered['company'] == company]
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Jobs", len(company_data))
        c2.metric("Locations", company_data['location'].nunique())
        c3.metric("Sources", company_data['source'].nunique())
        
        st.subheader("Positions")
        display = company_data[['date_scraped', 'title', 'location', 'source', 'technologies']].copy()
        display['date_scraped'] = display['date_scraped'].dt.strftime('%Y-%m-%d')
        st.dataframe(display, use_container_width=True, hide_index=True)

with tab3:
    st.header("Recent Jobs")
    
    display = df_filtered.sort_values('date_scraped', ascending=False).head(100).copy()
    display['date_scraped'] = display['date_scraped'].dt.strftime('%Y-%m-%d')
    display = display[['date_scraped', 'company', 'title', 'location', 'source', 'technologies']]
    
    st.dataframe(display, use_container_width=True, hide_index=True, height=600)
    
    csv = df_filtered.to_csv(index=False)
    st.download_button("📥 Download CSV", csv, f"treasury_jobs_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

st.markdown("---")
st.markdown("*🏦 Treasury Intelligence | Automated daily via GitHub Actions + Email Parsing*")
