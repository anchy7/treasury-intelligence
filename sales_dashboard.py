"""
All Jobs Dashboard
Simple table view of ALL treasury jobs from all sources
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(
    page_title="All Treasury Jobs",
    page_icon="📋",
    layout="wide"
)

# Simple CSS
st.markdown("""
    <style>
    .main { padding: 0rem 1rem; }
    h1 { color: #1f77b4; }
    </style>
    """, unsafe_allow_html=True)

def _country_from_source_vectorized(df):
    """Vectorized: set Country from source (Jobs.ch → CH, StepStone → DE) else from location."""
    src = df["source"].fillna("").astype(str).str.strip().str.lower()
    from_source = np.where(
        src.str.contains("jobs.ch|jobs_ch", regex=True, na=False),
        "Switzerland",
        np.where(src.str.contains("stepstone", na=False), "Germany", None),
    )
    loc = df["location"].fillna("").astype(str).str.strip()
    de = loc.str.contains(
        "Munich|Frankfurt|Berlin|Hamburg|Stuttgart|Düsseldorf|Cologne|München|Germany|Deutschland",
        case=False,
        regex=True,
        na=False,
    )
    ch = loc.str.contains(
        "Zurich|Basel|Geneva|Zug|Lausanne|Switzerland|Schweiz",
        case=False,
        regex=True,
        na=False,
    )
    at = loc.str.contains("Vienna|Wien|Austria|Österreich", case=False, regex=True, na=False)
    from_loc = np.select([de, ch, at], ["Germany", "Switzerland", "Austria"], default="Unknown")
    mask = (from_source == "Switzerland") | (from_source == "Germany")
    return np.where(mask, from_source, from_loc)


@st.cache_data(ttl=3600)
def load_jobs():
    """Load all jobs from GitHub and add Country (vectorized, cached)."""
    base_url = "https://raw.githubusercontent.com/anchy7/treasury-intelligence/main/"
    try:
        df = pd.read_csv(base_url + "treasury_jobs.csv")
        df["date_scraped"] = pd.to_datetime(df["date_scraped"])
        df["Country"] = _country_from_source_vectorized(df)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


# Load data (cached; Country already set)
jobs_df = load_jobs()

if jobs_df.empty:
    st.warning("⏳ No data available yet. Run the scraper first!")
    st.stop()

# Sidebar filters
st.sidebar.markdown("### 🔍 Filters")

# Date range filter
date_options = ["All Time", "Last 7 days", "Last 30 days", "Last 90 days"]
date_filter = st.sidebar.selectbox("Date Range", date_options)

if date_filter != "All Time":
    days_map = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}
    days = days_map[date_filter]
    cutoff = datetime.now() - timedelta(days=days)
    jobs_filtered = jobs_df[jobs_df['date_scraped'] >= cutoff]
else:
    jobs_filtered = jobs_df

# Source filter
all_sources = ['All Sources'] + sorted(jobs_df['source'].unique().tolist())
source_filter = st.sidebar.selectbox("Job Source", all_sources)

if source_filter != 'All Sources':
    jobs_filtered = jobs_filtered[jobs_filtered['source'] == source_filter]

# Country filter
all_countries = ['All Countries'] + sorted(jobs_df['Country'].unique().tolist())
country_filter = st.sidebar.selectbox("Country", all_countries)

if country_filter != 'All Countries':
    jobs_filtered = jobs_filtered[jobs_filtered['Country'] == country_filter]

# Company search
st.sidebar.markdown("### 🏢 Company Search")
company_search = st.sidebar.text_input("Search company name", "")
if company_search:
    jobs_filtered = jobs_filtered[
        jobs_filtered['company'].str.contains(company_search, case=False, na=False)
    ]

# Technology filter
st.sidebar.markdown("### 💻 Technology")
tech_search = st.sidebar.text_input("Search technology", "")
if tech_search:
    jobs_filtered = jobs_filtered[
        jobs_filtered['technologies'].str.contains(tech_search, case=False, na=False)
    ]

# Stats sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Statistics")
st.sidebar.metric("Total Jobs", len(jobs_df))
st.sidebar.metric("Filtered Jobs", len(jobs_filtered))
st.sidebar.metric("Companies", jobs_filtered['company'].nunique())
st.sidebar.metric("Last Updated", jobs_df['date_scraped'].max().strftime('%Y-%m-%d'))

# Main content
st.title("📋 All Treasury Jobs - DACH Region")
st.markdown(f"*Complete list of all jobs from all sources • {date_filter}*")

# Key metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📊 Total Jobs", len(jobs_filtered))

with col2:
    st.metric("🏢 Companies", jobs_filtered['company'].nunique())

with col3:
    stepstone = len(jobs_filtered[jobs_filtered['source'].str.contains('StepStone', na=False)])
    st.metric("🇩🇪 StepStone", stepstone)

with col4:
    linkedin = len(jobs_filtered[jobs_filtered['source'].str.contains('LinkedIn', na=False)])
    st.metric("💼 LinkedIn", linkedin)

st.markdown("---")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📋 All Jobs Table", "📊 Statistics", "🔧 Technology Trends", "✉️ Outreach & Email Draft"])

with tab1:
    st.header("Complete Jobs List")
    
    st.markdown(f"**Showing {len(jobs_filtered)} jobs**")
    
    # Sort options
    col1, col2 = st.columns([1, 3])
    
    with col1:
        sort_by = st.selectbox(
            "Sort by",
            ["Newest First", "Oldest First", "Company A-Z", "Company Z-A"]
        )
    
    # Apply sorting
    if sort_by == "Newest First":
        display_df = jobs_filtered.sort_values('date_scraped', ascending=False)
    elif sort_by == "Oldest First":
        display_df = jobs_filtered.sort_values('date_scraped', ascending=True)
    elif sort_by == "Company A-Z":
        display_df = jobs_filtered.sort_values('company', ascending=True)
    else:  # Company Z-A
        display_df = jobs_filtered.sort_values('company', ascending=False)
    
    # Prepare display table
    table_df = display_df[['company', 'title', 'location', 'Country', 'source', 
                            'date_scraped', 'technologies']].copy()
    
    table_df['date_scraped'] = table_df['date_scraped'].dt.strftime('%Y-%m-%d')
    
    table_df.columns = [
        'Company', 'Job Title', 'Location', 'Country', 
        'Job Source', 'Posted Date', 'Technologies'
    ]
    
    # Display full table
    st.dataframe(
        table_df,
        use_container_width=True,
        height=600
    )
    
    # Export button
    csv = table_df.to_csv(index=False)
    st.download_button(
        "📥 Download Full Table as CSV",
        csv,
        f"treasury_jobs_full_{datetime.now().strftime('%Y%m%d')}.csv",
        "text/csv"
    )

with tab2:
    st.header("Job Market Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Jobs by Source")
        source_counts = jobs_filtered['source'].value_counts()
        
        fig = px.bar(
            x=source_counts.values,
            y=source_counts.index,
            orientation='h',
            labels={'x': 'Number of Jobs', 'y': 'Source'},
            color=source_counts.values,
            color_continuous_scale='Blues'
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed breakdown
        st.markdown("**Breakdown:**")
        for source, count in source_counts.items():
            pct = count / len(jobs_filtered) * 100
            st.markdown(f"• **{source}**: {count} jobs ({pct:.1f}%)")
    
    with col2:
        st.subheader("Jobs by Country")
        country_counts = jobs_filtered['Country'].value_counts()
        
        fig = px.pie(
            values=country_counts.values,
            names=country_counts.index,
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed breakdown
        st.markdown("**Breakdown:**")
        for country, count in country_counts.items():
            pct = count / len(jobs_filtered) * 100
            st.markdown(f"• **{country}**: {count} jobs ({pct:.1f}%)")
    
    st.markdown("---")
    
    # Top companies
    st.subheader("📊 Top Hiring Companies")
    
    company_counts = jobs_filtered['company'].value_counts().head(20)
    
    fig = px.bar(
        x=company_counts.values,
        y=company_counts.index,
        orientation='h',
        labels={'x': 'Number of Jobs', 'y': 'Company'},
        color=company_counts.values,
        color_continuous_scale='Viridis'
    )
    fig.update_layout(showlegend=False, height=600, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # Hiring over time
    st.subheader("📈 Jobs Posted Over Time")
    
    timeline = jobs_filtered.groupby(jobs_filtered['date_scraped'].dt.date).size().reset_index()
    timeline.columns = ['Date', 'Jobs Posted']
    
    fig = px.line(
        timeline,
        x='Date',
        y='Jobs Posted',
        markers=True
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header("Technology Trends")
    
    # Filter jobs with technologies
    jobs_with_tech = jobs_filtered[
        jobs_filtered["technologies"].notna() & (jobs_filtered["technologies"].astype(str).str.strip() != "")
    ]
    n_with_tech = len(jobs_with_tech)
    n_total = len(jobs_filtered)
    pct_tech = (n_with_tech / n_total * 100) if n_total else 0
    st.metric("Jobs with Technology Info", f"{n_with_tech} / {n_total} ({pct_tech:.1f}%)")

    if n_with_tech > 0:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Most Mentioned Technologies")
            tech_counts = jobs_with_tech["technologies"].value_counts().head(15)
            fig = px.bar(
                x=tech_counts.values,
                y=tech_counts.index,
                orientation="h",
                labels={"x": "Mentions", "y": "Technology"},
                color=tech_counts.values,
                color_continuous_scale="Blues",
            )
            fig.update_layout(showlegend=False, height=500)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Technology Distribution")
            # Vectorized: split and count technologies (no Python loop)
            tech_series = (
                jobs_with_tech["technologies"]
                .astype(str)
                .str.split(",")
                .explode()
                .str.strip()
            )
            tech_series = tech_series[tech_series != ""]
            top_tech = tech_series.value_counts().head(10)
            if len(top_tech) > 0:
                fig = px.pie(values=top_tech.values, names=top_tech.index)
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.caption("No technology tags to show.")
        
        # Technology by source
        st.subheader("💻 Technology Mentions by Source")
        
        tech_by_source = jobs_with_tech.groupby('source')['technologies'].apply(
            lambda x: x.str.split(',').explode().str.strip().value_counts().head(5)
        ).reset_index()
        
        for source in jobs_with_tech['source'].unique():
            with st.expander(f"{source} - Top Technologies"):
                source_jobs = jobs_with_tech[jobs_with_tech['source'] == source]
                if len(source_jobs) > 0:
                    source_techs = source_jobs['technologies'].str.split(',').explode().str.strip()
                    source_tech_counts = source_techs.value_counts().head(10)
                    
                    for tech, count in source_tech_counts.items():
                        st.markdown(f"• **{tech}**: {count} mentions")
    else:
        st.info("No technology information available in current data")

with tab4:
    st.header("✉️ Outreach & Email Draft")
    st.markdown("*Use this section to prepare outreach for companies from the jobs list.*")

    st.markdown("##### Recommended Actions")
    st.markdown("""
    **Immediate next steps:**
    1. Research company leadership (CFO, Treasurer)
    2. Identify warm connections (LinkedIn)
    3. Prepare tailored outreach email
    4. Book discovery call within 48 hours

    **Outreach approach:** Focus on their hiring need and offer a relevant case study or capability.
    """)

    st.markdown("##### Email draft template")
    company_placeholder = st.text_input("Company name (optional)", placeholder="e.g. Acme Corp")
    role_placeholder = st.text_input("Role / context (optional)", placeholder="e.g. Treasury Manager position")

    draft_subject = f"Treasury transformation support – {company_placeholder or '[Company]'}"
    draft_body = f"""Subject: {draft_subject}

Dear [Name],

I noticed {company_placeholder or '[Company]'} is strengthening its treasury team{f' around the {role_placeholder}' if role_placeholder else ''}. We support companies in the DACH region with treasury technology, cash visibility, and process improvement.

I would be happy to share a short case study or arrange a 15-minute call to explore whether we could add value.

Best regards,
[Your name]"""

    st.text_area("Draft (edit and copy)", value=draft_body, height=280, key="email_draft")
    st.caption("Edit the fields above and the draft will update. Copy the text to your email client.")

# Footer
st.markdown("---")
st.markdown("*📋 Complete Treasury Jobs Database | All Sources: StepStone, LinkedIn, Jobs.ch*")
