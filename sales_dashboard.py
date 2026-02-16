"""
All Jobs Dashboard - V2
Green color scheme | Revenue data | Email drafts | No location column
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(
    page_title="All Treasury Jobs",
    page_icon="📋",
    layout="wide"
)

# Green color scheme CSS
st.markdown("""
    <style>
    .main { padding: 0rem 1rem; }
    h1 { color: #2d7a3e; }
    h2 { color: #2d7a3e; }
    h3 { color: #3a9d4f; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_jobs():
    """Load all jobs from GitHub"""
    base_url = "https://raw.githubusercontent.com/anchy7/treasury-intelligence/main/"
    
    try:
        df = pd.read_csv(base_url + "treasury_jobs.csv")
        df['date_scraped'] = pd.to_datetime(df['date_scraped'])
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def extract_country(location):
    """Extract country from location"""
    location = str(location).strip()
    
    if any(city in location for city in ['Munich', 'Frankfurt', 'Berlin', 'Hamburg', 
                                          'Stuttgart', 'Düsseldorf', 'Cologne', 'München']):
        return 'Germany'
    elif any(city in location for city in ['Zurich', 'Basel', 'Geneva', 'Zug', 'Lausanne']):
        return 'Switzerland'
    elif any(city in location for city in ['Vienna', 'Wien']):
        return 'Austria'
    elif 'Germany' in location or 'Deutschland' in location:
        return 'Germany'
    elif 'Switzerland' in location or 'Schweiz' in location:
        return 'Switzerland'
    elif 'Austria' in location or 'Österreich' in location:
        return 'Austria'
    else:
        return 'Unknown'

def estimate_revenue(company):
    """Estimate company revenue"""
    revenue_database = {
        'Siemens': '€75B+', 'Siemens AG': '€75B+',
        'BMW': '€140B+', 'BMW Group': '€140B+',
        'Mercedes-Benz': '€150B+', 'Mercedes-Benz Group': '€150B+',
        'Volkswagen': '€300B+', 'Volkswagen AG': '€300B+',
        'Deutsche Bank': '€30B+', 'Deutsche Bank AG': '€30B+',
        'Allianz': '€150B+', 'Allianz SE': '€150B+',
        'BASF': '€80B+', 'BASF SE': '€80B+',
        'SAP': '€30B+', 'SAP SE': '€30B+',
        'Bayer': '€50B+', 'Bayer AG': '€50B+',
        'Deutsche Telekom': '€100B+',
        'Munich Re': '€60B+', 'Münchener Rück': '€60B+',
        'Adidas': '€22B+', 'Adidas AG': '€22B+',
        'Lufthansa': '€35B+', 'Deutsche Lufthansa': '€35B+',
        'Porsche': '€40B+',
        'Continental': '€40B+',
        'Bosch': '€90B+', 'Robert Bosch': '€90B+',
        'Nestlé': '€100B+', 'Nestle': '€100B+',
        'Novartis': '€50B+', 'Novartis AG': '€50B+',
        'Roche': '€65B+', 'Roche Holding': '€65B+',
        'UBS': '€35B+', 'UBS Group': '€35B+',
        'Zurich Insurance': '€70B+',
        'Credit Suisse': '€25B+',
        'ABB': '€30B+', 'ABB Ltd': '€30B+',
        'Sika': '€10B+', 'Sika AG': '€10B+',
        'OMV': '€60B+', 'OMV AG': '€60B+',
        'Voestalpine': '€15B+',
        'Raiffeisen Bank': '€20B+',
        'Erste Group': '€20B+',
    }
    
    company_clean = str(company).strip()
    if company_clean in revenue_database:
        return revenue_database[company_clean]
    
    for known_company, revenue in revenue_database.items():
        if known_company.lower() in company_clean.lower():
            return revenue
    
    return 'Not Available'

def generate_cold_email(company, job_title, country):
    """Generate personalized cold acquisition email draft"""
    
    title_lower = job_title.lower()
    
    if 's/4hana' in title_lower or 's4hana' in title_lower:
        signal = 'SAP S/4HANA'
        project = 'SAP S/4HANA Treasury implementation'
        value_prop = "We've delivered 15+ S/4HANA Treasury implementations for companies like BMW, Siemens, and Volkswagen, accelerating timelines by 30% while reducing implementation risk."
    elif 'kyriba' in title_lower:
        signal = 'Kyriba TMS'
        project = 'Kyriba TMS implementation'
        value_prop = "We've led 20+ cloud TMS implementations with an average go-live of 6 months and consistent achievement of business case ROI from day one."
    elif 'api' in title_lower or 'integration' in title_lower:
        signal = 'API connectivity'
        project = 'real-time treasury connectivity'
        value_prop = "We specialize in API-first treasury architecture, helping companies achieve real-time visibility and automation across their treasury landscape."
    elif any(kw in title_lower for kw in ['transformation', 'change', 'program']):
        signal = 'Transformation'
        project = 'treasury transformation program'
        value_prop = "We've supported 25+ treasury transformations, delivering operating model design, system implementation, and change management as integrated services."
    elif any(kw in title_lower for kw in ['head', 'director', 'lead']):
        signal = 'Leadership'
        project = 'strategic treasury initiative'
        value_prop = "We provide interim leadership and strategic advisory services, supporting CFOs and Treasurers through critical transformation phases."
    else:
        signal = 'Treasury'
        project = 'treasury optimization'
        value_prop = "We offer end-to-end treasury consulting services, from strategy to implementation, helping companies modernize their treasury function."
    
    if country == 'Germany':
        location_ref = "across Germany and DACH region"
    elif country == 'Switzerland':
        location_ref = "across Switzerland and the broader DACH region"
    elif country == 'Austria':
        location_ref = "across Austria and the DACH region"
    else:
        location_ref = "in the DACH region"
    
    email = f"""Subject: Treasury expertise for {company}'s {signal} initiative

Dear Treasury Team,

I noticed {company} is hiring for a {job_title} position, which suggests an active {project}.

{value_prop}

We work with leading companies {location_ref} and would be delighted to discuss how we might support {company}'s treasury initiatives.

Would you be open to a brief 20-minute call to explore potential collaboration?

Key areas where we typically add value:
• Implementation acceleration and risk reduction
• Best practice process design
• Change management and stakeholder alignment
• Post-go-live optimization and support

I'd be happy to share relevant case studies and discuss your specific needs.

Best regards,
[Your Name]
[Your Company] | Treasury Consulting
[Your Contact Info]"""
    
    return email

# Load data
jobs_df = load_jobs()

if jobs_df.empty:
    st.warning("⏳ No data available yet. Run the scraper first!")
    st.stop()

# Add columns
jobs_df['Country'] = jobs_df['location'].apply(extract_country)
jobs_df['Revenue'] = jobs_df['company'].apply(estimate_revenue)
jobs_df['Email_Draft'] = jobs_df.apply(
    lambda row: generate_cold_email(row['company'], row['title'], row['Country']), 
    axis=1
)

# Sidebar filters
st.sidebar.markdown("### 🔍 Filters")

date_options = ["All Time", "Last 7 days", "Last 30 days", "Last 90 days"]
date_filter = st.sidebar.selectbox("Date Range", date_options)

if date_filter != "All Time":
    days_map = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}
    days = days_map[date_filter]
    cutoff = datetime.now() - timedelta(days=days)
    jobs_filtered = jobs_df[jobs_df['date_scraped'] >= cutoff]
else:
    jobs_filtered = jobs_df

all_sources = ['All Sources'] + sorted(jobs_df['source'].unique().tolist())
source_filter = st.sidebar.selectbox("Job Source", all_sources)

if source_filter != 'All Sources':
    jobs_filtered = jobs_filtered[jobs_filtered['source'] == source_filter]

all_countries = ['All Countries'] + sorted(jobs_df['Country'].unique().tolist())
country_filter = st.sidebar.selectbox("Country", all_countries)

if country_filter != 'All Countries':
    jobs_filtered = jobs_filtered[jobs_filtered['Country'] == country_filter]

company_search = st.sidebar.text_input("Search company name", "")
if company_search:
    jobs_filtered = jobs_filtered[
        jobs_filtered['company'].str.contains(company_search, case=False, na=False)
    ]

# Stats sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Statistics")
st.sidebar.metric("Total Jobs", len(jobs_df))
st.sidebar.metric("Filtered Jobs", len(jobs_filtered))
st.sidebar.metric("Companies", jobs_filtered['company'].nunique())

# Main content
st.title("📋 All Treasury Jobs - DACH Region")
st.markdown(f"*Complete list • Revenue data • Email drafts*")

# Key metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📊 Total Jobs", len(jobs_filtered))

with col2:
    st.metric("🏢 Companies", jobs_filtered['company'].nunique())

with col3:
    large_cap = len(jobs_filtered[jobs_filtered['Revenue'].str.contains('B\+', na=False)])
    st.metric("💰 Large Cap", large_cap)

with col4:
    germany = len(jobs_filtered[jobs_filtered['Country'] == 'Germany'])
    st.metric("🇩🇪 Germany", germany)

st.markdown("---")

# Tabs
tab1, tab2, tab3 = st.tabs(["📋 All Jobs Table", "📊 Statistics", "📧 Email Drafts"])

with tab1:
    st.header("Complete Jobs List")
    
    st.markdown(f"**Showing {len(jobs_filtered)} jobs**")
    
    sort_by = st.selectbox(
        "Sort by",
        ["Newest First", "Oldest First", "Company A-Z"]
    )
    
    if sort_by == "Newest First":
        display_df = jobs_filtered.sort_values('date_scraped', ascending=False)
    elif sort_by == "Oldest First":
        display_df = jobs_filtered.sort_values('date_scraped', ascending=True)
    else:
        display_df = jobs_filtered.sort_values('company', ascending=True)
    
    # Table without Location column
    table_df = display_df[[
        'company', 'title', 'Country', 'Revenue', 'source', 
        'date_scraped', 'technologies'
    ]].copy()
    
    table_df['date_scraped'] = table_df['date_scraped'].dt.strftime('%Y-%m-%d')
    
    table_df.columns = [
        'Company', 'Job Title', 'Country', 'Revenue', 
        'Job Source', 'Posted Date', 'Technologies'
    ]
    
    st.dataframe(table_df, use_container_width=True, height=600)
    
    csv = table_df.to_csv(index=False)
    st.download_button(
        "📥 Download Table as CSV",
        csv,
        f"treasury_jobs_{datetime.now().strftime('%Y%m%d')}.csv",
        "text/csv"
    )

with tab2:
    st.header("Job Market Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Jobs by Country")
        country_counts = jobs_filtered['Country'].value_counts()
        
        # Green color scale
        fig = px.pie(
            values=country_counts.values,
            names=country_counts.index,
            color_discrete_sequence=px.colors.sequential.Greens_r
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("**Breakdown:**")
        for country, count in country_counts.items():
            pct = count / len(jobs_filtered) * 100
            st.markdown(f"• **{country}**: {count} jobs ({pct:.1f}%)")
    
    with col2:
        st.subheader("Revenue Distribution")
        
        revenue_categories = []
        for rev in jobs_filtered['Revenue']:
            if 'Not Available' in str(rev):
                revenue_categories.append('Unknown')
            elif 'B+' in str(rev):
                try:
                    val = float(str(rev).replace('€', '').replace('B+', ''))
                    if val >= 50:
                        revenue_categories.append('€50B+')
                    elif val >= 10:
                        revenue_categories.append('€10-50B')
                    else:
                        revenue_categories.append('€1-10B')
                except:
                    revenue_categories.append('Unknown')
            else:
                revenue_categories.append('Unknown')
        
        rev_series = pd.Series(revenue_categories)
        rev_counts = rev_series.value_counts()
        
        fig = px.bar(
            x=rev_counts.values,
            y=rev_counts.index,
            orientation='h',
            labels={'x': 'Number of Jobs', 'y': 'Revenue Range'},
            color=rev_counts.values,
            color_continuous_scale='Greens'
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
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
        color_continuous_scale='Greens'
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
    fig.update_traces(line_color='#2d7a3e')
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header("Cold Email Drafts")
    
    st.markdown("""
    Select a job below to view the automatically generated cold acquisition email draft.
    Each email is personalized based on the job title, company, and country.
    """)
    
    # Company selector
    companies = sorted(jobs_filtered['company'].unique())
    selected_company = st.selectbox("Select Company", companies)
    
    if selected_company:
        company_jobs = jobs_filtered[jobs_filtered['company'] == selected_company]
        
        st.markdown(f"### {selected_company}")
        st.markdown(f"**Revenue:** {company_jobs.iloc[0]['Revenue']}")
        st.markdown(f"**Country:** {company_jobs.iloc[0]['Country']}")
        st.markdown(f"**Open Positions:** {len(company_jobs)}")
        
        st.markdown("---")
        
        # Show all jobs for this company
        for idx, job in company_jobs.iterrows():
            with st.expander(f"📋 {job['title']} (Posted: {job['date_scraped'].strftime('%Y-%m-%d')})"):
                st.markdown("##### Email Draft:")
                st.text_area(
                    "Copy this email",
                    job['Email_Draft'],
                    height=400,
                    key=f"email_{idx}"
                )
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"**Source:** {job['source']}")
                with col2:
                    st.markdown(f"**Technologies:** {job.get('technologies', 'None')}")
                with col3:
                    if job.get('url'):
                        st.markdown(f"[View Job Posting]({job['url']})")

# Footer
st.markdown("---")
st.markdown("*📋 Complete Treasury Jobs Database | Revenue Data | Cold Email Generator*")
