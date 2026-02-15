"""
Sales Intelligence Dashboard
Displays consulting prospects and transformation signals
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Treasury Sales Intelligence",
    page_icon="🎯",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .main { padding: 0rem 1rem; }
    h1 { color: #1f77b4; }
    .hot-lead {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .warm-lead {
        background: linear-gradient(135deg, #ffa502 0%, #ff7f00 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .signal-badge {
        background-color: #e8f4f8;
        padding: 5px 10px;
        border-radius: 5px;
        margin: 2px;
        display: inline-block;
        font-size: 0.9em;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_data():
    """Load prospects and jobs data"""
    # REPLACE with your GitHub username
    base_url = "https://raw.githubusercontent.com/anchy7/treasury-intelligence/main/"
    
    try:
        prospects = pd.read_csv(base_url + "prospects.csv")
        jobs = pd.read_csv(base_url + "treasury_jobs.csv")
        jobs['date_scraped'] = pd.to_datetime(jobs['date_scraped'])
        prospects['first_seen'] = pd.to_datetime(prospects['first_seen'])
        prospects['last_activity'] = pd.to_datetime(prospects['last_activity'])
        
        # Handle all_signals column - split into list if present
        if 'all_signals' in prospects.columns:
            prospects['signals_list'] = prospects['all_signals'].fillna('').apply(
                lambda x: [s.strip() for s in x.split('|') if s.strip()]
            )
        else:
            prospects['signals_list'] = prospects['primary_signal'].apply(lambda x: [x] if x != 'None' else [])
        
        return prospects, jobs
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame()

prospects_df, jobs_df = load_data()

if prospects_df.empty or jobs_df.empty:
    st.warning("⏳ Waiting for data... Run lead scoring first!")
    st.info("Go to GitHub → Actions → Daily Job Scraper → Run workflow")
    st.stop()

# Sidebar
st.sidebar.markdown("### 🎯 Sales Intelligence")

# Tier filter
all_tiers = ['All Tiers'] + sorted(prospects_df['tier'].unique().tolist())
selected_tier = st.sidebar.selectbox("Filter by Tier", all_tiers)

if selected_tier != 'All Tiers':
    prospects_filtered = prospects_df[prospects_df['tier'] == selected_tier]
else:
    prospects_filtered = prospects_df

# Stats
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Pipeline Stats")

tier1_count = len(prospects_df[prospects_df['tier'].str.contains('Tier 1')])
tier2_count = len(prospects_df[prospects_df['tier'].str.contains('Tier 2')])
tier3_count = len(prospects_df[prospects_df['tier'].str.contains('Tier 3')])

st.sidebar.metric("🔥 Hot Leads (Tier 1)", tier1_count)
st.sidebar.metric("🌡️ Warm Leads (Tier 2)", tier2_count)
st.sidebar.metric("📋 Qualified (Tier 3)", tier3_count)

# Total signals detected
total_signals = prospects_df['signal_count'].sum()
st.sidebar.metric("🎯 Total Signals", int(total_signals))

st.sidebar.markdown("---")
st.sidebar.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")

# Main content
st.title("🎯 Treasury Consulting Sales Intelligence")
st.markdown("*Automated prospect identification from job market data*")

# Top metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("🏢 Total Prospects", len(prospects_df))

with col2:
    hot_prospects = prospects_df[prospects_df['score'] >= 80]
    st.metric("🔥 Hot Prospects", len(hot_prospects), delta=f"{len(hot_prospects)}")

with col3:
    active_last_7 = prospects_df[prospects_df['last_activity'] >= datetime.now() - timedelta(days=7)]
    st.metric("📈 Active This Week", len(active_last_7))

with col4:
    avg_score = prospects_df['score'].mean()
    st.metric("⭐ Avg Score", f"{avg_score:.0f}/100")

st.markdown("---")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["🔥 Hot Prospects", "📊 All Prospects", "🏢 Company Profiles", "📈 Market Trends"])

with tab1:
    st.header("Hot Prospects - Immediate Action Required")
    
    hot_leads = prospects_df[prospects_df['score'] >= 80].sort_values('score', ascending=False)
    
    if len(hot_leads) == 0:
        st.info("No Tier 1 hot leads at the moment. Check Tier 2 warm leads.")
    else:
        for idx, prospect in hot_leads.iterrows():
            # Display signals badges
            signals_html = ""
            if prospect['signal_count'] > 0 and 'signals_list' in prospect:
                for signal in prospect['signals_list'][:5]:  # Show max 5
                    signals_html += f'<span class="signal-badge">{signal}</span> '
            
            st.markdown(f"""
                <div class="hot-lead">
                <h3>🔥 {prospect['company']}</h3>
                <p><strong>Transformation Score: {prospect['score']}/100</strong> | {prospect['action']}</p>
                <p>📊 Activity: <strong>{prospect['jobs_last_30_days']} jobs</strong> posted in last 30 days (Total: {prospect['total_jobs']})</p>
                <p>🎯 Transformation Signals: <strong>{prospect['signal_count']}</strong> detected</p>
                <p>{signals_html}</p>
                <p>📍 Locations: <strong>{prospect['locations']}</strong> sites</p>
                <p>⏰ Last Activity: <strong>{prospect['last_activity'].strftime('%Y-%m-%d')}</strong></p>
                </div>
            """, unsafe_allow_html=True)
            
            # Show company details
            with st.expander(f"View detailed intelligence for {prospect['company']}"):
                company_jobs = jobs_df[jobs_df['company'] == prospect['company']]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("##### Recent Job Postings")
                    recent = company_jobs.sort_values('date_scraped', ascending=False).head(10)
                    for _, job in recent.iterrows():
                        st.markdown(f"**{job['title']}**")
                        st.markdown(f"📍 {job['location']} | 📅 {job['date_scraped'].strftime('%Y-%m-%d')}")
                        if job['technologies']:
                            st.markdown(f"🔧 {job['technologies']}")
                        st.markdown("---")
                
                with col2:
                    st.markdown("##### Transformation Signals Detected")
                    
                    # Display all signals with details
                    if 'signals_list' in prospect and len(prospect['signals_list']) > 0:
                        for i, signal in enumerate(prospect['signals_list'], 1):
                            st.markdown(f"**{i}. {signal}**")
                            st.markdown("")
                    else:
                        st.info(f"Primary Signal: {prospect['primary_signal']}")
                    
                    st.markdown("---")
                    
                    st.markdown("##### Recommended Actions")
                    st.markdown("""
                    **Immediate Next Steps:**
                    1. Research company leadership (CFO, Treasurer)
                    2. Identify warm connections (LinkedIn)
                    3. Prepare tailored outreach email
                    4. Book discovery call within 48 hours
                    
                    **Outreach Approach**: 
                    Focus on their primary signal and offer relevant case study
                    """)
                    
                    st.markdown("##### Quick Stats")
                    st.metric("Total Jobs", len(company_jobs))
                    st.metric("Hiring Velocity", f"+{prospect['jobs_last_30_days']} in 30 days")
                    st.metric("Geographic Spread", f"{prospect['locations']} locations")
                    st.metric("Transformation Signals", prospect['signal_count'])

with tab2:
    st.header("All Prospects")
    
    # Score distribution
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Prospect Score Distribution")
        fig = px.histogram(
            prospects_filtered,
            x='score',
            nbins=20,
            color_discrete_sequence=['#1f77b4']
        )
        fig.update_layout(
            xaxis_title="Transformation Score",
            yaxis_title="Number of Companies",
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Prospects by Tier")
        tier_counts = prospects_filtered['tier'].value_counts()
        
        # Custom colors for tiers
        colors = {
            'Tier 1: Hot': '#ff6b6b',
            'Tier 2: Warm': '#ffa502',
            'Tier 3: Qualified': '#4CAF50',
            'Tier 4: Monitor': '#9E9E9E'
        }
        
        tier_colors = [colors.get(tier, '#666666') for tier in tier_counts.index]
        
        fig = px.pie(
            values=tier_counts.values,
            names=tier_counts.index,
            color_discrete_sequence=tier_colors
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    # Prospects table
    st.subheader("Prospect List")
    
    # Add color coding
    def color_tier(val):
        if 'Tier 1' in val:
            return 'background-color: #ffebee'
        elif 'Tier 2' in val:
            return 'background-color: #fff3e0'
        elif 'Tier 3' in val:
            return 'background-color: #e8f5e9'
        return ''
    
    display_df = prospects_filtered[[
        'company', 'score', 'tier', 'jobs_last_30_days', 
        'signal_count', 'primary_signal', 'last_activity'
    ]].copy()
    
    display_df['last_activity'] = display_df['last_activity'].dt.strftime('%Y-%m-%d')
    display_df.columns = [
        'Company', 'Score', 'Tier', 'Jobs (30d)', 
        'Signals', 'Primary Signal', 'Last Activity'
    ]
    
    st.dataframe(
        display_df.style.applymap(color_tier, subset=['Tier']),
        use_container_width=True,
        height=600
    )
    
    # Export
    csv = display_df.to_csv(index=False)
    st.download_button(
        "📥 Export Prospects to CSV",
        csv,
        f"prospects_export_{datetime.now().strftime('%Y%m%d')}.csv",
        "text/csv"
    )

with tab3:
    st.header("Company Intelligence Profiles")
    
    # Company selector
    companies = sorted(prospects_filtered['company'].unique())
    selected_company = st.selectbox("Select Company", companies)
    
    if selected_company:
        prospect = prospects_df[prospects_df['company'] == selected_company].iloc[0]
        company_jobs = jobs_df[jobs_df['company'] == selected_company]
        
        # Header
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🎯 Score", f"{prospect['score']}/100")
        with col2:
            st.metric("📊 Total Jobs", prospect['total_jobs'])
        with col3:
            st.metric("📈 Last 30 Days", prospect['jobs_last_30_days'])
        with col4:
            st.metric("🎯 Signals", prospect['signal_count'])
        
        st.markdown("---")
        
        # Intelligence sections
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("📋 Recent Activity")
            
            recent_jobs = company_jobs.sort_values('date_scraped', ascending=False).head(15)
            
            for _, job in recent_jobs.iterrows():
                with st.container():
                    st.markdown(f"**{job['title']}**")
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.markdown(f"📍 {job['location']}")
                    with col_b:
                        st.markdown(f"📅 {job['date_scraped'].strftime('%Y-%m-%d')}")
                    with col_c:
                        st.markdown(f"🔗 {job['source']}")
                    
                    if job['technologies']:
                        st.markdown(f"🔧 **Technologies**: {job['technologies']}")
                    
                    st.markdown("---")
        
        with col2:
            st.subheader("🎯 Intelligence Summary")
            
            st.markdown(f"""
            <div class="metric-card">
            <h4>{prospect['tier']}</h4>
            <p>{prospect['action']}</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("##### Transformation Signals")
            
            # Display all signals
            if 'signals_list' in prospect and len(prospect['signals_list']) > 0:
                for i, signal in enumerate(prospect['signals_list'], 1):
                    st.markdown(f"**{i}. {signal}**")
            else:
                st.info(f"Primary Signal: {prospect['primary_signal']}")
            
            st.markdown("---")
            
            st.markdown("##### Timeline")
            st.markdown(f"**First Detected**: {prospect['first_seen'].strftime('%Y-%m-%d')}")
            st.markdown(f"**Last Activity**: {prospect['last_activity'].strftime('%Y-%m-%d')}")
            days_active = (prospect['last_activity'] - prospect['first_seen']).days
            st.markdown(f"**Active Period**: {days_active} days")
            
            st.markdown("##### Geographic Footprint")
            locations = company_jobs['location'].value_counts()
            for loc, count in locations.items():
                st.markdown(f"• {loc}: {count} jobs")
            
            st.markdown("##### Technology Stack")
            all_tech = company_jobs[company_jobs['technologies'] != '']['technologies'].value_counts()
            for tech, count in all_tech.head(5).items():
                st.markdown(f"• {tech}: {count} mentions")

with tab4:
    st.header("Market Trends")
    
    # Hiring velocity over time
    st.subheader("📈 Market Hiring Velocity")
    
    jobs_by_date = jobs_df.groupby(jobs_df['date_scraped'].dt.date).size().reset_index()
    jobs_by_date.columns = ['Date', 'Jobs Posted']
    
    fig = px.line(
        jobs_by_date,
        x='Date',
        y='Jobs Posted',
        markers=True
    )
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)
    
    # Top technologies
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔧 Technology Trends")
        
        tech_data = jobs_df[jobs_df['technologies'] != '']['technologies'].value_counts().head(8)
        
        fig = px.bar(
            x=tech_data.values,
            y=tech_data.index,
            orientation='h',
            color=tech_data.values,
            color_continuous_scale='Blues'
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Prospect Generation Rate")
        
        # Prospects by first seen date
        prospects_df['week'] = prospects_df['first_seen'].dt.to_period('W').astype(str)
        weekly_prospects = prospects_df.groupby('week').size().reset_index()
        weekly_prospects.columns = ['Week', 'New Prospects']
        
        fig = px.bar(
            weekly_prospects,
            x='Week',
            y='New Prospects',
            color_discrete_sequence=['#1f77b4']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Market insights
    st.subheader("💡 Market Insights")
    
    avg_signals = prospects_df['signal_count'].mean()
    total_companies_tracked = len(prospects_df)
    companies_with_multiple_signals = len(prospects_df[prospects_df['signal_count'] >= 2])
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
        <h3>{total_companies_tracked}</h3>
        <p>Companies Tracked</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
        <h3>{avg_signals:.1f}</h3>
        <p>Avg Signals per Company</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        pct_multiple = (companies_with_multiple_signals / total_companies_tracked * 100)
        st.markdown(f"""
        <div class="metric-card">
        <h3>{pct_multiple:.0f}%</h3>
        <p>With Multiple Signals</p>
        </div>
        """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("*🎯 Treasury Consulting Sales Intelligence | Powered by automated job market analysis*")
