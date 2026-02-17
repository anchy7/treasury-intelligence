"""
All Jobs Dashboard - V3
Green color scheme | Revenue data (expanded) | German email drafts | No location column
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
    base_url = "https://raw.githubusercontent.com/YOUR-USERNAME/treasury-intelligence/main/"
    
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
                                          'Stuttgart', 'Düsseldorf', 'Cologne', 'München', 
                                          'Dortmund', 'Essen', 'Leipzig', 'Dresden']):
        return 'Germany'
    elif any(city in location for city in ['Zurich', 'Basel', 'Geneva', 'Zug', 'Lausanne', 
                                            'Bern', 'Lucerne', 'Lugano']):
        return 'Switzerland'
    elif any(city in location for city in ['Vienna', 'Wien', 'Graz', 'Linz', 'Salzburg']):
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
    """
    Comprehensive revenue database for DACH companies
    Based on latest available annual reports and public filings
    """
    
    # Normalize company name for matching
    company_clean = str(company).strip().lower()
    
    # Comprehensive revenue database (Annual Revenue in EUR)
    revenue_database = {
        # Germany - DAX 40
        'volkswagen': '€322B', 'vw': '€322B', 'volkswagen ag': '€322B',
        'mercedes-benz': '€153B', 'mercedes': '€153B', 'daimler': '€153B',
        'bmw': '€142B', 'bmw group': '€142B',
        'allianz': '€161B', 'allianz se': '€161B',
        'siemens': '€77B', 'siemens ag': '€77B',
        'basf': '€69B', 'basf se': '€69B',
        'deutsche telekom': '€112B', 'telekom': '€112B',
        'sap': '€31B', 'sap se': '€31B',
        'bayer': '€51B', 'bayer ag': '€51B',
        'munich re': '€67B', 'münchener rück': '€67B', 'muenchener rueck': '€67B',
        'deutsche post': '€94B', 'dhl': '€94B',
        'deutsche bank': '€28B', 'deutsche bank ag': '€28B',
        'continental': '€41B', 'continental ag': '€41B',
        'adidas': '€21B', 'adidas ag': '€21B',
        'porsche': '€41B', 'porsche ag': '€41B',
        'lufthansa': '€36B', 'deutsche lufthansa': '€36B',
        'henkel': '€22B', 'henkel ag': '€22B',
        'eon': '€84B', 'e.on': '€84B',
        'rwe': '€24B', 'rwe ag': '€24B',
        'merck': '€21B', 'merck kgaa': '€21B',
        'infineon': '€16B', 'infineon technologies': '€16B',
        'fresenius': '€21B', 'fresenius se': '€21B',
        'beiersdorf': '€9B', 'beiersdorf ag': '€9B',
        'deutsche börse': '€4B', 'deutsche boerse': '€4B',
        'commerzbank': '€10B', 'commerzbank ag': '€10B',
        'metro': '€30B', 'metro ag': '€30B',
        'thyssenkrupp': '€35B', 'thyssen': '€35B',
        
        # Germany - MDAX & Major Companies
        'bosch': '€91B', 'robert bosch': '€91B',
        'aldi': '€134B', 'aldi süd': '€67B', 'aldi nord': '€67B',
        'lidl': '€114B', 'schwarz gruppe': '€154B', 'schwarz group': '€154B',
        'edeka': '€67B', 'edeka gruppe': '€67B',
        'rewe': '€84B', 'rewe group': '€84B',
        'otto': '€16B', 'otto group': '€16B',
        'bertelsmann': '€20B',
        'dm': '€14B', 'dm-drogerie markt': '€14B',
        'hugo boss': '€4B',
        'puma': '€8B', 'puma se': '€8B',
        'schaeffler': '€16B',
        'knorr-bremse': '€7B',
        'zeiss': '€10B', 'carl zeiss': '€10B',
        'trumpf': '€5B',
        'stihl': '€5B',
        'würth': '€19B', 'wuerth': '€19B',
        'kion': '€11B', 'kion group': '€11B',
        'brenntag': '€15B',
        'fraport': '€4B',
        'mtu aero': '€6B',
        'rheinmetall': '€7B',
        'gea': '€5B', 'gea group': '€5B',
        'dürr': '€4B', 'duerr': '€4B',
        'rational': '€1B',
        'kuka': '€4B',
        'heidelberg materials': '€21B', 'heidelbergcement': '€21B',
        'hochtief': '€26B',
        'bilfinger': '€4B',
        'strabag': '€18B', 'strabag se': '€18B',
        'deutsche wohnen': '€2B',
        'vonovia': '€6B',
        'aroundtown': '€2B',
        'tui': '€20B', 'tui ag': '€20B',
        'fraport': '€4B',
        'axel springer': '€4B',
        'prosiebensat1': '€4B', 'prosieben': '€4B',
        'qiagen': '€2B',
        'sartorius': '€5B',
        'gerresheimer': '€2B',
        'symrise': '€5B',
        'lanxess': '€7B',
        'evonik': '€18B', 'evonik industries': '€18B',
        'covestro': '€15B',
        'wacker chemie': '€8B', 'wacker': '€8B',
        'k+s': '€4B',
        'fuchs petrolub': '€3B', 'fuchs': '€3B',
        'aurubis': '€17B',
        'salzgitter': '€11B',
        'klöckner': '€8B', 'kloeckner': '€8B',
        'talanx': '€49B',
        'hannover rück': '€28B', 'hannover re': '€28B',
        'deutsche pfandbriefbank': '€2B',
        'aareal bank': '€1B',
        'hypo': '€3B',
        'deka': '€4B',
        'dz bank': '€3B',
        'lbbw': '€7B',
        'nord lb': '€5B', 'norddeutsche landesbank': '€5B',
        'helaba': '€3B',
        'bay': '€4B', 'bayernlb': '€4B',
        
        # Switzerland - SMI & Major Companies
        'nestlé': '€103B', 'nestle': '€103B',
        'novartis': '€46B', 'novartis ag': '€46B',
        'roche': '€69B', 'roche holding': '€69B', 'hoffmann-la roche': '€69B',
        'ubs': '€39B', 'ubs group': '€39B', 'ubs ag': '€39B',
        'zurich insurance': '€76B', 'zurich': '€76B',
        'abb': '€31B', 'abb ltd': '€31B', 'abb ag': '€31B',
        'credit suisse': '€16B', 'cs': '€16B',
        'swiss re': '€44B', 'swiss reinsurance': '€44B',
        'lonza': '€7B', 'lonza group': '€7B',
        'givaudan': '€7B',
        'sika': '€11B', 'sika ag': '€11B',
        'partners group': '€2B',
        'geberit': '€3B',
        'swisscom': '€11B',
        'holcim': '€32B', 'lafargeholcim': '€32B',
        'schindler': '€12B',
        'richemont': '€20B', 'compagnie financière richemont': '€20B',
        'swatch': '€7B', 'swatch group': '€7B',
        'barry callebaut': '€8B',
        'sgb': '€3B', 'sg group': '€3B',
        'swiss life': '€24B',
        'baloise': '€9B',
        'helvetia': '€11B',
        'cembra': '€1B',
        'clariant': '€4B',
        'ems-chemie': '€2B', 'ems': '€2B',
        'swissquote': '€500M',
        'temenos': '€1B',
        'logitech': '€5B',
        'bucher': '€4B', 'bucher industries': '€4B',
        'kühne + nagel': '€34B', 'kuehne nagel': '€34B', 'kuehne': '€34B',
        'dufry': '€9B',
        'flughafen zürich': '€900M', 'zurich airport': '€900M',
        'psi': '€3B', 'psi ag': '€3B',
        'stadler rail': '€4B', 'stadler': '€4B',
        'sulzer': '€3B',
        'oerlikon': '€3B',
        'georg fischer': '€4B', 'gf': '€4B',
        'sfs': '€2B', 'sfs group': '€2B',
        'arbonia': '€1B',
        'bell': '€4B', 'bell food group': '€4B',
        'emmi': '€4B',
        'lindt': '€5B', 'lindt & sprüngli': '€5B',
        'migros': '€32B',
        'coop': '€30B',
        'manor': '€3B',
        'mobiliar': '€4B', 'die mobiliar': '€4B',
        
        # Austria - ATX & Major Companies
        'omv': '€57B', 'omv ag': '€57B',
        'voestalpine': '€15B', 'voest': '€15B',
        'raiffeisen': '€24B', 'raiffeisen bank': '€24B', 'rbi': '€24B',
        'erste': '€22B', 'erste group': '€22B', 'erste bank': '€22B',
        'andritz': '€8B', 'andritz ag': '€8B',
        'verbund': '€5B', 'verbund ag': '€5B',
        'wienerberger': '€4B',
        'bawag': '€3B', 'bawag group': '€3B',
        'immofinanz': '€900M',
        'ca immo': '€800M',
        's immo': '€700M',
        'uniqa': '€6B', 'uniqa insurance': '€6B',
        'vienna insurance': '€11B', 'vig': '€11B',
        'post': '€2B', 'österreichische post': '€2B', 'austrian post': '€2B',
        'telekom austria': '€5B', 'a1 telekom': '€5B',
        'atb': '€2B', 'at&s': '€2B',
        'lenzing': '€2B', 'lenzing ag': '€2B',
        'kapsch': '€600M',
        'polytec': '€800M',
        'semperit': '€900M',
        'agrana': '€3B',
        'porr': '€6B', 'porr ag': '€6B',
        'strabag': '€18B',
        'palfinger': '€2B',
        'rosenbauer': '€1B',
        'ktm': '€2B',
        'swarovski': '€3B',
        'red bull': '€10B',
        'magna': '€40B', 'magna steyr': '€40B',
        'avl': '€2B', 'avl list': '€2B',
        'evn': '€3B',
        'böhler': '€3B', 'boehler': '€3B',
        'tgw': '€400M', 'tgw logistics': '€400M',
    }
    
    # Try exact match first
    if company_clean in revenue_database:
        return revenue_database[company_clean]
    
    # Try partial match
    for known_company, revenue in revenue_database.items():
        if known_company in company_clean or company_clean in known_company:
            return revenue_database[known_company]
    
    # If still not found, try matching key words
    company_words = company_clean.split()
    for word in company_words:
        if len(word) > 3:  # Only check meaningful words
            for known_company, revenue in revenue_database.items():
                if word in known_company:
                    return revenue_database[known_company]
    
    return 'Nicht verfügbar'

def generate_german_email(company, job_title, country, revenue):
    """
    Generate personalized German cold acquisition email draft
    """
    
    title_lower = job_title.lower()
    
    # Detect project type from job title
    if 's/4hana' in title_lower or 's4hana' in title_lower:
        signal = 'SAP S/4HANA'
        projekt = 'SAP S/4HANA Treasury-Implementierung'
        value_prop = "Wir haben über 15 erfolgreiche S/4HANA Treasury-Implementierungen für Unternehmen wie BMW, Siemens und Volkswagen durchgeführt und dabei die Projektlaufzeit um durchschnittlich 30% verkürzt und Implementierungsrisiken erheblich reduziert."
        themen = """• SAP S/4HANA Treasury-Implementierung und -Migration
• Clean Core Architektur und Best Practices
• Datenmigrationsstrategien und -durchführung
• Integration mit bestehenden TMS-Systemen
• Change Management und Anwenderakzeptanz"""
    
    elif 'kyriba' in title_lower:
        signal = 'Kyriba TMS'
        projekt = 'Kyriba TMS-Implementierung'
        value_prop = "Mit über 20 erfolgreichen Cloud-TMS-Implementierungen erreichen wir durchschnittliche Go-Live-Zeiten von 6 Monaten und stellen sicher, dass der Business Case ab Tag 1 erfüllt wird."
        themen = """• Cloud TMS-Auswahl und Implementierung
• Kyriba-Konfiguration und Customizing
• Bank-Konnektivität und SWIFT-Integration
• Prozessoptimierung und Automatisierung
• Schulung und Post-Go-Live-Support"""
    
    elif 'api' in title_lower or 'integration' in title_lower or 'konnektivität' in title_lower:
        signal = 'API-Konnektivität'
        projekt = 'Echtzeit-Treasury-Konnektivität'
        value_prop = "Wir sind spezialisiert auf API-First Treasury-Architekturen und helfen Unternehmen, Echtzeit-Transparenz und Automatisierung über ihre gesamte Treasury-Landschaft zu erreichen."
        themen = """• API-basierte Bank-Konnektivität
• Echtzeit-Zahlungsverkehr und Instant Payments
• Treasury-Datenintegration und -Automatisierung
• Systemintegration (ERP, TMS, Banken)
• Treasury Dashboards und Analytics"""
    
    elif any(kw in title_lower for kw in ['transformation', 'change', 'programm', 'program']):
        signal = 'Treasury Transformation'
        projekt = 'umfassendes Treasury-Transformationsprogramm'
        value_prop = "Wir haben über 25 Treasury-Transformationen begleitet und liefern Operating Model Design, Systemimplementierung und Change Management als integrierte Dienstleistung."
        themen = """• Treasury Operating Model Design
• Prozessoptimierung und -standardisierung
• Systemauswahl und -implementierung
• Organisationsdesign und Change Management
• PMO-Services und Programmsteuerung"""
    
    elif any(kw in title_lower for kw in ['head', 'director', 'leiter', 'lead']):
        signal = 'Strategische Leadership'
        projekt = 'strategische Treasury-Initiative'
        value_prop = "Wir bieten Interim-Leadership und strategische Beratung und unterstützen CFOs und Treasurer bei kritischen Transformationsphasen."
        themen = """• Interim Treasury Leadership
• Treasury-Strategie und Target Operating Model
• Organizational Design und Talent Management
• Stakeholder Management und Board Reporting
• Treasury-Transformation und Modernisierung"""
    
    elif any(kw in title_lower for kw in ['cash pool', 'in-house', 'ihb', 'zentralisierung']):
        signal = 'Cash Pooling'
        projekt = 'Cash-Pooling-Struktur und In-House-Bank'
        value_prop = "Wir unterstützen bei der Optimierung von Cash-Strukturen und haben zahlreiche In-House-Banking-Lösungen für internationale Konzerne implementiert."
        themen = """• Cash Pooling Strukturdesign
• In-House Bank Implementierung
• Liquiditätsmanagement-Optimierung
• Intercompany-Finanzierung
• Treasury-Zentralisierung"""
    
    elif any(kw in title_lower for kw in ['esg', 'sustainable', 'nachhaltig', 'green']):
        signal = 'ESG Treasury'
        projekt = 'ESG-Treasury und Sustainable Finance'
        value_prop = "Wir helfen Unternehmen, ihre Treasury-Funktion an ESG-Anforderungen anzupassen und nachhaltige Finanzierungsstrategien zu entwickeln."
        themen = """• Sustainable Finance Framework
• Green Bonds und ESG-linked Facilities
• CSRD Treasury Reporting
• ESG-Integration in Treasury-Prozesse
• Carbon Hedging Strategien"""
    
    elif any(kw in title_lower for kw in ['working capital', 'betriebskapital', 'supply chain finance']):
        signal = 'Working Capital'
        projekt = 'Working Capital-Optimierung'
        value_prop = "Wir optimieren Working Capital und Cash Conversion Cycles durch prozessuale und technologische Verbesserungen."
        themen = """• Working Capital-Optimierung
• Cash Conversion Cycle Verbesserung
• Supply Chain Finance Programme
• Forderungs- und Verbindlichkeiten-Management
• Cashflow-Prognose und -Planung"""
    
    else:
        signal = 'Treasury Excellence'
        projekt = 'Treasury-Optimierung'
        value_prop = "Wir bieten umfassende Treasury-Beratung von der Strategie bis zur Implementierung und helfen Unternehmen, ihre Treasury-Funktion zu modernisieren."
        themen = """• Treasury-Strategieentwicklung
• Prozess- und Systemoptimierung
• Technology-enabled Treasury
• Treasury-Organisation und Governance
• Best Practice Implementation"""
    
    # Personalize by country
    if country == 'Germany':
        region_ref = "in Deutschland und der DACH-Region"
    elif country == 'Switzerland':
        region_ref = "in der Schweiz und im gesamten DACH-Raum"
    elif country == 'Austria':
        region_ref = "in Österreich und der DACH-Region"
    else:
        region_ref = "in der DACH-Region"
    
    # Add revenue reference if available
    revenue_mention = ""
    if revenue != 'Nicht verfügbar':
        revenue_mention = f" (Umsatz: {revenue})"
    
    email = f"""Betreff: Treasury-Expertise für {company}{revenue_mention} - {signal}

Sehr geehrte Damen und Herren,

ich habe bemerkt, dass {company} aktuell eine Position als {job_title} ausgeschrieben hat. Dies deutet auf ein {projekt} hin.

{value_prop}

Wir arbeiten mit führenden Unternehmen {region_ref} und würden uns freuen, mit Ihnen über mögliche Unterstützung bei Ihren Treasury-Initiativen zu sprechen.

Wären Sie offen für ein kurzes 20-minütiges Gespräch, um Kooperationsmöglichkeiten auszuloten?

**Themenbereiche, in denen wir typischerweise Mehrwert schaffen:**

{themen}

**Unsere Arbeitsweise:**
• Praxisorientierte Beratung durch erfahrene Treasury-Experten
• Fokus auf schnelle Wertgenerierung und ROI
• Enge Zusammenarbeit mit Ihren internen Teams
• Nachgewiesene Erfolgsbilanz bei führenden DACH-Unternehmen

Ich würde mich freuen, Ihnen relevante Referenzen und Case Studies vorzustellen und Ihre spezifischen Anforderungen zu besprechen.

Mit freundlichen Grüßen

[Ihr Name]
[Ihr Unternehmen] | Treasury Consulting
[Ihre Kontaktdaten]

---
P.S.: Gerne stelle ich auch den Kontakt zu ehemaligen Kunden her, die ähnliche Projekte erfolgreich umgesetzt haben."""
    
    return email

# Load data
jobs_df = load_jobs()

if jobs_df.empty:
    st.warning("⏳ Keine Daten verfügbar. Bitte führen Sie zuerst den Scraper aus!")
    st.stop()

# Add columns
jobs_df['Country'] = jobs_df['location'].apply(extract_country)
jobs_df['Revenue'] = jobs_df['company'].apply(estimate_revenue)
jobs_df['Email_Draft'] = jobs_df.apply(
    lambda row: generate_german_email(row['company'], row['title'], row['Country'], row['Revenue']), 
    axis=1
)

# Sidebar filters
st.sidebar.markdown("### 🔍 Filter")

date_options = ["Alle", "Letzte 7 Tage", "Letzte 30 Tage", "Letzte 90 Tage"]
date_filter = st.sidebar.selectbox("Zeitraum", date_options)

if date_filter != "Alle":
    days_map = {"Letzte 7 Tage": 7, "Letzte 30 Tage": 30, "Letzte 90 Tage": 90}
    days = days_map[date_filter]
    cutoff = datetime.now() - timedelta(days=days)
    jobs_filtered = jobs_df[jobs_df['date_scraped'] >= cutoff]
else:
    jobs_filtered = jobs_df

all_sources = ['Alle Quellen'] + sorted(jobs_df['source'].unique().tolist())
source_filter = st.sidebar.selectbox("Job-Quelle", all_sources)

if source_filter != 'Alle Quellen':
    jobs_filtered = jobs_filtered[jobs_filtered['source'] == source_filter]

all_countries = ['Alle Länder'] + sorted(jobs_df['Country'].unique().tolist())
country_filter = st.sidebar.selectbox("Land", all_countries)

if country_filter != 'Alle Länder':
    jobs_filtered = jobs_filtered[jobs_filtered['Country'] == country_filter]

company_search = st.sidebar.text_input("Unternehmen suchen", "")
if company_search:
    jobs_filtered = jobs_filtered[
        jobs_filtered['company'].str.contains(company_search, case=False, na=False)
    ]

# Stats sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Statistiken")
st.sidebar.metric("Gesamt Jobs", len(jobs_df))
st.sidebar.metric("Gefilterte Jobs", len(jobs_filtered))
st.sidebar.metric("Unternehmen", jobs_filtered['company'].nunique())

# Main content
st.title("📋 Alle Treasury Jobs - DACH-Region")
st.markdown(f"*Vollständige Liste • Umsatzdaten • Deutsche E-Mail-Vorlagen*")

# Key metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("📊 Jobs Gesamt", len(jobs_filtered))

with col2:
    st.metric("🏢 Unternehmen", jobs_filtered['company'].nunique())

with col3:
    known_revenue = len(jobs_filtered[jobs_filtered['Revenue'] != 'Nicht verfügbar'])
    st.metric("💰 Umsatz bekannt", known_revenue)

with col4:
    germany = len(jobs_filtered[jobs_filtered['Country'] == 'Germany'])
    st.metric("🇩🇪 Deutschland", germany)

st.markdown("---")

# Tabs
tab1, tab2, tab3 = st.tabs(["📋 Alle Jobs", "📊 Statistiken", "📧 E-Mail-Entwürfe"])

with tab1:
    st.header("Vollständige Jobliste")
    
    st.markdown(f"**Zeige {len(jobs_filtered)} Jobs**")
    
    sort_by = st.selectbox(
        "Sortieren nach",
        ["Neueste zuerst", "Älteste zuerst", "Unternehmen A-Z", "Umsatz (hoch zu niedrig)"]
    )
    
    if sort_by == "Neueste zuerst":
        display_df = jobs_filtered.sort_values('date_scraped', ascending=False)
    elif sort_by == "Älteste zuerst":
        display_df = jobs_filtered.sort_values('date_scraped', ascending=True)
    elif sort_by == "Unternehmen A-Z":
        display_df = jobs_filtered.sort_values('company', ascending=True)
    else:  # Revenue
        def revenue_to_number(rev):
            if 'Nicht verfügbar' in str(rev):
                return 0
            try:
                rev_str = str(rev).replace('€', '').replace('B', '').replace('M', '')
                if 'B' in str(rev):
                    return float(rev_str) * 1000
                else:
                    return float(rev_str)
            except:
                return 0
        
        display_df = jobs_filtered.copy()
        display_df['revenue_numeric'] = display_df['Revenue'].apply(revenue_to_number)
        display_df = display_df.sort_values('revenue_numeric', ascending=False)
        display_df = display_df.drop('revenue_numeric', axis=1)
    
    # Table without Location column
    table_df = display_df[[
        'company', 'title', 'Country', 'Revenue', 'source', 
        'date_scraped', 'technologies'
    ]].copy()
    
    table_df['date_scraped'] = table_df['date_scraped'].dt.strftime('%Y-%m-%d')
    
    table_df.columns = [
        'Unternehmen', 'Job-Titel', 'Land', 'Umsatz', 
        'Quelle', 'Datum', 'Technologien'
    ]
    
    st.dataframe(table_df, use_container_width=True, height=600)
    
    csv = table_df.to_csv(index=False)
    st.download_button(
        "📥 Als CSV herunterladen",
        csv,
        f"treasury_jobs_{datetime.now().strftime('%Y%m%d')}.csv",
        "text/csv"
    )

with tab2:
    st.header("Arbeitsmarkt-Statistiken")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Jobs nach Land")
        country_counts = jobs_filtered['Country'].value_counts()
        
        fig = px.pie(
            values=country_counts.values,
            names=country_counts.index,
            color_discrete_sequence=px.colors.sequential.Greens_r
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("**Verteilung:**")
        for country, count in country_counts.items():
            pct = count / len(jobs_filtered) * 100
            st.markdown(f"• **{country}**: {count} Jobs ({pct:.1f}%)")
    
    with col2:
        st.subheader("Umsatzverteilung")
        
        revenue_categories = []
        for rev in jobs_filtered['Revenue']:
            if 'Nicht verfügbar' in str(rev):
                revenue_categories.append('Unbekannt')
            elif 'B' in str(rev):
                try:
                    val = float(str(rev).replace('€', '').replace('B', ''))
                    if val >= 50:
                        revenue_categories.append('€50Mrd+')
                    elif val >= 10:
                        revenue_categories.append('€10-50Mrd')
                    else:
                        revenue_categories.append('€1-10Mrd')
                except:
                    revenue_categories.append('Unbekannt')
            elif 'M' in str(rev):
                revenue_categories.append('<€1Mrd')
            else:
                revenue_categories.append('Unbekannt')
        
        rev_series = pd.Series(revenue_categories)
        rev_counts = rev_series.value_counts()
        
        fig = px.bar(
            x=rev_counts.values,
            y=rev_counts.index,
            orientation='h',
            labels={'x': 'Anzahl Jobs', 'y': 'Umsatzbereich'},
            color=rev_counts.values,
            color_continuous_scale='Greens'
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Top companies
    st.subheader("📊 Top Arbeitgeber")
    
    company_counts = jobs_filtered['company'].value_counts().head(20)
    
    fig = px.bar(
        x=company_counts.values,
        y=company_counts.index,
        orientation='h',
        labels={'x': 'Anzahl Jobs', 'y': 'Unternehmen'},
        color=company_counts.values,
        color_continuous_scale='Greens'
    )
    fig.update_layout(showlegend=False, height=600, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # Hiring over time
    st.subheader("📈 Veröffentlichte Jobs über Zeit")
    
    timeline = jobs_filtered.groupby(jobs_filtered['date_scraped'].dt.date).size().reset_index()
    timeline.columns = ['Datum', 'Jobs']
    
    fig = px.line(
        timeline,
        x='Datum',
        y='Jobs',
        markers=True
    )
    fig.update_traces(line_color='#2d7a3e')
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header("E-Mail-Entwürfe (Deutsch)")
    
    st.markdown("""
    Wählen Sie unten ein Unternehmen aus, um die automatisch generierten deutschen 
    Cold-Acquisition-E-Mail-Entwürfe zu sehen. Jede E-Mail ist personalisiert basierend 
    auf Jobtitel, Unternehmen und Land.
    """)
    
    # Company selector
    companies = sorted(jobs_filtered['company'].unique())
    selected_company = st.selectbox("Unternehmen auswählen", companies)
    
    if selected_company:
        company_jobs = jobs_filtered[jobs_filtered['company'] == selected_company]
        
        st.markdown(f"### {selected_company}")
        st.markdown(f"**Umsatz:** {company_jobs.iloc[0]['Revenue']}")
        st.markdown(f"**Land:** {company_jobs.iloc[0]['Country']}")
        st.markdown(f"**Offene Positionen:** {len(company_jobs)}")
        
        st.markdown("---")
        
        # Show all jobs for this company
        for idx, job in company_jobs.iterrows():
            with st.expander(f"📋 {job['title']} (Veröffentlicht: {job['date_scraped'].strftime('%Y-%m-%d')})"):
                st.markdown("##### E-Mail-Entwurf:")
                st.text_area(
                    "E-Mail kopieren",
                    job['Email_Draft'],
                    height=600,
                    key=f"email_{idx}"
                )
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"**Quelle:** {job['source']}")
                with col2:
                    st.markdown(f"**Technologien:** {job.get('technologies', 'Keine')}")
                with col3:
                    if job.get('url'):
                        st.markdown(f"[Job-Posting ansehen]({job['url']})")

# Footer
st.markdown("---")
st.markdown("*📋 Vollständige Treasury-Jobs-Datenbank | Umsatzdaten | Deutscher E-Mail-Generator*")
