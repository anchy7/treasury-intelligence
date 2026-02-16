"""
Simple Job Table Generator
Creates a comprehensive table of ALL treasury jobs from all sources
No scoring, just raw job data
"""

import pandas as pd
from datetime import datetime

def create_jobs_table():
    """
    Load all jobs and create a comprehensive table
    """
    print("\n" + "=" * 60)
    print("📋 TREASURY JOBS TABLE GENERATOR")
    print("=" * 60 + "\n")
    
    # Load job data
    print("📂 Loading job data...")
    try:
        df = pd.read_csv('treasury_jobs.csv')
        df['date_scraped'] = pd.to_datetime(df['date_scraped'])
        print(f"✅ Loaded {len(df)} jobs from {df['company'].nunique()} companies")
        
        # Show source distribution
        print("\n📊 Jobs by Source:")
        source_counts = df['source'].value_counts()
        for source, count in source_counts.items():
            print(f"   • {source}: {count} jobs ({count/len(df)*100:.1f}%)")
        print()
        
    except FileNotFoundError:
        print("❌ Error: treasury_jobs.csv not found")
        print("   Run the scraper first to collect job data")
        return
    
    # Extract country from location
    def extract_country(location):
        """Extract country from location string"""
        location = str(location).strip()
        
        # Common patterns
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
    
    # Create comprehensive table
    print("🔨 Creating comprehensive jobs table...")
    
    jobs_table = pd.DataFrame({
        'Company': df['company'],
        'Job_Title': df['title'],
        'Location': df['location'],
        'Country': df['location'].apply(extract_country),
        'Job_Source': df['source'],
        'Posted_Date': df['date_scraped'].dt.strftime('%Y-%m-%d'),
        'Technologies': df.get('technologies', ''),
        'Job_URL': df.get('url', '')
    })
    
    # Sort by date (newest first), then company
    jobs_table = jobs_table.sort_values(['Posted_Date', 'Company'], 
                                        ascending=[False, True])
    
    # Save to CSV
    output_file = 'treasury_jobs_table.csv'
    jobs_table.to_csv(output_file, index=False)
    
    print(f"✅ Created table with {len(jobs_table)} jobs")
    print(f"💾 Saved to: {output_file}")
    
    # Show statistics
    print("\n" + "=" * 60)
    print("📊 STATISTICS")
    print("=" * 60)
    
    print(f"\n🏢 Companies: {jobs_table['Company'].nunique()}")
    print(f"📍 Countries:")
    country_counts = jobs_table['Country'].value_counts()
    for country, count in country_counts.items():
        print(f"   • {country}: {count} jobs ({count/len(jobs_table)*100:.1f}%)")
    
    print(f"\n🔗 Job Sources:")
    source_counts = jobs_table['Job_Source'].value_counts()
    for source, count in source_counts.items():
        print(f"   • {source}: {count} jobs ({count/len(jobs_table)*100:.1f}%)")
    
    print(f"\n📅 Date Range:")
    print(f"   • Earliest: {jobs_table['Posted_Date'].min()}")
    print(f"   • Latest: {jobs_table['Posted_Date'].max()}")
    
    print(f"\n🏆 Top 10 Hiring Companies:")
    top_companies = jobs_table['Company'].value_counts().head(10)
    for i, (company, count) in enumerate(top_companies.items(), 1):
        print(f"   {i}. {company}: {count} jobs")
    
    # Show sample of data
    print("\n" + "=" * 60)
    print("📋 SAMPLE DATA (First 10 rows)")
    print("=" * 60 + "\n")
    
    sample = jobs_table.head(10)
    for idx, row in sample.iterrows():
        print(f"Company: {row['Company']}")
        print(f"Title: {row['Job_Title']}")
        print(f"Location: {row['Location']} ({row['Country']})")
        print(f"Source: {row['Job_Source']} | Date: {row['Posted_Date']}")
        if row['Technologies']:
            print(f"Tech: {row['Technologies']}")
        print("-" * 60)
    
    print("\n" + "=" * 60)
    print(f"✅ ALL JOBS SAVED TO: {output_file}")
    print("=" * 60 + "\n")
    
    return jobs_table

if __name__ == "__main__":
    create_jobs_table()
