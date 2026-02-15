"""
Lead Scoring Engine for Treasury Consulting
Analyzes job posting data to identify high-value prospects
"""

import pandas as pd
from datetime import datetime, timedelta
import re

class LeadScoringEngine:
    def __init__(self, jobs_df):
        self.df = jobs_df
        self.company_scores = []
        
    def calculate_transformation_score(self, company_name):
        """
        Calculate 0-100 transformation score for a company
        Higher score = Better consulting prospect
        """
        
        # Get all jobs for this company
        company_jobs = self.df[self.df['company'] == company_name]
        
        if len(company_jobs) == 0:
            return 0
        
        score = 0
        
        # FACTOR 1: Hiring Velocity (0-25 points)
        # Recent jobs indicate active projects
        last_30_days = company_jobs[
            company_jobs['date_scraped'] >= datetime.now() - timedelta(days=30)
        ]
        jobs_count = len(last_30_days)
        
        if jobs_count >= 5:
            score += 25
        elif jobs_count >= 3:
            score += 20
        elif jobs_count >= 2:
            score += 15
        elif jobs_count >= 1:
            score += 10
        
        # FACTOR 2: Technology Transformation Signals (0-30 points)
        # SAP S/4HANA, Kyriba, APIs = major projects
        all_tech = ' '.join(company_jobs['technologies'].fillna('').astype(str))
        
        if 'SAP S/4HANA' in all_tech:
            score += 15  # Major transformation
        if 'Kyriba' in all_tech:
            score += 10  # Cloud TMS implementation
        if 'API' in all_tech or 'Python' in all_tech:
            score += 5   # Modernization/automation
        
        # FACTOR 3: Job Type Mix (0-25 points)
        # Interim/project roles = active transformation
        titles = ' '.join(company_jobs['title'].fillna('').astype(str)).lower()
        
        interim_keywords = ['interim', 'freelance', 'project', 'contract', 'consultant']
        interim_count = sum(1 for kw in interim_keywords if kw in titles)
        
        if interim_count >= 2:
            score += 25
        elif interim_count >= 1:
            score += 15
        
        # FACTOR 4: Seniority Mix (0-10 points)
        # Senior roles = strategic change
        senior_keywords = ['head', 'director', 'senior', 'lead', 'principal', 'chief']
        senior_count = sum(1 for kw in senior_keywords if kw in titles)
        
        if senior_count >= 2:
            score += 10
        elif senior_count >= 1:
            score += 5
        
        # FACTOR 5: Geographic Expansion (0-10 points)
        # Multi-location = expansion/transformation
        unique_locations = company_jobs['location'].nunique()
        
        if unique_locations >= 3:
            score += 10
        elif unique_locations >= 2:
            score += 5
        
        return min(100, score)
    
    def detect_transformation_signals(self, company_name):
        """
        Identify specific transformation projects
        Returns list of signals (SAP migration, TMS implementation, etc.)
        """
        
        company_jobs = self.df[self.df['company'] == company_name]
        signals = []
        
        # Get all text to analyze
        all_text = ' '.join(
            company_jobs['title'].fillna('') + ' ' + 
            company_jobs['technologies'].fillna('')
        ).lower()
        
        # SAP S/4HANA Migration
        if 's/4hana' in all_text or 's4hana' in all_text:
            signals.append({
                'type': 'SAP S/4HANA Migration',
                'confidence': 'High',
                'project_value': '€2-4M',
                'duration': '18-24 months',
                'service': 'Implementation & Change Management'
            })
        
        # Cloud TMS Implementation
        if 'kyriba' in all_text:
            signals.append({
                'type': 'Kyriba TMS Implementation',
                'confidence': 'High',
                'project_value': '€500K-1M',
                'duration': '9-12 months',
                'service': 'TMS Implementation & Training'
            })
        
        # API Modernization
        if 'api' in all_text and ('connectivity' in all_text or 'integration' in all_text):
            signals.append({
                'type': 'API Connectivity & Automation',
                'confidence': 'Medium',
                'project_value': '€200-500K',
                'duration': '6-9 months',
                'service': 'API Integration & Treasury Automation'
            })
        
        # Treasury Transformation Program
        if len(company_jobs) >= 3 and any(kw in all_text for kw in ['transformation', 'program', 'change']):
            signals.append({
                'type': 'Treasury Transformation Program',
                'confidence': 'High',
                'project_value': '€1-3M',
                'duration': '12-24 months',
                'service': 'Operating Model Design & PMO'
            })
        
        # ESG/Sustainable Finance
        if any(kw in all_text for kw in ['esg', 'sustainable', 'green', 'csrd']):
            signals.append({
                'type': 'ESG Treasury Build-Out',
                'confidence': 'Medium',
                'project_value': '€150-400K',
                'duration': '6-12 months',
                'service': 'Sustainable Finance Advisory'
            })
        
        return signals
    
    def classify_prospect_tier(self, score):
        """
        Classify prospects into tiers based on score
        """
        if score >= 80:
            return 'Tier 1: Hot', '🔥 Immediate Outreach'
        elif score >= 60:
            return 'Tier 2: Warm', '🌡️ Targeted Campaign'
        elif score >= 40:
            return 'Tier 3: Qualified', '📋 Marketing Funnel'
        else:
            return 'Tier 4: Monitor', '👀 Database Only'
    
    def estimate_project_value(self, signals):
        """
        Estimate total project value based on detected signals
        """
        if not signals:
            return '€0'
        
        # Parse project values and sum them
        total_min = 0
        total_max = 0
        
        for signal in signals:
            value_str = signal['project_value']
            # Parse "€500K-1M" format
            matches = re.findall(r'€(\d+(?:\.\d+)?)(K|M)', value_str)
            
            if len(matches) >= 2:
                # Range found
                min_val = float(matches[0][0])
                min_unit = matches[0][1]
                max_val = float(matches[1][0])
                max_unit = matches[1][1]
                
                # Convert to euros
                min_euros = min_val * 1000 if min_unit == 'K' else min_val * 1000000
                max_euros = max_val * 1000 if max_unit == 'K' else max_val * 1000000
                
                total_min += min_euros
                total_max += max_euros
        
        if total_min == 0:
            return '€0'
        
        # Format nicely
        if total_max >= 1000000:
            return f"€{total_min/1000000:.1f}-{total_max/1000000:.1f}M"
        else:
            return f"€{total_min/1000:.0f}-{total_max/1000:.0f}K"
    
    def analyze_all_companies(self):
        """
        Analyze all companies and generate prospect list
        """
        print("🔍 Analyzing companies for consulting opportunities...")
        print("=" * 60)
        
        companies = self.df['company'].unique()
        
        for company in companies:
            # Calculate score
            score = self.calculate_transformation_score(company)
            
            # Skip low-value prospects
            if score < 20:
                continue
            
            # Detect signals
            signals = self.detect_transformation_signals(company)
            
            # Get tier
            tier, action = self.classify_prospect_tier(score)
            
            # Get company stats
            company_jobs = self.df[self.df['company'] == company]
            
            last_30 = company_jobs[
                company_jobs['date_scraped'] >= datetime.now() - timedelta(days=30)
            ]
            
            self.company_scores.append({
                'company': company,
                'score': score,
                'tier': tier,
                'action': action,
                'total_jobs': len(company_jobs),
                'jobs_last_30_days': len(last_30),
                'locations': company_jobs['location'].nunique(),
                'signals': signals,
                'project_value': self.estimate_project_value(signals),
                'first_seen': company_jobs['date_scraped'].min(),
                'last_activity': company_jobs['date_scraped'].max()
            })
        
        # Sort by score
        self.company_scores.sort(key=lambda x: x['score'], reverse=True)
        
        print(f"✅ Analyzed {len(companies)} companies")
        print(f"📊 Identified {len(self.company_scores)} prospects (score ≥ 20)")
        
        tier1 = len([c for c in self.company_scores if c['score'] >= 80])
        tier2 = len([c for c in self.company_scores if 60 <= c['score'] < 80])
        tier3 = len([c for c in self.company_scores if 40 <= c['score'] < 60])
        
        print(f"🔥 Tier 1 (Hot): {tier1}")
        print(f"🌡️ Tier 2 (Warm): {tier2}")
        print(f"📋 Tier 3 (Qualified): {tier3}")
        
        return self.company_scores
    
    def save_prospects_csv(self, filename='prospects.csv'):
        """
        Save prospect list to CSV
        """
        if not self.company_scores:
            print("⚠️ No prospects to save")
            return
        
        # Flatten for CSV
        rows = []
        for prospect in self.company_scores:
            row = {
                'company': prospect['company'],
                'score': prospect['score'],
                'tier': prospect['tier'],
                'action': prospect['action'],
                'total_jobs': prospect['total_jobs'],
                'jobs_last_30_days': prospect['jobs_last_30_days'],
                'locations': prospect['locations'],
                'project_value': prospect['project_value'],
                'signal_count': len(prospect['signals']),
                'primary_signal': prospect['signals'][0]['type'] if prospect['signals'] else 'None',
                'first_seen': prospect['first_seen'],
                'last_activity': prospect['last_activity']
            }
            rows.append(row)
        
        df = pd.DataFrame(rows)
        df.to_csv(filename, index=False)
        
        print(f"\n💾 Saved prospects to {filename}")

def main():
    """
    Main function - run lead scoring
    """
    print("\n" + "=" * 60)
    print("🎯 TREASURY CONSULTING LEAD SCORING ENGINE")
    print("=" * 60 + "\n")
    
    # Load job data
    print("📂 Loading job data...")
    try:
        df = pd.read_csv('treasury_jobs.csv')
        df['date_scraped'] = pd.to_datetime(df['date_scraped'])
        print(f"✅ Loaded {len(df)} jobs from {df['company'].nunique()} companies\n")
    except FileNotFoundError:
        print("❌ Error: treasury_jobs.csv not found")
        print("   Run the scraper first to collect job data")
        return
    
    # Run scoring
    engine = LeadScoringEngine(df)
    prospects = engine.analyze_all_companies()
    
    # Save results
    engine.save_prospects_csv('prospects.csv')
    
    # Display top prospects
    print("\n" + "=" * 60)
    print("🔥 TOP 10 HOT PROSPECTS")
    print("=" * 60 + "\n")
    
    for i, prospect in enumerate(prospects[:10], 1):
        print(f"{i}. {prospect['company']}")
        print(f"   Score: {prospect['score']}/100 | {prospect['tier']}")
        print(f"   Activity: {prospect['jobs_last_30_days']} jobs in last 30 days")
        print(f"   Project Value: {prospect['project_value']}")
        
        if prospect['signals']:
            print(f"   Signals:")
            for signal in prospect['signals'][:2]:  # Show top 2
                print(f"      • {signal['type']} ({signal['project_value']})")
        
        print()
    
    print("=" * 60)
    print("✅ Lead scoring complete!")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
