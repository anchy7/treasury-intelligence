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
        Returns list of signals with detailed descriptions
        """
        
        company_jobs = self.df[self.df['company'] == company_name]
        signals = []
        
        # Get all text to analyze
        all_text = ' '.join(
            company_jobs['title'].fillna('') + ' ' + 
            company_jobs['technologies'].fillna('')
        ).lower()
        
        # SIGNAL 1: SAP S/4HANA Migration
        if 's/4hana' in all_text or 's4hana' in all_text:
            signals.append({
                'type': 'SAP S/4HANA Migration',
                'confidence': 'High',
                'duration': '18-24 months',
                'service': 'S/4HANA Treasury Implementation & Change Management',
                'description': 'ERP transformation project with 2027 deadline'
            })
        
        # SIGNAL 2: Cloud TMS Implementation - Kyriba
        if 'kyriba' in all_text:
            signals.append({
                'type': 'Kyriba TMS Implementation',
                'confidence': 'High',
                'duration': '9-12 months',
                'service': 'Cloud TMS Implementation & Training',
                'description': 'Moving to cloud-based treasury management system'
            })
        
        # SIGNAL 3: Other Cloud TMS (GTreasury, FIS, Finastra)
        tms_systems = {
            'gtreasury': 'GTreasury',
            'fis': 'FIS TMS',
            'finastra': 'Finastra',
            'bloomberg': 'Bloomberg AIM'
        }
        for keyword, system_name in tms_systems.items():
            if keyword in all_text:
                signals.append({
                    'type': f'{system_name} Implementation',
                    'confidence': 'High',
                    'duration': '9-12 months',
                    'service': 'TMS Selection & Implementation',
                    'description': f'Cloud TMS implementation project - {system_name}'
                })
        
        # SIGNAL 4: API Connectivity & Real-Time Treasury
        if 'api' in all_text and ('connectivity' in all_text or 'integration' in all_text or 'real-time' in all_text):
            signals.append({
                'type': 'API Connectivity & Real-Time Treasury',
                'confidence': 'Medium',
                'duration': '6-9 months',
                'service': 'API Integration & Treasury Automation',
                'description': 'Building real-time connectivity with banks and systems'
            })
        
        # SIGNAL 5: Treasury Transformation Program
        if len(company_jobs) >= 3 and any(kw in all_text for kw in ['transformation', 'program', 'change']):
            signals.append({
                'type': 'Treasury Transformation Program',
                'confidence': 'High',
                'duration': '12-24 months',
                'service': 'Operating Model Design & PMO Services',
                'description': 'Comprehensive treasury function overhaul'
            })
        
        # SIGNAL 6: ESG/Sustainable Finance
        if any(kw in all_text for kw in ['esg', 'sustainable', 'green', 'csrd', 'taxonomy']):
            signals.append({
                'type': 'ESG Treasury & Sustainable Finance',
                'confidence': 'Medium',
                'duration': '6-12 months',
                'service': 'Sustainable Finance Framework & CSRD Compliance',
                'description': 'Building ESG treasury capabilities and reporting'
            })
        
        # SIGNAL 7: Cash Pooling & In-House Bank
        if any(kw in all_text for kw in ['cash pool', 'in-house bank', 'ihb', 'centralization']):
            signals.append({
                'type': 'Cash Pooling & In-House Bank Setup',
                'confidence': 'High',
                'duration': '6-12 months',
                'service': 'Cash Structure Optimization & IHB Implementation',
                'description': 'Centralizing cash and implementing in-house banking'
            })
        
        # SIGNAL 8: Working Capital Optimization
        if any(kw in all_text for kw in ['working capital', 'cash conversion', 'supply chain finance', 'payables', 'receivables']):
            signals.append({
                'type': 'Working Capital Optimization',
                'confidence': 'Medium',
                'duration': '6-9 months',
                'service': 'Working Capital Advisory & Process Optimization',
                'description': 'Improving cash conversion cycle and supply chain finance'
            })
        
        # SIGNAL 9: FX & Commodity Risk Management
        if any(kw in all_text for kw in ['hedge', 'hedging', 'forex', 'currency', 'commodity', 'derivatives']):
            signals.append({
                'type': 'FX & Risk Management Program',
                'confidence': 'Medium',
                'duration': '6-9 months',
                'service': 'Risk Management Framework & Hedging Strategy',
                'description': 'Implementing or upgrading risk management capabilities'
            })
        
        # SIGNAL 10: Treasury Analytics & Reporting
        if any(kw in all_text for kw in ['analytics', 'reporting', 'dashboard', 'visualization', 'power bi', 'tableau']):
            signals.append({
                'type': 'Treasury Analytics & Reporting',
                'confidence': 'Medium',
                'duration': '4-6 months',
                'service': 'Treasury Data Warehouse & Analytics Implementation',
                'description': 'Building advanced analytics and executive dashboards'
            })
        
        # SIGNAL 11: Bank Relationship Optimization
        if any(kw in all_text for kw in ['bank relationship', 'banking structure', 'bank connectivity', 'swift']):
            signals.append({
                'type': 'Bank Relationship Optimization',
                'confidence': 'Medium',
                'duration': '6-9 months',
                'service': 'Banking Structure Review & Optimization',
                'description': 'Optimizing bank relationships and connectivity'
            })
        
        # SIGNAL 12: Post-Merger Integration
        if any(kw in all_text for kw in ['integration', 'merger', 'acquisition', 'consolidation', 'harmonization']):
            signals.append({
                'type': 'Post-Merger Treasury Integration',
                'confidence': 'High',
                'duration': '12-18 months',
                'service': 'M&A Treasury Integration & Harmonization',
                'description': 'Integrating treasury functions after M&A activity'
            })
        
        # SIGNAL 13: Instant Payments Implementation
        if any(kw in all_text for kw in ['instant payment', 'sepa instant', 'real-time payment', 'rtp']):
            signals.append({
                'type': 'Instant Payments Implementation',
                'confidence': 'High',
                'duration': '6-9 months',
                'service': 'Instant Payments Strategy & Implementation',
                'description': 'Implementing real-time payment capabilities'
            })
        
        # SIGNAL 14: Treasury Shared Service Center
        if any(kw in all_text for kw in ['shared service', 'ssc', 'center of excellence', 'coe', 'centralization']):
            signals.append({
                'type': 'Treasury Shared Service Center',
                'confidence': 'High',
                'duration': '12-18 months',
                'service': 'Shared Service Center Design & Implementation',
                'description': 'Building treasury shared services or center of excellence'
            })
        
        # SIGNAL 15: Robotic Process Automation (RPA)
        if any(kw in all_text for kw in ['rpa', 'automation', 'robotic', 'bot']):
            signals.append({
                'type': 'Treasury Process Automation (RPA)',
                'confidence': 'Medium',
                'duration': '4-8 months',
                'service': 'RPA Implementation & Process Automation',
                'description': 'Automating manual treasury processes with bots'
            })
        
        # SIGNAL 16: Treasury Policy & Procedures
        if any(kw in all_text for kw in ['policy', 'procedure', 'governance', 'framework', 'guidelines']):
            signals.append({
                'type': 'Treasury Policy & Governance',
                'confidence': 'Low',
                'duration': '3-6 months',
                'service': 'Policy Development & Governance Framework',
                'description': 'Developing or updating treasury policies and procedures'
            })
        
        # SIGNAL 17: Liquidity & Cash Forecasting
        if any(kw in all_text for kw in ['forecast', 'liquidity', 'cash projection', 'planning']):
            signals.append({
                'type': 'Cash Forecasting Enhancement',
                'confidence': 'Medium',
                'duration': '4-6 months',
                'service': 'Cash Forecasting Model & Process Optimization',
                'description': 'Improving cash forecasting accuracy and process'
            })
        
        # SIGNAL 18: Treasury Organization Design
        if any(kw in all_text for kw in ['organization', 'org design', 'restructure', 'operating model', 'target operating model', 'tom']):
            signals.append({
                'type': 'Treasury Organization Redesign',
                'confidence': 'High',
                'duration': '6-12 months',
                'service': 'Operating Model & Organization Design',
                'description': 'Redesigning treasury organization structure and roles'
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
                'signal_count': len(signals),
                'primary_signal': signals[0]['type'] if signals else 'None',
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
                'signal_count': len(prospect['signals']),
                'primary_signal': prospect['signals'][0]['type'] if prospect['signals'] else 'None',
                'all_signals': ' | '.join([s['type'] for s in prospect['signals']]),
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
        print(f"   Total Signals: {prospect['signal_count']}")
        
        if prospect['signals']:
            print(f"   Key Signals:")
            for signal in prospect['signals'][:3]:  # Show top 3
                print(f"      • {signal['type']}")
                print(f"        Duration: {signal['duration']} | Service: {signal['service']}")
        
        print()
    
    print("=" * 60)
    print("✅ Lead scoring complete!")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
