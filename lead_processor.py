# lead_processor.py
import pandas as pd
from typing import Dict, List, Any, Optional, Set
import streamlit as st
from urllib.parse import urlparse
import time
import json
from io import BytesIO
import hashlib
import sqlite3
from datetime import datetime, timedelta
import os

class LeadProcessor:
    def __init__(self, scraper, analyzer, email_finder, generator, email_cleaner=None, cache_db_path="lead_cache.db"):
        self.scraper = scraper
        self.analyzer = analyzer
        self.email_finder = email_finder
        self.generator = generator
        self.email_cleaner = email_cleaner
        self.cache_db_path = cache_db_path
        self._init_cache()
        
    def _init_cache(self):
        """Initialize the SQLite database for caching processed leads"""
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_leads (
            lead_id TEXT PRIMARY KEY,
            website TEXT,
            processed_data TEXT,
            timestamp DATETIME
        )
        ''')
        
        conn.commit()
        conn.close()
        
    def _lead_cache_key(self, lead: Dict) -> str:
        """Generate a unique identifier for a lead based on company name and website"""
        company = self._clean_string(lead.get('company_name', '')).lower()
        website = self._clean_string(lead.get('Website', '')).lower()
        
        # Create a unique hash from company name and website
        key_str = f"{company}|{website}"
        return hashlib.md5(key_str.encode()).hexdigest()
        
    def _get_cached_lead(self, lead: Dict) -> Optional[Dict]:
        """Retrieve a processed lead from the cache if available"""
        lead_id = self._lead_cache_key(lead)
        website = self._clean_string(lead.get('Website', '')).lower()
        
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        # Look for a match by lead_id (exact match) or website
        cursor.execute(
            "SELECT processed_data FROM processed_leads WHERE lead_id = ? OR website = ?", 
            (lead_id, website)
        )
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            try:
                return json.loads(row[0])
            except:
                return None
        return None
        
    def _save_to_cache(self, lead: Dict, processed_data: Dict):
        """Save a processed lead to the cache"""
        lead_id = self._lead_cache_key(lead)
        website = self._clean_string(lead.get('Website', '')).lower()
        
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT OR REPLACE INTO processed_leads VALUES (?, ?, ?, ?)",
            (lead_id, website, json.dumps(processed_data), datetime.now().isoformat())
        )
        
        conn.commit()
        conn.close()

    def _format_list_to_string(self, data: List[Any]) -> str:
        """Convert list to string representation"""
        if not data:
            return ""
        return "; ".join(str(item) for item in data if item)

    def _clean_string(self, value: Any) -> str:
        """Clean and format string values"""
        if pd.isna(value) or value is None:
            return ""
        return str(value).strip()

    def process_lead(self, lead: Dict, use_cache: bool = True) -> Dict:
        """Process a single lead with caching support"""
        try:
            website = self._clean_string(lead.get('Website', ''))
            if not website or website.lower() == 'n/a':
                return self._create_empty_result(lead)
            
            # Check cache first if enabled
            if use_cache:
                cached_result = self._get_cached_lead(lead)
                if cached_result:
                    st.info(f"Using cached data for {lead.get('company_name', '')}")
                    return cached_result
            
            # Basic website data extraction
            website_data = self.scraper.scrape_website(website)
            
            # Simple email pattern matching first
            emails = self.email_finder.extract_emails_from_text(website_data['content'])
            
            # Analysis with cost-optimized LLM
            analysis = self.analyzer.analyze_content(website_data)
            
            # Generate potential emails if owner found
            domain = urlparse(website).netloc.replace('www.', '')
            potential_emails = []
            if analysis.get('owner_name'):
                potential_emails = self.email_finder.generate_potential_emails(
                    domain, 
                    analysis.get('owner_name')
                )
            
            # Use LLM to find additional emails
            llm_emails = self.email_finder.find_emails_with_llm(website_data['content'])
            all_emails = list(set(emails + llm_emails))
            
            # Clean emails if email_cleaner is available
            if self.email_cleaner:
                discovered_emails_str = self._format_list_to_string(all_emails)
                potential_emails_str = self._format_list_to_string(potential_emails)
                
                cleaned_discovered = self.email_cleaner.llm_clean_emails(discovered_emails_str)
                cleaned_potential = self.email_cleaner.llm_clean_emails(potential_emails_str)
                
                # Format back to strings
                discovered_emails_str = self._format_list_to_string(cleaned_discovered)
                potential_emails_str = self._format_list_to_string(cleaned_potential)
            else:
                discovered_emails_str = self._format_list_to_string(all_emails)
                potential_emails_str = self._format_list_to_string(potential_emails)
            
            # Create result
            result = {
                'company_name': self._clean_string(lead.get('company_name')),
                'full_address': self._clean_string(lead.get('full_address')),
                'town': self._clean_string(lead.get('town')),
                'Phone': self._clean_string(lead.get('Phone')),
                'Website': website,
                'Business Type': self._clean_string(lead.get('Business Type')),
                'processed': True,
                'owner_name': self._clean_string(analysis.get('owner_name')),
                'owner_title': self._clean_string(analysis.get('owner_title')),
                'confidence': self._clean_string(analysis.get('confidence', 'low')),
                'confidence_reasoning': self._clean_string(analysis.get('confidence_reasoning')),
                'discovered_emails': discovered_emails_str,
                'potential_emails': potential_emails_str,
                'key_facts': self._format_list_to_string(analysis.get('key_facts', [])),
                'error': ''
            }
            
            # Save to cache
            self._save_to_cache(lead, result)
            
            return result

        except Exception as e:
            error_result = self._create_empty_result(lead, str(e))
            return error_result

    def _create_empty_result(self, lead: Dict, error: str = "") -> Dict:
        """Create an empty result with basic lead info"""
        return {
            'company_name': self._clean_string(lead.get('company_name')),
            'full_address': self._clean_string(lead.get('full_address')),
            'town': self._clean_string(lead.get('town')),
            'Phone': self._clean_string(lead.get('Phone')),
            'Website': self._clean_string(lead.get('Website')),
            'Business Type': self._clean_string(lead.get('Business Type')),
            'processed': False,
            'owner_name': '',
            'owner_title': '',
            'confidence': 'none',
            'confidence_reasoning': '',
            'discovered_emails': '',
            'potential_emails': '',
            'key_facts': '',
            'error': error
        }

    def process_leads(self, leads: pd.DataFrame, use_cache: bool = True) -> pd.DataFrame:
        """Process multiple leads with caching support"""
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Define columns for output
        columns = [
            'company_name', 'full_address', 'town', 'Phone', 'Website', 
            'Business Type', 'processed', 'error', 'owner_name', 'owner_title',
            'confidence', 'confidence_reasoning', 'discovered_emails', 
            'potential_emails', 'key_facts'
        ]
        
        # Track duplicates to skip processing
        processed_websites = set()
        
        try:
            total_leads = len(leads)
            processed_count = 0
            skipped_count = 0
            
            for idx, row in leads.iterrows():
                lead_dict = row.to_dict()
                website = self._clean_string(lead_dict.get('Website', '')).lower()
                
                # Skip duplicate websites to avoid redundant processing
                if website in processed_websites and website and website != 'n/a':
                    skipped_count += 1
                    status_text.text(f"Skipping duplicate website: {website} ({skipped_count} skipped)")
                    continue
                
                if website:
                    processed_websites.add(website)
                
                status_text.text(f"Processing {processed_count + 1}/{total_leads}: {lead_dict.get('company_name', '')}")
                
                result = self.process_lead(lead_dict, use_cache=use_cache)
                results.append(result)
                
                processed_count += 1
                progress_bar.progress(processed_count / total_leads)
                
                # Reduce rate limiting for external APIs
                time.sleep(0.5)
            
            # Create DataFrame with specified columns
            df = pd.DataFrame(results)
            
            # Make sure all required columns exist
            for col in columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Reorder columns
            df = df[columns]
            df = df.fillna('')  # Clean up any NaN values
            
            # Clean emails once more as a final batch process if email_cleaner is available
            if self.email_cleaner and len(df) > 0:
                if 'discovered_emails' in df.columns and 'potential_emails' in df.columns:
                    # Convert DataFrame to list of dicts for batch processing
                    cleaned_data = self.email_cleaner.batch_clean_emails(df.to_dict('records'))
                    # Convert back to DataFrame
                    cleaned_df = pd.DataFrame(cleaned_data)
                    # Make sure we maintain the same columns and order
                    for col in df.columns:
                        if col not in cleaned_df.columns:
                            cleaned_df[col] = df[col]
                    df = cleaned_df[df.columns]
            
            status_text.text(f"Processing complete: {processed_count} leads processed, {skipped_count} duplicates skipped")
            
            return df
            
        except Exception as e:
            st.error(f"Error in batch processing: {str(e)}")
            return pd.DataFrame(columns=columns)