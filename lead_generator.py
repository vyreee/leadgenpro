# lead_generator.py
import requests
import time
import math
from typing import Dict, List, Tuple, Optional
import streamlit as st
import os
import json
from urllib.parse import quote
import hashlib
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

class LeadGenerator:
    def __init__(self, api_key: str, cache_db_path: str = "lead_cache.db"):
        if not api_key:
            raise ValueError("Missing Google API Key")
        self.api_key = api_key
        self.cache_db_path = cache_db_path
        self._initialize_cache()
        
    def _initialize_cache(self):
        """Initialize SQLite database for caching results"""
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_cache (
            cache_key TEXT PRIMARY KEY,
            search_params TEXT,
            results TEXT,
            timestamp DATETIME
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS place_details_cache (
            place_id TEXT PRIMARY KEY,
            details TEXT,
            timestamp DATETIME
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def _cache_key(self, params: Dict) -> str:
        """Generate a unique cache key from search parameters"""
        param_str = json.dumps(params, sort_keys=True)
        return hashlib.md5(param_str.encode()).hexdigest()
    
    def _get_cached_search(self, params: Dict) -> Optional[List[Dict]]:
        """Retrieve results from cache if available and not expired"""
        cache_key = self._cache_key(params)
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        # Check for cached results less than 7 days old
        seven_days_ago = (datetime.now() - timedelta(days=7)).isoformat()
        cursor.execute(
            "SELECT results FROM search_cache WHERE cache_key = ? AND timestamp > ?", 
            (cache_key, seven_days_ago)
        )
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return json.loads(row[0])
        return None
    
    def _save_to_cache(self, params: Dict, results: List[Dict]):
        """Save results to cache"""
        cache_key = self._cache_key(params)
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT OR REPLACE INTO search_cache VALUES (?, ?, ?, ?)",
            (cache_key, json.dumps(params), json.dumps(results), datetime.now().isoformat())
        )
        
        conn.commit()
        conn.close()
    
    def _get_cached_place_details(self, place_id: str) -> Optional[Dict]:
        """Get cached place details if available"""
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        # Check for cached details less than 30 days old
        thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
        cursor.execute(
            "SELECT details FROM place_details_cache WHERE place_id = ? AND timestamp > ?", 
            (place_id, thirty_days_ago)
        )
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return json.loads(row[0])
        return None
    
    def _save_place_details(self, place_id: str, details: Dict):
        """Save place details to cache"""
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT OR REPLACE INTO place_details_cache VALUES (?, ?, ?)",
            (place_id, json.dumps(details), datetime.now().isoformat())
        )
        
        conn.commit()
        conn.close()
    
    def geocode_location(self, location: str) -> Tuple[float, float]:
        """Geocode a location string to coordinates with improved caching and error handling"""
        try:
            # Normalize location string to improve cache hits
            normalized_location = location.strip().lower()
            
            # Create a simple cache key for the location
            cache_params = {"geocode": normalized_location}
            cached_result = self._get_cached_search(cache_params)
            
            if cached_result:
                st.info(f"Using cached geocode for {location}")
                return cached_result[0], cached_result[1]
                
            # Get location coordinates
            geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={quote(location)}&key={self.api_key}"
            
            # Add retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    geocode_response = requests.get(geocode_url, timeout=10)
                    geocode_data = geocode_response.json()
                    
                    if geocode_data['status'] == 'OK':
                        # Extract coordinates
                        lat = geocode_data['results'][0]['geometry']['location']['lat']
                        lng = geocode_data['results'][0]['geometry']['location']['lng']
                        
                        # Cache the result
                        self._save_to_cache(cache_params, [lat, lng])
                        
                        return lat, lng
                    elif geocode_data['status'] == 'ZERO_RESULTS':
                        raise ValueError(f"Location not found: {location}")
                    elif geocode_data['status'] in ['OVER_QUERY_LIMIT', 'REQUEST_DENIED']:
                        if attempt < max_retries - 1:
                            # Exponential backoff
                            time.sleep(2 ** attempt)
                            continue
                        raise ValueError(f"API limit reached or request denied: {geocode_data['status']}")
                    else:
                        raise ValueError(f"Geocoding error: {geocode_data['status']}")
                
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    raise ValueError(f"Network error: {str(e)}")
            
            raise ValueError(f"Failed to geocode after {max_retries} attempts")
            
        except Exception as e:
            st.error(f"Geocoding error: {str(e)}")
            # Log the error for debugging
            print(f"Geocoding error for '{location}': {str(e)}")
            raise ValueError(f"Could not geocode location: {location} - {str(e)}")
        
    def clear_geocode_cache(self, location=None):
        """Clear geocode cache entries for a specific location or all locations"""
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        if location:
            # Normalize location string
            normalized_location = location.strip().lower()
            cache_params = {"geocode": normalized_location}
            cache_key = self._cache_key(cache_params)
            
            cursor.execute("DELETE FROM search_cache WHERE cache_key = ?", (cache_key,))
            deleted = cursor.rowcount
            st.info(f"Cleared geocode cache for '{location}' ({deleted} entries)")
        else:
            cursor.execute("DELETE FROM search_cache WHERE search_params LIKE '%geocode%'")
            deleted = cursor.rowcount
            st.info(f"Cleared all geocode cache entries ({deleted} entries)")
                
        conn.commit()
        conn.close()
    
    def get_place_details(self, place_id: str) -> Dict:
        """Get details for a place with caching"""
        # Check cache first
        cached_details = self._get_cached_place_details(place_id)
        if cached_details:
            return cached_details
            
        # Make API request if not in cache
        details_url = f"https://maps.googleapis.com/maps/api/place/details/json"
        details_params = {
            'place_id': place_id,
            'fields': 'name,formatted_address,formatted_phone_number,website',
            'key': self.api_key
        }
        
        details_response = requests.get(details_url, params=details_params)
        details_data = details_response.json()
        
        if details_data['status'] == 'OK':
            # Cache the result
            self._save_place_details(place_id, details_data['result'])
            return details_data['result']
        else:
            return {}
    
    def split_region_search(self, business_type: str, location: str, radius: int = 20, 
                           max_results: int = 300, splits: int = 2) -> List[Dict]:
        """Split a large region into smaller areas for more comprehensive results"""
        try:
            all_leads = []
            seen_place_ids = set()
            
            # Calculate how many results we need per region to reach max_results
            results_per_region = max(20, math.ceil(max_results / (splits * splits)))
            
            # Get the central location coordinates
            lat, lng = self.geocode_location(location)
            
            # Convert radius from miles to degrees (approximate)
            # 1 degree latitude = ~69 miles, 1 degree longitude varies but ~69 miles at equator
            degree_radius = radius / 69.0
            
            # Display progress information
            progress_bar = st.progress(0)
            status_text = st.empty()
            total_regions = splits * splits
            current_region = 0
            
            # Search in each sub-region
            for i in range(splits):
                for j in range(splits):
                    # Calculate new center point by offsetting from central location
                    new_lat = lat - (degree_radius/2) + (i * degree_radius/splits)
                    new_lng = lng - (degree_radius/2) + (j * degree_radius/splits)
                    
                    # Update status
                    current_region += 1
                    status_text.text(f"Searching region {current_region}/{total_regions}: {business_type}")
                    
                    # Calculate smaller radius for this sub-region (in miles, converted to meters for API)
                    sub_radius = (radius / splits) * 1609.34  # Convert miles to meters
                    
                    # Create search params for this sub-region
                    search_params = {
                        'location': f"{new_lat},{new_lng}",
                        'radius': sub_radius,
                        'keyword': business_type,
                        'key': self.api_key
                    }
                    
                    # Check cache first
                    cached_results = self._get_cached_search(search_params)
                    if cached_results:
                        st.info(f"Using cached results for region {current_region}")
                        region_leads = cached_results
                    else:
                        # If not in cache, make the API request
                        region_leads = self._search_places(new_lat, new_lng, business_type, sub_radius, results_per_region)
                        # Cache the results
                        self._save_to_cache(search_params, region_leads)
                    
                    # Add new leads to results, avoiding duplicates
                    for lead in region_leads:
                        if lead.get('place_id') not in seen_place_ids:
                            all_leads.append(lead)
                            seen_place_ids.add(lead.get('place_id'))
                            
                            # Check if we've reached the maximum
                            if len(all_leads) >= max_results:
                                status_text.text(f"Found maximum number of results: {max_results}")
                                return all_leads[:max_results]
                    
                    # Update progress
                    progress_bar.progress(current_region / total_regions)
                    
            status_text.text(f"Found {len(all_leads)} unique businesses")
            return all_leads[:max_results]
            
        except Exception as e:
            st.error(f"Error in split region search: {str(e)}")
            return []
    
    def _search_places(self, lat: float, lng: float, business_type: str, 
                      radius: float, max_results: int = 60) -> List[Dict]:
        """Search for places in a specific area (helper for split_region_search)"""
        leads = []
        next_page_token = None
        total_results = 0
        
        while total_results < max_results:
            # Prepare Places API request
            url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
            params = {
                'location': f"{lat},{lng}",
                'radius': radius,
                'keyword': business_type,
                'key': self.api_key
            }
            
            if next_page_token:
                params['pagetoken'] = next_page_token
                time.sleep(2)  # Required delay for next page token
            
            # Make request
            response = requests.get(url, params=params)
            data = response.json()
            
            if data['status'] != 'OK':
                break
            
            # Process results
            for place in data['results']:
                if total_results >= max_results:
                    break
                
                # Get place details (from cache if available)
                details = self.get_place_details(place['place_id'])
                
                if details:
                    lead = {
                        'company_name': details.get('name', place.get('name', '')),
                        'full_address': details.get('formatted_address', ''),
                        'Phone': details.get('formatted_phone_number', 'N/A'),
                        'Website': details.get('website', 'N/A'),
                        'place_id': place['place_id']
                    }
                    
                    leads.append(lead)
                    total_results += 1
                
                time.sleep(0.2)  # Reduced rate limiting
            
            next_page_token = data.get('next_page_token')
            if not next_page_token:
                break
        
        return leads
    
    def generate_leads(self, business_type: str, location: str, radius: int = 20, max_results: int = 60) -> List[Dict]:
        """Legacy method for compatibility - now calls split_region_search"""
        # For small result sets (<=60), just do a regular search
        if max_results <= 60:
            try:
                lat, lng = self.geocode_location(location)
                return self._search_places(lat, lng, business_type, radius * 1609.34, max_results)
            except Exception as e:
                st.error(f"Error generating leads: {str(e)}")
                return []
        else:
            # For larger result sets, use the split region approach
            splits = 2
            if max_results > 150:
                splits = 3
            if max_results > 250:
                splits = 4
                
            return self.split_region_search(business_type, location, radius, max_results, splits)
    
    def clear_cache(self, days_old: int = 0):
        """Clear cache entries older than specified days (0 means all)"""
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        if days_old > 0:
            cutoff_date = (datetime.now() - timedelta(days=days_old)).isoformat()
            cursor.execute("DELETE FROM search_cache WHERE timestamp < ?", (cutoff_date,))
            cursor.execute("DELETE FROM place_details_cache WHERE timestamp < ?", (cutoff_date,))
            deleted = cursor.rowcount
            st.info(f"Cleared {deleted} cache entries older than {days_old} days")
        else:
            cursor.execute("DELETE FROM search_cache")
            cursor.execute("DELETE FROM place_details_cache")
            st.info("Entire cache cleared")
            
        conn.commit()
        conn.close()
    
   # In the show_cache_stats function, update the cache management buttons:
def show_cache_stats(components):
    """Display cache statistics"""
    st.subheader("Cache Statistics")
    
    col1, col2 = st.columns(2)
    
    # Lead Generator cache stats
    try:
        with col1:
            generator_stats = components['lead_generator'].cache_stats()
            st.write("**Lead Generator Cache:**")
            st.write(f"- Search Cache Entries: {generator_stats['search_cache_entries']}")
            st.write(f"- Place Details Cache Entries: {generator_stats['place_details_cache_entries']}")
            
            if generator_stats['search_cache_date_range'][0]:
                st.write(f"- Oldest Entry: {generator_stats['search_cache_date_range'][0][:10]}")
                st.write(f"- Newest Entry: {generator_stats['search_cache_date_range'][1][:10]}")
                
            st.write(f"- Estimated API Calls Saved: {generator_stats['estimated_api_calls_saved']}")
    except Exception as e:
        col1.error(f"Error retrieving generator cache stats: {str(e)}")
    
    # Lead Processor cache stats
    try:
        with col2:
            processor_stats = components['processor'].cache_stats()
            st.write("**Lead Processor Cache:**")
            st.write(f"- Processed Lead Entries: {processor_stats['processed_lead_entries']}")
            st.write(f"- Unique Websites: {processor_stats['unique_websites']}")
            
            if processor_stats['date_range'][0]:
                st.write(f"- Oldest Entry: {processor_stats['date_range'][0][:10]}")
                st.write(f"- Newest Entry: {processor_stats['date_range'][1][:10]}")
                
            st.write(f"- Estimated API Calls Saved: {processor_stats['estimated_api_calls_saved']}")
    except Exception as e:
        col2.error(f"Error retrieving processor cache stats: {str(e)}")
    
    # Cache management buttons
    st.subheader("Cache Management")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Clear Old Cache (30+ days)"):
            components['lead_generator'].clear_cache(days_old=30)
            components['processor'].clear_lead_cache(days_old=30)
            st.success("Cleared cache entries older than 30 days")
            
    with col2:
        if st.button("Clear All Search Cache"):
            components['lead_generator'].clear_cache(days_old=0)
            st.success("Cleared all search cache entries")
            
    with col3:
        if st.button("Clear All Processed Lead Cache"):
            components['processor'].clear_lead_cache(days_old=0)
            st.success("Cleared all processed lead cache entries")
    
    # Add a new section specifically for geocode cache management
    st.subheader("Geocode Cache Management")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Clear All Geocode Cache"):
            components['lead_generator'].clear_geocode_cache()
            st.success("Cleared all geocode cache entries")
            
    with col2:
        location_to_clear = st.text_input("Location to Clear from Cache", 
                                          placeholder="e.g., Waltham, MA")
        if st.button("Clear Specific Location Cache") and location_to_clear:
            components['lead_generator'].clear_geocode_cache(location=location_to_clear)
            st.success(f"Cleared cache for {location_to_clear}")


def verify_cache(self) -> Dict:
    """Verify cache integrity and return diagnostics"""
    conn = sqlite3.connect(self.cache_db_path)
    cursor = conn.cursor()
    
    # Check if tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    # Check row counts
    table_counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        table_counts[table] = cursor.fetchone()[0]
    
    # Check for corrupted entries
    corrupted_entries = {}
    
    # Check search_cache for valid JSON
    cursor.execute("SELECT cache_key, search_params, results FROM search_cache")
    for row in cursor.fetchall():
        cache_key, search_params, results = row
        try:
            json.loads(search_params)
            json.loads(results)
        except json.JSONDecodeError:
            if 'search_cache' not in corrupted_entries:
                corrupted_entries['search_cache'] = []
            corrupted_entries['search_cache'].append(cache_key)
    
    # Check place_details_cache for valid JSON
    cursor.execute("SELECT place_id, details FROM place_details_cache")
    for row in cursor.fetchall():
        place_id, details = row
        try:
            json.loads(details)
        except json.JSONDecodeError:
            if 'place_details_cache' not in corrupted_entries:
                corrupted_entries['place_details_cache'] = []
            corrupted_entries['place_details_cache'].append(place_id)
    
    conn.close()
    
    # Return diagnostics
    return {
        'tables_present': tables,
        'table_counts': table_counts,
        'corrupted_entries': corrupted_entries,
        'cache_size_kb': os.path.getsize(self.cache_db_path) / 1024 if os.path.exists(self.cache_db_path) else 0,
        'cache_health': 'good' if not corrupted_entries else 'corrupted'
    }
