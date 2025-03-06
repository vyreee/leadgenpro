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
        """Geocode a location string to coordinates with caching"""
        # Create a simple cache key for the location
        cache_params = {"geocode": location}
        cached_result = self._get_cached_search(cache_params)
        
        if cached_result:
            st.info(f"Using cached geocode for {location}")
            return cached_result[0], cached_result[1]
            
        # Get location coordinates
        geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={quote(location)}&key={self.api_key}"
        geocode_response = requests.get(geocode_url)
        geocode_data = geocode_response.json()
        
        if geocode_data['status'] != 'OK':
            raise ValueError(f"Could not geocode location: {location}")
        
        # Extract coordinates
        lat = geocode_data['results'][0]['geometry']['location']['lat']
        lng = geocode_data['results'][0]['geometry']['location']['lng']
        
        # Cache the result
        self._save_to_cache(cache_params, [lat, lng])
        
        return lat, lng
    
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
    
    def cache_stats(self) -> Dict:
        """Return statistics about the cache"""
        conn = sqlite3.connect(self.cache_db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM search_cache")
        search_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM place_details_cache")
        details_count = cursor.fetchone()[0]
        
        # Get oldest and newest entries
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM search_cache")
        search_dates = cursor.fetchone()
        
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM place_details_cache")
        details_dates = cursor.fetchone()
        
        conn.close()
        
        return {
            "search_cache_entries": search_count,
            "place_details_cache_entries": details_count,
            "search_cache_date_range": search_dates,
            "place_details_date_range": details_dates,
            "estimated_api_calls_saved": search_count + details_count
        }