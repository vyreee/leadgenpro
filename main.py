import streamlit as st
import pandas as pd
from scraper import EnhancedWebsiteScraper
from analyzer import EnhancedContentAnalyzer
from email_finder import EmailFinder
from lead_processor import LeadProcessor
from lead_generator import LeadGenerator
from email_cleaner import EmailCleaner  # Import the new email cleaner
import traceback
import os

def init_api_components(openai_key, google_key):
    """Initialize API components with validation and debugging"""
    try:
        components = {}
        
        # Initialize each component separately with debug output
        with st.spinner("Initializing components..."):
            # Initialize scraper
            components['scraper'] = EnhancedWebsiteScraper()
            
            # Initialize Content Analyzer
            components['analyzer'] = EnhancedContentAnalyzer(api_key=str(openai_key).strip())
            
            # Initialize Email Finder
            components['email_finder'] = EmailFinder(api_key=str(openai_key).strip())
            
            # Initialize Lead Generator with caching
            components['lead_generator'] = LeadGenerator(api_key=str(google_key).strip())
            
            # Initialize Email Cleaner
            components['email_cleaner'] = EmailCleaner(api_key=str(openai_key).strip())
            
            # Initialize Lead Processor
            components['processor'] = LeadProcessor(
                scraper=components['scraper'],
                analyzer=components['analyzer'],
                email_finder=components['email_finder'],
                generator=components['lead_generator'],
                email_cleaner=components['email_cleaner']
            )
        
        return components
        
    except Exception as e:
        st.error("🚨 Initialization Error")
        st.error(f"Error details: {str(e)}")
        st.error(f"Error location:\n{traceback.format_exc()}")
        return None

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

def main():
    st.set_page_config(page_title="Lead Generator Pro", layout="wide")
    st.title("🎯 Lead Generator Pro")
    
    # Store session state for components
    if 'components' not in st.session_state:
        st.session_state['components'] = None
    
    # Add API key inputs in sidebar
    st.sidebar.title("API Configuration")
    
    # Use environment variables as defaults if available
    default_openai_key = os.environ.get('OPENAI_API_KEY', '')
    default_google_key = os.environ.get('GOOGLE_API_KEY', '')
    
    openai_api_key = st.sidebar.text_input(
        "OpenAI API Key", 
        value=default_openai_key,
        type="password",
        help="Get your API key from https://platform.openai.com/api-keys"
    )
    
    google_api_key = st.sidebar.text_input(
        "Google Places API Key",
        value=default_google_key,
        type="password",
        help="Get your API key from https://console.cloud.google.com/apis/credentials"
    )
    
    # Initialize components button
    if st.sidebar.button("Initialize/Reinitialize Components"):
        with st.spinner("Initializing components..."):
            st.session_state['components'] = init_api_components(
                openai_key=openai_api_key,
                google_key=google_api_key
            )

    # Check for API keys and components
    if not openai_api_key or not google_api_key:
        st.warning("Please enter your API keys in the sidebar to use the application.")
        
    if st.session_state['components'] is None:
        if openai_api_key and google_api_key:
            with st.spinner("Initializing components..."):
                st.session_state['components'] = init_api_components(
                    openai_key=openai_api_key,
                    google_key=google_api_key
                )
    
    # Proceed only if components are initialized
    if st.session_state['components'] is None:
        st.info("Please initialize the application components using the sidebar.")
        return
    
    components = st.session_state['components']
    processor = components['processor']
    
    # Create tabs for different functionalities
    tab1, tab2, tab3 = st.tabs(["Upload Leads", "Generate Leads", "Cache Management"])

    with tab1:
        st.header("Process Existing Leads")
        uploaded_file = st.file_uploader("Upload CSV with leads", type="csv")
        
        use_cache = st.checkbox("Use cached results when available", value=True, 
                              help="Enable to use previously processed data for faster results and reduced API costs")
        
        if uploaded_file:
            try:
                df = pd.read_csv(uploaded_file)
                st.write("Preview of uploaded data:")
                st.dataframe(df.head())
                
                required_cols = ['company_name', 'Website']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    st.error(f"Missing required columns: {', '.join(missing_cols)}")
                    return
                
                if st.button("Process Leads"):
                    with st.spinner("Processing leads..."):
                        # Remove duplicates
                        df = df.drop_duplicates(subset=['company_name', 'Website'])
                        st.write(f"Processing {len(df)} unique leads...")
                        
                        # Process leads
                        results_df = processor.process_leads(df, use_cache=use_cache)
                        
                        # Show results and download options
                        st.success("Processing complete!")
                        st.write("Results:")
                        st.dataframe(results_df)
                        
                        # Prepare download options
                        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                        
                        # CSV download
                        csv = results_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv,
                            file_name=f"processed_leads_{timestamp}.csv",
                            mime="text/csv"
                        )
                        
                        # Excel download
                        excel_data = processor.download_excel(results_df, f"processed_leads_{timestamp}.xlsx")
                        st.download_button(
                            label="📊 Download Excel",
                            data=excel_data,
                            file_name=f"processed_leads_{timestamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")
                st.error(f"Error details:\n{traceback.format_exc()}")

    with tab2:
        st.header("Generate New Leads")
        
        # Business type selection with categories
        business_categories = {
            "Professional Services": {
                "Real Estate": "real estate agent OR realtor",
                "Insurance Agent": "insurance agent OR insurance broker",
                "Financial Advisor": "financial advisor OR financial planner",
                "Lawyer": "lawyer OR attorney OR law firm",
                "Accountant": "accountant OR CPA OR accounting firm",
                "Marketing Agency": "marketing agency OR digital marketing",
                "Professional Services": "professional services"
            },
            "Health & Wellness": {
                "Doctor": "doctor OR physician OR medical practice",
                "Dentist": "dentist OR dental practice",
                "Health & Wellness": "health and wellness",
                "Health & Beauty": "health and beauty",
                "Fitness & Sports": "fitness and sports",
                "Pet Services": "pet services"
            },
            "Home & Auto Services": {
                "Home Services": "home services",
                "Automotive Services": "automotive services",
            },
            "Food & Entertainment": {
                "Restaurants & Food Services": "restaurants and food services",
                "Event & Entertainment Services": "event and entertainment services",
            },
            "Education & Retail": {
                "Education & Tutoring": "education and tutoring",
                "Retail & Local Shops": "retail and local shops",
            },
            "Events & Celebrations": {
                "Celebrations & Parties": "celebrations and parties",
                "Weddings": "weddings",
                "Baby & Parenting Events": "baby and parenting events",
                "Graduations & Educational Milestones": "graduations and educational milestones"
            }
        }
        
        # Flatten categories for selection
        all_business_types = {}
        for category, types in business_categories.items():
            all_business_types.update(types)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # First select category
            selected_category = st.selectbox(
                "Select Business Category",
                options=list(business_categories.keys())
            )
            
            # Then select business type within category
            selected_type = st.selectbox(
                "Select Business Type",
                options=list(business_categories[selected_category].keys())
            )
            
            # Allow custom search term
            custom_term = st.text_input(
                "Custom Search Term (Optional)",
                placeholder="Leave empty to use default term"
            )
            
            location = st.text_input(
                "Location (City, State)",
                placeholder="e.g., Boston, MA"
            )
        
        with col2:
            radius = st.slider(
                "Search Radius (miles)",
                min_value=5,
                max_value=50,
                value=20
            )
            
            max_results = st.slider(
                "Maximum Results",
                min_value=5,
                max_value=300,
                value=60,
                help="Note: Values over 60 will use region splitting to overcome API limitations"
            )
            
            use_region_splitting = st.checkbox(
                "Use Region Splitting", 
                value=max_results > 60,
                help="Split the search area into smaller regions to find more results"
            )
            
            use_cache_generation = st.checkbox(
                "Use Cached Results", 
                value=True,
                help="Use previously cached search results to reduce API costs"
            )
        
        # Replace the Generate Leads button handler in main.py
        if st.button("Generate Leads"):
            if not location or ',' not in location:
                st.error("Please enter location in City, State format")
                return
            
            # Verify the API key is valid
            if not google_api_key or google_api_key.strip() == "":
                st.error("Please enter a valid Google Places API Key in the sidebar")
                return
                
            try:
                with st.spinner("Generating leads..."):
                    # Use custom term if provided, otherwise use default
                    search_term = custom_term if custom_term else business_categories[selected_category][selected_type]
                    
                    # First verify the location can be geocoded
                    try:
                        # Clear existing geocode cache for this location if requested
                        if st.checkbox("Clear location cache before searching", value=False):
                            components['lead_generator'].clear_geocode_cache(location=location)
                            st.info(f"Cleared cache for {location}")
                        
                        # Test geocoding first before proceeding
                        with st.status("Verifying location...") as status:
                            lat, lng = components['lead_generator'].geocode_location(location)
                            status.update(label=f"Location verified: {lat}, {lng}", state="complete")
                    except ValueError as e:
                        st.error(f"Location error: {str(e)}")
                        st.info("Try clearing the cache for this location or check your Google API key billing status")
                        return
                        
                    # Generate leads
                    if use_region_splitting or max_results > 60:
                        # Use split region search for more comprehensive results
                        leads = components['lead_generator'].split_region_search(
                            business_type=search_term,
                            location=location,
                            radius=radius,
                            max_results=max_results
                        )
                    else:
                        # Use standard search for smaller result sets
                        leads = components['lead_generator'].generate_leads(
                            business_type=search_term,
                            location=location,
                            radius=radius,
                            max_results=max_results
                        )
                
                if leads:
                    # Convert to DataFrame
                    leads_df = pd.DataFrame(leads)
                    
                    # Remove duplicates
                    leads_df = leads_df.drop_duplicates(subset=['company_name', 'Website'])
                    
                    st.write(f"Found {len(leads_df)} unique leads")
                    st.dataframe(leads_df)
                    
                    # Add business type column
                    leads_df['Business Type'] = selected_type
                    
                    # Download raw leads option
                    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                    csv = leads_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Raw Leads",
                        data=csv,
                        file_name=f"raw_leads_{timestamp}.csv",
                        mime="text/csv"
                    )
                    
                    if st.button("Process Generated Leads"):
                        with st.spinner("Processing leads with owner/email discovery..."):
                            # Process leads with owner/email discovery
                            results_df = processor.process_leads(leads_df, use_cache=use_cache_generation)
                        
                        st.success("Processing complete!")
                        st.write("Results with owner information:")
                        st.dataframe(results_df)
                        
                        # Prepare downloads
                        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                        
                        # CSV download
                        csv = results_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv,
                            file_name=f"generated_leads_{timestamp}.csv",
                            mime="text/csv"
                        )
                        
                        # Excel download
                        excel_data = processor.download_excel(results_df, f"processed_leads_{timestamp}.xlsx")
                        st.download_button(
                            label="📊 Download Excel",
                            data=excel_data,
                            file_name=f"processed_leads_{timestamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                else:
                    st.warning("No leads found. Try adjusting your search parameters.")
                        
            except Exception as e:
                st.error(f"Error generating leads: {str(e)}")
                st.error(f"Error details:\n{traceback.format_exc()}")
                
                # If it's a Google API error, provide helpful suggestions
                error_str = str(e).lower()
                if "api" in error_str and ("key" in error_str or "billing" in error_str or "quota" in error_str):
                    st.warning("""
                    This appears to be a Google API issue. Please check:
                    1. Your Google API key is correct
                    2. Billing is enabled for your Google Cloud project
                    3. You have the Places API enabled
                    4. You haven't exceeded your quota
                    """)

    with tab3:
        if components:
            show_cache_stats(components)
        else:
            st.warning("Components not initialized. Please initialize the application first.")

if __name__ == "__main__":
    main()
