# analyzer.py
from openai import OpenAI
from typing import Dict
import json
import streamlit as st

class EnhancedContentAnalyzer:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Missing OPENAI_API_KEY")
        # Direct API key assignment during client creation
        self.client = OpenAI(api_key=api_key)

    def analyze_content(self, website_data: Dict) -> Dict:
        """Analyze website content using GPT-3.5-turbo with cost optimization"""
        if not website_data['success']:
            return {
                'owner_name': None,
                'key_facts': [],
                'reasoning': website_data.get('error', 'Failed to fetch content')
            }

        try:
            # Optimize content length to reduce token usage
            content = website_data['content'][:3000]  # Limit content length
            
            messages = [
                {
                    "role": "system",
                    "content": """Extract business information from website content.
Focus on:
1. Owner/founder name (if mentioned with high confidence)
2. Contact methods and preferences
3. Key business facts

Return JSON with:
{
    "owner_name": "Full Name or null",
    "owner_title": "Position/Title or null",
    "confidence": "high/medium/low",
    "confidence_reasoning": "Brief explanation",
    "key_facts": ["fact1", "fact2"],
    "contact_methods": {
        "primary": "main contact method",
        "email_pattern": "typical email format if found"
    }
}"""
                },
                {
                    "role": "user",
                    "content": f"Website content to analyze:\n\n{content}"
                }
            ]

            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=0,
                max_tokens=400,
                response_format={ "type": "json_object" }
            )

            analysis = json.loads(response.choices[0].message.content)
            
            # Format the response
            return {
                'owner_name': analysis.get('owner_name'),
                'owner_title': analysis.get('owner_title'),
                'confidence': analysis.get('confidence', 'low'),
                'confidence_reasoning': analysis.get('confidence_reasoning', ''),
                'key_facts': analysis.get('key_facts', []),
                'business_identity': {},  # Simplified
                'contact_patterns': analysis.get('contact_methods', {})
            }

        except Exception as e:
            st.error(f"Analysis error: {str(e)}")
            return {
                'owner_name': None,
                'key_facts': [],
                'reasoning': f'Error in analysis: {str(e)}'
            }


# In analyzer.py and email_finder.py constructor:
def __init__(self, api_key: str):
    if not api_key:
        raise ValueError("Missing API Key")
    
    # Add proxy configuration if needed
    proxy_config = {}
    if 'https_proxy' in os.environ:
        proxy_config['https'] = os.environ['https_proxy']
    if 'http_proxy' in os.environ:
        proxy_config['http'] = os.environ['http_proxy']
        
    self.client = OpenAI(
        api_key=api_key,
        http_client=requests.Session()  # Add custom session if needed
    )