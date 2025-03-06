# email_finder.py
import re
from typing import List, Optional
from openai import OpenAI
import streamlit as st
from urllib.parse import urlparse
import json
import os

class EmailFinder:
    def __init__(self, api_key: str):
        self.email_patterns = [
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            r'mailto:[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            r'data-email=["\'][a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}["\']',
            r'email:["\']?[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}["\']?'
        ]
        self.client = OpenAI(api_key=api_key)

    def extract_emails_from_text(self, text: str) -> List[str]:
        """Extract emails using multiple regex patterns"""
        all_emails = set()
        for pattern in self.email_patterns:
            matches = re.findall(pattern, text)
            clean_matches = [re.sub(r'^mailto:|^email:|^data-email=|["\']', '', match) for match in matches]
            all_emails.update(clean_matches)
        return list(all_emails)

    def generate_potential_emails(self, domain: str, owner_name: Optional[str] = None) -> List[str]:
        """Generate potential email addresses based on domain and owner name"""
        variations = [
            f"info@{domain}",
            f"contact@{domain}",
            f"hello@{domain}",
            f"support@{domain}",
            f"sales@{domain}"
        ]
        
        if owner_name:
            name_parts = owner_name.lower().split()
            if len(name_parts) >= 2:
                first, last = name_parts[0], name_parts[-1]
                variations.extend([
                    f"{first}@{domain}",
                    f"{last}@{domain}",
                    f"{first}.{last}@{domain}",
                    f"{first[0]}{last}@{domain}",
                    f"{first}{last[0]}@{domain}"
                ])
        return variations

    def find_emails_with_llm(self, content: str) -> List[str]:
        """Use LLM to find potential emails in content"""
        try:
            messages = [
                {
                    "role": "system",
                    "content": """Analyze the text and extract:
1. Any email addresses mentioned
2. Any patterns that could be email addresses
3. Any contact information that might suggest email formats

Return JSON with:
{
    "discovered_emails": ["list of found emails"],
    "potential_patterns": ["list of likely email patterns"],
    "confidence": "high/medium/low"
}"""
                },
                {
                    "role": "user",
                    "content": f"Find email addresses in this content:\n\n{content[:2000]}"
                }
            ]

            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=0,
                max_tokens=200,
                response_format={ "type": "json_object" }
            )

            result = json.loads(response.choices[0].message.content)
            return result.get('discovered_emails', []) + result.get('potential_patterns', [])
        except Exception as e:
            st.error(f"Error in LLM email discovery: {str(e)}")
            return []
            
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