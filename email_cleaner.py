# email_cleaner.py
from typing import List, Dict, Set
import re
import json
import streamlit as st
from openai import OpenAI

class EmailCleaner:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Missing OpenAI API Key")
        self.client = OpenAI(api_key=api_key)
        self.common_email_errors = {
            'Get': '',
            'Email:': '',
            'mailto:': '',
            'email:': '',
            'Email': '',
            '[dot]': '.',
            '[at]': '@',
            ' at ': '@',
            ' dot ': '.',
            'e-mail:': '',
            '<': '',
            '>': '',
            '"': '',
            "'": '',
            '\\': '',
            ' ': '',
            '(': '',
            ')': '',
        }

    def basic_clean_emails(self, emails_str: str) -> List[str]:
        """Basic cleaning of email strings without LLM"""
        if not emails_str or emails_str.strip() == '':
            return []
            
        # Split by common separators
        email_list = re.split(r'[;,\s]+', emails_str)
        
        # Clean each email
        cleaned_emails = []
        for email in email_list:
            # Skip empty items
            if not email.strip():
                continue
                
            # Apply common fixes
            clean_email = email.strip().lower()
            for error, replacement in self.common_email_errors.items():
                clean_email = clean_email.replace(error, replacement)
            
            # Verify it looks like an email (basic check)
            if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', clean_email):
                cleaned_emails.append(clean_email)
                
        # Remove duplicates while preserving order
        seen = set()
        unique_emails = [x for x in cleaned_emails if not (x in seen or seen.add(x))]
        
        return unique_emails

    def llm_clean_emails(self, emails_str: str) -> List[str]:
        """Use LLM to clean and extract valid emails from a messy string"""
        # First do basic cleaning to handle common cases
        basic_cleaned = self.basic_clean_emails(emails_str)
        
        # For simple cases that are already clean, just return them
        if len(basic_cleaned) <= 3 and all('@' in email for email in basic_cleaned):
            return basic_cleaned
            
        # For more complex cases, use LLM
        try:
            messages = [
                {
                    "role": "system",
                    "content": """Extract valid email addresses from the provided text. 
Clean up any formatting issues and remove duplicates.
The final output should be a JSON array containing only valid email addresses:
["email1@example.com", "email2@example.com"]

Rules for cleaning:
1. Remove any text that's not part of an email address like "Email:", "Get", etc.
2. Fix common obfuscation patterns ([at] → @, [dot] → .)
3. Remove duplicates (case-insensitive)
4. Ensure all emails follow the standard format: username@domain.tld"""
                },
                {
                    "role": "user",
                    "content": f"Clean and extract email addresses from this text: {emails_str}"
                }
            ]

            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=0,
                max_tokens=150,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            if "emails" in result:
                return result["emails"]
            else:
                # Try to find an array in the response
                for key, value in result.items():
                    if isinstance(value, list):
                        return value
                
                # If we can't find an array, use basic cleaned results
                return basic_cleaned if basic_cleaned else []
                
        except Exception as e:
            st.warning(f"Error in LLM email cleaning: {str(e)}. Using basic cleaning instead.")
            return basic_cleaned

    def batch_clean_emails(self, lead_data: List[Dict], 
                           email_column: str = 'discovered_emails', 
                           potential_column: str = 'potential_emails') -> List[Dict]:
        """Batch clean emails for multiple leads"""
        cleaned_data = []
        
        for lead in lead_data:
            # Copy the lead data
            cleaned_lead = lead.copy()
            
            # Clean discovered emails
            if email_column in lead and lead[email_column]:
                cleaned_emails = self.llm_clean_emails(lead[email_column])
                cleaned_lead[email_column] = '; '.join(cleaned_emails)
            
            # Clean potential emails if present
            if potential_column in lead and lead[potential_column]:
                cleaned_potential = self.llm_clean_emails(lead[potential_column])
                cleaned_lead[potential_column] = '; '.join(cleaned_potential)
                
            cleaned_data.append(cleaned_lead)
            
        return cleaned_data
        
    def verify_email_format(self, email: str) -> bool:
        """Verify that a string follows standard email format"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, email))
        
    def extract_domains(self, emails: List[str]) -> List[str]:
        """Extract unique domains from a list of emails"""
        domains = set()
        for email in emails:
            parts = email.split('@')
            if len(parts) == 2:
                domains.add(parts[1])
        return list(domains)