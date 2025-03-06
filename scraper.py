from bs4 import BeautifulSoup, Tag
import requests
from typing import Dict, List, Optional, Tuple
import re
from urllib.parse import urljoin, urlparse
import json
import time
import streamlit as st

class EnhancedWebsiteScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.relevant_paths = [
            '/about', '/contact', '/team', '/our-story', '/meet-the-team',
            '/about-us', '/contact-us', '/leadership', '/management'
        ]
        self.important_tags = {
            'header_tags': ['h1', 'h2', 'h3', 'h4', 'h5', 'h6'],
            'content_tags': ['p', 'article', 'section', 'div'],
            'list_tags': ['ul', 'ol', 'li'],
            'emphasis_tags': ['strong', 'em', 'b', 'i'],
            'link_tags': ['a'],
            'meta_tags': ['meta', 'title']
        }
        self.important_classes = [
            'about', 'contact', 'team', 'bio', 'profile', 'person',
            'founder', 'ceo', 'owner', 'management', 'leadership',
            'company', 'mission', 'vision', 'values', 'history'
        ]

    def extract_meta_tags(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract metadata from head section"""
        metadata = {}
        
        # Get meta tags
        for meta in soup.find_all('meta'):
            name = meta.get('name', meta.get('property', ''))
            content = meta.get('content', '')
            if name and content:
                metadata[name] = content
        
        # Get title
        title_tag = soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.text.strip()
            
        return metadata

    def extract_schema_data(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract schema.org structured data"""
        schema_data = []
        
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    schema_data.append(data)
                elif isinstance(data, list):
                    schema_data.extend(data)
            except:
                continue
                
        return schema_data

    def clean_text(self, text: str) -> str:
        """Clean text while preserving important whitespace"""
        # Remove extra whitespace while preserving paragraph breaks
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()

    def get_element_context(self, element: Tag) -> Dict[str, str]:
        """Get contextual information about an element"""
        context = {
            'tag': element.name,
            'classes': ' '.join(element.get('class', [])),
            'id': element.get('id', ''),
            'parent_tag': element.parent.name if element.parent else '',
            'parent_classes': ' '.join(element.parent.get('class', [])) if element.parent else ''
        }
        
        # Get nearest header
        header = element.find_previous(self.important_tags['header_tags'])
        if header:
            context['nearest_header'] = header.text.strip()
            
        return context

    def process_element(self, element: Tag, section_type: str = 'general') -> Optional[Dict]:
        """Process a single HTML element with context"""
        # Skip empty elements
        if not element.text.strip():
            return None
            
        # Get element text and context
        text = self.clean_text(element.text)
        context = self.get_element_context(element)
        
        # Score the relevance of this element
        relevance_score = 0
        
        # Check classes for relevance
        element_classes = ' '.join(element.get('class', [])).lower()
        for important_class in self.important_classes:
            if important_class in element_classes:
                relevance_score += 2
                
        # Check content for relevant keywords
        text_lower = text.lower()
        relevant_keywords = {
            'about': ['about', 'history', 'story', 'mission', 'vision', 'values'],
            'team': ['founder', 'ceo', 'owner', 'team', 'leadership', 'management'],
            'contact': ['contact', 'email', 'phone', 'address', 'reach']
        }
        
        for keyword in relevant_keywords.get(section_type, []):
            if keyword in text_lower:
                relevance_score += 1
                
        # Only return if the element has some relevance
        if relevance_score > 0:
            return {
                'text': text,
                'context': context,
                'relevance_score': relevance_score,
                'html': str(element)  # Preserve original HTML
            }
            
        return None

    def extract_content_with_context(self, soup: BeautifulSoup, section_type: str = 'general') -> List[Dict]:
        """Extract content while preserving structure and context"""
        content_elements = []
        
        # Process headers first
        for tag in soup.find_all(self.important_tags['header_tags']):
            processed = self.process_element(tag, section_type)
            if processed:
                processed['type'] = 'header'
                content_elements.append(processed)
        
        # Process main content
        for tag in soup.find_all(self.important_tags['content_tags']):
            processed = self.process_element(tag, section_type)
            if processed:
                processed['type'] = 'content'
                content_elements.append(processed)
        
        # Sort by relevance score
        return sorted(content_elements, key=lambda x: x['relevance_score'], reverse=True)

    def format_for_llm(self, content_elements: List[Dict], metadata: Dict, schema_data: List[Dict]) -> str:
        """Format the extracted content for LLM analysis"""
        formatted_content = []
        
        # Add metadata section
        if metadata:
            formatted_content.append("### Page Metadata ###")
            for key, value in metadata.items():
                formatted_content.append(f"{key}: {value}")
            formatted_content.append("\n")
        
        # Add schema.org data
        if schema_data:
            formatted_content.append("### Structured Data ###")
            formatted_content.append(json.dumps(schema_data, indent=2))
            formatted_content.append("\n")
        
        # Add main content
        formatted_content.append("### Main Content ###")
        for element in content_elements:
            # Add context information
            formatted_content.append(f"\nElement Type: {element['type']}")
            formatted_content.append(f"Context: {json.dumps(element['context'], indent=2)}")
            formatted_content.append("Content:")
            formatted_content.append(element['text'])
            formatted_content.append("-" * 50)
        
        return "\n".join(formatted_content)

    def scrape_website(self, url: str) -> Dict:
        """Enhanced website scraping with context preservation"""
        try:
            if not url or url == 'N/A':
                return {'success': False, 'error': 'Invalid URL'}

            collected_content = []
            scraped_urls = set()
            schema_data = []

            # Clean URL
            url = url if url.startswith(('http://', 'https://')) else f'https://{url}'
            base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"

            # Scrape main page
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract metadata and schema.org data
            metadata = self.extract_meta_tags(soup)
            schema_data.extend(self.extract_schema_data(soup))
            
            # Process main page content
            main_content = self.extract_content_with_context(soup)
            collected_content.extend(main_content)
            scraped_urls.add(url)

            # Find and scrape relevant internal pages
            internal_links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if any(path in href.lower() for path in self.relevant_paths):
                    full_url = urljoin(base_url, href)
                    if urlparse(full_url).netloc == urlparse(base_url).netloc:
                        internal_links.append(full_url)

            # Scrape additional pages
            for link in internal_links[:3]:  # Limit to 3 additional pages
                if link not in scraped_urls:
                    try:
                        response = requests.get(link, headers=self.headers, timeout=10)
                        if response.ok:
                            page_soup = BeautifulSoup(response.text, 'html.parser')
                            
                            # Determine section type from URL
                            section_type = 'about' if 'about' in link.lower() else 'contact' if 'contact' in link.lower() else 'team'
                            
                            # Extract content with context
                            page_content = self.extract_content_with_context(page_soup, section_type)
                            collected_content.extend(page_content)
                            
                            # Get additional schema data
                            schema_data.extend(self.extract_schema_data(page_soup))
                            
                            scraped_urls.add(link)
                            time.sleep(1)  # Rate limiting
                    except Exception as e:
                        st.warning(f"Error scraping {link}: {str(e)}")

            # Format everything for LLM analysis
            formatted_content = self.format_for_llm(collected_content, metadata, schema_data)

            return {
                'success': True,
                'content': formatted_content,
                'structured_data': schema_data,
                'metadata': metadata,
                'scraped_urls': list(scraped_urls)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}