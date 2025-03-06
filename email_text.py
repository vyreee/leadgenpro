# email_test.py
import streamlit as st
from email_cleaner import EmailCleaner
import os
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    default_openai_key = os.environ.get('OPENAI_API_KEY', '')
    
    st.set_page_config(page_title="Email Cleaner Test", layout="wide")
    st.title("🧹 Email Cleaner Test Tool")
    
    # API key input
    openai_api_key = st.text_input(
        "OpenAI API Key", 
        value=default_openai_key,
        type="password",
        help="Get your API key from https://platform.openai.com/api-keys"
    )
    
    if not openai_api_key:
        st.warning("Please enter your OpenAI API key to use this tool.")
        return
    
    # Initialize email cleaner
    try:
        cleaner = EmailCleaner(api_key=openai_api_key)
    except Exception as e:
        st.error(f"Failed to initialize EmailCleaner: {str(e)}")
        return
    
    # Input for emails to clean
    st.subheader("Clean Email Addresses")
    
    sample_input = """Eddielassonde@gmail.com; EddieLassonde@gmail.com; Eddielassonde@gmail.comGet;
    mailto:contact@example.com; Email: info@business.org; contact [at] domain [dot] com;
    support@company.com, sales@company.com"""
    
    emails_input = st.text_area(
        "Enter messy email addresses",
        value=sample_input,
        height=150,
        help="Enter multiple email addresses that may have formatting issues"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Clean with Basic Method"):
            if emails_input:
                with st.spinner("Cleaning emails..."):
                    cleaned = cleaner.basic_clean_emails(emails_input)
                    
                st.success(f"Found {len(cleaned)} valid email addresses")
                st.write("Basic cleaned emails:")
                for email in cleaned:
                    st.write(f"- {email}")
    
    with col2:
        if st.button("Clean with LLM (AI) Method"):
            if emails_input:
                with st.spinner("Cleaning emails using AI..."):
                    cleaned = cleaner.llm_clean_emails(emails_input)
                    
                st.success(f"Found {len(cleaned)} valid email addresses")
                st.write("LLM cleaned emails:")
                for email in cleaned:
                    st.write(f"- {email}")

    # Batch testing option
    st.subheader("Batch Test")
    
    batch_test = st.text_area(
        "Test multiple email strings (one per line)",
        height=150,
        placeholder="Enter one email string per line to test batch processing"
    )
    
    if st.button("Run Batch Test"):
        if batch_test:
            lines = batch_test.strip().split('\n')
            results = []
            
            with st.spinner(f"Processing {len(lines)} entries..."):
                for line in lines:
                    basic = cleaner.basic_clean_emails(line)
                    llm = cleaner.llm_clean_emails(line)
                    results.append({
                        'input': line,
                        'basic_cleaned': '; '.join(basic),
                        'llm_cleaned': '; '.join(llm),
                        'basic_count': len(basic),
                        'llm_count': len(llm)
                    })
            
            # Display results
            for i, result in enumerate(results):
                st.write(f"### Test {i+1}")
                st.write(f"**Input:** {result['input']}")
                st.write(f"**Basic ({result['basic_count']}):** {result['basic_cleaned']}")
                st.write(f"**LLM ({result['llm_count']}):** {result['llm_cleaned']}")
                st.write("---")

if __name__ == "__main__":
    main()