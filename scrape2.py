import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timezone
import time
import random

def scrape_wealthsimple_corporate_actions():
    """
    Scrape corporate actions from Wealthsimple and save to JSON
    """
    
    try:
        print("🚀 Starting scrape of Wealthsimple corporate actions...")
        
        # Target URL
        url = "https://help.wealthsimple.com/hc/en-ca/articles/4415455710363-Corporate-actions-tracker"
        
        # Try multiple approaches
        approaches = [
            ("Standard requests", try_standard_requests),
            ("Cloudscraper", try_cloudscraper),
            ("Selenium", try_selenium)
        ]
        
        for approach_name, approach_func in approaches:
            print(f"\n🔄 Trying {approach_name}...")
            try:
                result = approach_func(url)
                if result and result.get('metadata', {}).get('status') in ['success', 'success_alternative']:
                    print(f"✅ {approach_name} succeeded!")
                    return result
            except Exception as e:
                print(f"❌ {approach_name} failed: {str(e)}")
                continue
        
        # If all approaches fail, create mock data
        print("⚠️  All scraping approaches failed. Creating mock data for testing...")
        return create_mock_data()
        
    except Exception as e:
        print(f"❌ Scraping failed: {str(e)}")
        return create_error_response(str(e))

def try_standard_requests(url):
    """
    Try standard requests with improved headers
    """
    session = requests.Session()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }
    
    session.headers.update(headers)
    time.sleep(random.uniform(1, 3))
    
    response = session.get(url, timeout=30)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        actions = extract_corporate_actions(soup)
        
        return create_success_response(actions, url, "standard_requests")
    else:
        raise Exception(f"HTTP {response.status_code}")

def try_cloudscraper(url):
    """
    Try using cloudscraper to bypass Cloudflare protection
    """
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(delay=10, browser='chrome')
        response = scraper.get(url)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            actions = extract_corporate_actions(soup)
            return create_success_response(actions, url, "cloudscraper")
        else:
            raise Exception(f"HTTP {response.status_code}")
    except ImportError:
        raise Exception("cloudscraper not installed")
    except Exception as e:
        raise Exception(f"Cloudscraper failed: {str(e)}")

def try_selenium(url):
    """
    Try using Selenium with a headless browser
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Wait a bit more for dynamic content
            time.sleep(5)
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            actions = extract_corporate_actions(soup)
            
            return create_success_response(actions, url, "selenium")
            
        finally:
            driver.quit()
            
    except ImportError:
        raise Exception("selenium not installed")
    except Exception as e:
        raise Exception(f"Selenium failed: {str(e)}")

def create_success_response(actions, url, method):
    """
    Create a success response with the scraped data
    """
    output_data = {
        "corporate_actions": actions,
        "metadata": {
            "total_actions": len(actions),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_url": url,
            "scraper_version": "1.2",
            "status": "success" if actions else "no_data_found",
            "method_used": method
        }
    }
    
    # Save to JSON file
    output_filename = 'corporate_actions.json'
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Successfully scraped {len(actions)} corporate actions")
    print(f"💾 Data saved to: {output_filename}")
    
    # Print first few actions for verification
    if actions:
        print(f"\n📋 Sample of scraped actions:")
        for i, action in enumerate(actions[:3]):
            print(f"  {i+1}. {action['company']} ({action['ticker']}) - {action['action_type']}")
    
    return output_data

def create_error_response(error_message):
    """
    Create an error response
    """
    error_data = {
        "corporate_actions": [],
        "metadata": {
            "total_actions": 0,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_url": "https://help.wealthsimple.com/hc/en-ca/articles/4415455710363-Corporate-actions-tracker",
            "scraper_version": "1.2",
            "status": "error",
            "error_message": error_message
        }
    }
    
    # Save error state
    with open('corporate_actions.json', 'w', encoding='utf-8') as f:
        json.dump(error_data, f, indent=2)
    
    return error_data

def create_mock_data():
    """
    Create mock corporate actions data for testing when scraping fails
    """
    mock_actions = [
        {
            "date": "2024-01-15",
            "company": "Example Corp",
            "ticker": "EXAM",
            "action_type": "Stock Split",
            "ratio": "2:1",
            "action_details": "Example Corp (EXAM) performed a 2:1 stock split"
        },
        {
            "date": "2024-01-10",
            "company": "Test Industries",
            "ticker": "TEST",
            "action_type": "Consolidation",
            "ratio": "1:8",
            "action_details": "Test Industries (TEST) performed a 1:8 consolidation"
        }
    ]
    
    output_data = {
        "corporate_actions": mock_actions,
        "metadata": {
            "total_actions": len(mock_actions),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_url": "https://help.wealthsimple.com/hc/en-ca/articles/4415455710363-Corporate-actions-tracker",
            "scraper_version": "1.2",
            "status": "mock_data",
            "note": "This is mock data created because scraping failed"
        }
    }
    
    with open('corporate_actions.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    return output_data

def extract_corporate_actions(soup):
    """
    Extract corporate actions from the parsed HTML
    """
    actions = []
    
    print("🔍 Looking for date headers...")
    
    # Find all h3 headers that might contain dates
    headers = soup.find_all('h3')
    print(f"Found {len(headers)} h3 headers")
    
    for i, header in enumerate(headers):
        header_text = header.get_text().strip()
        print(f"  Header {i+1}: {header_text[:50]}...")
        
        # Check if this header contains a date
        date_match = re.search(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', 
            header_text, 
            re.IGNORECASE
        )
        
        if date_match:
            date_str = date_match.group(0)
            normalized_date = normalize_date(date_str)
            print(f"    ✅ Found date: {date_str} -> {normalized_date}")
            
            # Look for the next ul or ol after this header
            next_list = header.find_next_sibling(['ul', 'ol'])
            
            # If not immediately after, look within next few siblings
            if not next_list:
                sibling = header.next_sibling
                for _ in range(5):  # Check next 5 siblings
                    if sibling and hasattr(sibling, 'find'):
                        next_list = sibling.find(['ul', 'ol'])
                        if next_list:
                            break
                        sibling = sibling.next_sibling
                    elif sibling:
                        sibling = sibling.next_sibling
                    else:
                        break
            
            if next_list:
                list_items = next_list.find_all('li')
                print(f"    📝 Found {len(list_items)} list items")
                
                for li in list_items:
                    action_text = li.get_text().strip()
                    
                    if len(action_text) > 30:  # Only process substantial entries
                        action = parse_single_action(action_text, normalized_date)
                        if action:
                            actions.append(action)
                            print(f"      ✓ {action['company']} ({action['ticker']}) - {action['action_type']}")
            else:
                print(f"    ❌ No list found after date header")
    
    print(f"\n📊 Total actions extracted: {len(actions)}")
    return actions

def parse_single_action(text, date):
    """
    Parse a single corporate action from text
    """
    # Clean up the text
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Extract company and ticker - handle various formats
    patterns = [
        r'^([^(]+?)\s*\(\s*([A-Z]{1,6}(?:\.[A-Z])?)\s*\)\s*performed',  # Standard format
        r'^([^(]+?)\s*\([^)]*\)\s*\(\s*([A-Z]{1,6}(?:\.[A-Z])?)\s*\)\s*performed',  # With class designation
        r'^([^(]+?)\s*\(\s*([A-Z]{1,6}(?:\.[A-Z])?)\s*\)',  # Just company (ticker)
    ]
    
    company = ""
    ticker = ""
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            company = match.group(1).strip()
            ticker = match.group(2).strip()
            break
    
    # Fallback: try to find any ticker in parentheses
    if not ticker:
        ticker_match = re.search(r'\(([A-Z]{1,6}(?:\.[A-Z])?)\)', text)
        if ticker_match:
            ticker = ticker_match.group(1)
    
    # Fallback: extract company name before "performed"
    if not company:
        company_match = re.search(r'^([^(]+?)(?:\s+performed|\s+\()', text)
        if company_match:
            company = company_match.group(1).strip()
    
    # Clean up company name
    if company:
        # Remove common suffixes and extra parenthetical info
        company = re.sub(r'\s*\([^)]*\)\s*$', '', company)
        company = re.sub(r'\s+(Inc|Corp|Ltd|LLC|Co)\.*$', r' \1', company)
        company = company.strip()
    
    # Classify action type
    action_type = classify_action_type(text)
    
    # Extract ratio
    ratio = extract_ratio(text)
    
    # Only return if we have minimum required info
    if company and len(company) > 1:
        return {
            "date": date,
            "company": company,
            "ticker": ticker,
            "action_type": action_type,
            "ratio": ratio,
            "action_details": text
        }
    
    return None

def classify_action_type(text):
    """
    Determine the type of corporate action
    """
    text_lower = text.lower()
    
    # Use word boundaries for better matching
    if re.search(r'\bconsolidation\b', text_lower):
        return 'Consolidation'
    elif re.search(r'\b(?:stock\s+split|forward\s+split)\b', text_lower):
        return 'Stock Split'
    elif re.search(r'\bmerger\b', text_lower):
        return 'Merger'
    elif re.search(r'\b(?:spin-?off|spinoff)\b', text_lower):
        return 'Spinoff'
    elif re.search(r'\bdividend\b', text_lower):
        return 'Dividend'
    elif re.search(r'\bacquisition\b', text_lower):
        return 'Acquisition'
    elif re.search(r'\bliquidation\b', text_lower):
        return 'Liquidation'
    elif re.search(r'\brights\s+distribution\b', text_lower):
        return 'Rights Distribution'
    elif re.search(r'\breorganization\b', text_lower):
        return 'Reorganization'
    elif re.search(r'\bredemption\b', text_lower):
        return 'Redemption'
    elif re.search(r'\bname\s+change\b', text_lower):
        return 'Name Change'
    else:
        return 'Other'

def extract_ratio(text):
    """
    Extract ratio information from action text
    """
    # Multiple ratio patterns
    patterns = [
        r'(\d+(?:\.\d+)?\s+for\s+\d+(?:\.\d+)?)',           # "1 for 8"
        r'(\d+(?:\.\d+)?:\d+(?:\.\d+)?)',                    # "1:8"
        r'(\d+(?:\.\d+)?\s*-\s*for\s*-\s*\d+(?:\.\d+)?)',   # "1-for-8"
        r'(\d+(?:\.\d+)?\s+to\s+\d+(?:\.\d+)?)',            # "1 to 8"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    return ""

def normalize_date(date_str):
    """
    Convert various date formats to YYYY-MM-DD
    """
    # Remove ordinal suffixes
    clean_date = re.sub(r'(\d+)(?:st|nd|rd|th)\b', r'\1', date_str)
    
    # Month mapping
    months = {
        'january': '01', 'february': '02', 'march': '03', 'april': '04',
        'may': '05', 'june': '06', 'july': '07', 'august': '08',
        'september': '09', 'october': '10', 'november': '11', 'december': '12'
    }
    
    # Parse common date formats
    date_patterns = [
        r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',     # "July 10, 2025"
        r'(\w+)\s+(\d{1,2})\s+(\d{4})',       # "July 10 2025"
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, clean_date, re.IGNORECASE)
        if match:
            month_name = match.group(1).lower()
            day = match.group(2).zfill(2)
            year = match.group(3)
            
            if month_name in months:
                return f"{year}-{months[month_name]}-{day}"
    
    return date_str  # Return original if parsing fails

if __name__ == "__main__":
    print("🤖 Wealthsimple Corporate Actions Scraper")
    print("=" * 50)
    
    try:
        result = scrape_wealthsimple_corporate_actions()
        
        if result and result['metadata']['status'] == 'success':
            print(f"\n🎉 SUCCESS!")
            print(f"📊 Scraped {result['metadata']['total_actions']} corporate actions")
            print(f"💾 Data saved to corporate_actions.json")
        else:
            print(f"\n⚠️  WARNING: Scraping completed but no data found")
            
    except Exception as e:
        print(f"\n💥 FAILED: {str(e)}")
        exit(1)