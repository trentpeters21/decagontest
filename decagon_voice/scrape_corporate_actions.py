import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timezone
import time
import random
import os

def scrape_wealthsimple_corporate_actions():
    """
    Scrape corporate actions from Wealthsimple and save to JSON
    """
    
    try:
        print("🚀 Starting scrape of Wealthsimple corporate actions...")
        
        # Target URL
        url = "https://help.wealthsimple.com/hc/en-ca/articles/4415455710363-Corporate-actions-tracker"
        
        # Try multiple approaches including Selenium
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
    Try standard requests with improved headers and session handling
    """
    session = requests.Session()
    
    # Enhanced headers to mimic real browser
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
        'Pragma': 'no-cache'
    }
    
    session.headers.update(headers)
    
    # Random delay to avoid being flagged as bot
    time.sleep(random.uniform(2, 5))
    
    print("📡 Making request with standard approach...")
    response = session.get(url, timeout=30)
    
    print(f"📊 Response status: {response.status_code}")
    print(f"📏 Response length: {len(response.text)} characters")
    
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
        
        print("🌩️ Initializing cloudscraper...")
        scraper = cloudscraper.create_scraper(
            delay=10,
            browser={
                'browser': 'chrome',
                'platform': 'darwin',  # macOS
                'desktop': True
            }
        )
        
        # Add some additional headers
        scraper.headers.update({
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        
        print("📡 Making request with cloudscraper...")
        response = scraper.get(url, timeout=30)
        
        print(f"📊 Cloudscraper response status: {response.status_code}")
        print(f"📏 Response length: {len(response.text)} characters")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            actions = extract_corporate_actions(soup)
            return create_success_response(actions, url, "cloudscraper")
        else:
            raise Exception(f"HTTP {response.status_code}")
            
    except ImportError:
        raise Exception("cloudscraper not installed - install with: pip install cloudscraper")
    except Exception as e:
        raise Exception(f"Cloudscraper failed: {str(e)}")

def try_selenium(url):
    """
    Try using Selenium with headless Chrome (GitHub Actions compatible)
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException, WebDriverException
        
        print("🌐 Initializing Selenium Chrome driver...")
        
        chrome_options = Options()
        
        # Essential headless options
        chrome_options.add_argument("--headless=new")  # Use new headless mode
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--disable-features=VizDisplayCompositor")
        
        # Window and display settings
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-javascript")  # Try without JS first
        
        # Performance optimizations
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=4096")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        
        # User agent
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Additional stability options for GitHub Actions
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-sync")
        chrome_options.add_argument("--disable-translate")
        chrome_options.add_argument("--hide-scrollbars")
        chrome_options.add_argument("--metrics-recording-only")
        chrome_options.add_argument("--mute-audio")
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--safebrowsing-disable-auto-update")
        chrome_options.add_argument("--ignore-ssl-errors")
        chrome_options.add_argument("--ignore-certificate-errors")
        
        # Set up Chrome service
        service = None
        
        # Try to detect if we're in GitHub Actions
        if os.environ.get('GITHUB_ACTIONS'):
            print("🔧 Detected GitHub Actions environment")
            # In GitHub Actions, Chrome and ChromeDriver should be available in PATH
            service = Service()  # Will use chromedriver in PATH
        else:
            print("🔧 Detected local environment")
            # For local development, you might need to specify the path
            try:
                service = Service()  # Try default first
            except Exception:
                # If default fails, you might need to specify the path
                service = Service('/usr/local/bin/chromedriver')
        
        # Initialize the driver
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            print(f"📡 Navigating to {url}...")
            driver.get(url)
            
            # Wait for basic page load
            print("⏳ Waiting for page to load...")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Additional wait for dynamic content
            print("⏳ Waiting for dynamic content...")
            time.sleep(8)
            
            # Try to wait for specific content that indicates the page is loaded
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h3"))
                )
                print("✅ Found h3 elements, page seems loaded")
            except TimeoutException:
                print("⚠️  Timeout waiting for h3 elements, proceeding anyway...")
            
            # Get page source
            page_source = driver.page_source
            print(f"📄 Retrieved page source ({len(page_source)} characters)")
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(page_source, 'html.parser')
            actions = extract_corporate_actions(soup)
            
            return create_success_response(actions, url, "selenium")
            
        finally:
            print("🔒 Closing Chrome driver...")
            driver.quit()
            
    except ImportError:
        raise Exception("selenium not installed - install with: pip install selenium")
    except WebDriverException as e:
        raise Exception(f"WebDriver error: {str(e)}")
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
            "scraper_version": "1.4",
            "status": "success" if actions else "no_data_found",
            "method_used": method,
            "github_actions_compatible": True
        }
    }
    
    # Save to JSON file
    output_filename = 'corporate_actions.json'
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Successfully scraped {len(actions)} corporate actions using {method}")
    print(f"💾 Data saved to: {output_filename}")
    
    # Print first few actions for verification
    if actions:
        print(f"\n📋 Sample of scraped actions:")
        for i, action in enumerate(actions[:3]):
            print(f"  {i+1}. {action['company']} ({action['ticker']}) - {action['action_type']}")
    else:
        print("⚠️  No corporate actions found - page structure may have changed")
    
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
            "scraper_version": "1.4",
            "status": "error",
            "error_message": error_message,
            "github_actions_compatible": True
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
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    mock_actions = [
        {
            "date": "2025-01-15",
            "company": "Example Corp",
            "ticker": "EXAM",
            "action_type": "Stock Split",
            "ratio": "2:1",
            "action_details": "Example Corp (EXAM) performed a 2 for 1 stock split. Holders will now hold 2 shares for every 1 share previously held."
        },
        {
            "date": "2025-01-10",
            "company": "Test Industries Ltd",
            "ticker": "TEST",
            "action_type": "Consolidation",
            "ratio": "1:8",
            "action_details": "Test Industries Ltd (TEST) performed a 1 for 8 consolidation. Holders will now hold 1 share for every 8 shares previously held."
        },
        {
            "date": "2025-01-08",
            "company": "Mock Financial Inc",
            "ticker": "MOCK",
            "action_type": "Merger",
            "ratio": "",
            "action_details": "Mock Financial Inc (MOCK) performed a merger. Details of the event are still to be determined."
        }
    ]
    
    output_data = {
        "corporate_actions": mock_actions,
        "metadata": {
            "total_actions": len(mock_actions),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_url": "https://help.wealthsimple.com/hc/en-ca/articles/4415455710363-Corporate-actions-tracker",
            "scraper_version": "1.4",
            "status": "mock_data",
            "method_used": "fallback_mock_data",
            "note": "This is mock data created because all scraping methods failed",
            "github_actions_compatible": True
        }
    }
    
    with open('corporate_actions.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"🎭 Created mock data with {len(mock_actions)} sample actions")
    return output_data

def extract_corporate_actions(soup):
    """
    Extract corporate actions from the parsed HTML
    """
    actions = []
    
    print("🔍 Analyzing page structure...")
    
    # Debug info about page structure
    page_title = soup.title.string if soup.title else "No title found"
    print(f"📄 Page title: {page_title}")
    
    headers = soup.find_all('h3')
    print(f"📋 Found {len(headers)} h3 headers")
    
    lists = soup.find_all(['ul', 'ol'])
    print(f"📝 Found {len(lists)} lists")
    
    # Look for date headers
    date_headers_found = 0
    
    for i, header in enumerate(headers):
        header_text = header.get_text().strip()
        print(f"  Header {i+1}: {header_text[:60]}{'...' if len(header_text) > 60 else ''}")
        
        # Check if this header contains a date
        date_match = re.search(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', 
            header_text, 
            re.IGNORECASE
        )
        
        if date_match:
            date_headers_found += 1
            date_str = date_match.group(0)
            normalized_date = normalize_date(date_str)
            print(f"    ✅ Found date: {date_str} -> {normalized_date}")
            
            # Look for the next ul or ol after this header
            next_list = header.find_next_sibling(['ul', 'ol'])
            
            # If not immediately after, look within next few siblings
            if not next_list:
                sibling = header.next_sibling
                for _ in range(10):  # Check next 10 siblings
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
                print(f"    📝 Found {len(list_items)} list items for this date")
                
                for j, li in enumerate(list_items):
                    action_text = li.get_text().strip()
                    
                    if len(action_text) > 30:  # Only process substantial entries
                        action = parse_single_action(action_text, normalized_date)
                        if action:
                            actions.append(action)
                            print(f"      ✓ Action {j+1}: {action['company']} ({action['ticker']}) - {action['action_type']}")
                        else:
                            print(f"      ❌ Failed to parse: {action_text[:50]}...")
            else:
                print(f"    ❌ No list found after date header")
    
    print(f"\n📊 Summary:")
    print(f"  - Date headers found: {date_headers_found}")
    print(f"  - Total actions extracted: {len(actions)}")
    
    # If no actions found, provide debug info
    if not actions:
        print("\n🔍 Debug: No actions found. Sample page content:")
        sample_text = soup.get_text()[:1000] if soup.get_text() else "No text content"
        print(f"First 1000 characters: {sample_text}")
    
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
    print("🤖 Wealthsimple Corporate Actions Scraper v1.4 (with Selenium)")
    print("=" * 65)
    
    try:
        result = scrape_wealthsimple_corporate_actions()
        
        if result:
            status = result['metadata']['status']
            count = result['metadata']['total_actions']
            method = result['metadata'].get('method_used', 'unknown')
            
            if status == 'success':
                print(f"\n🎉 SUCCESS!")
                print(f"📊 Scraped {count} corporate actions using {method}")
                print(f"💾 Data saved to corporate_actions.json")
            elif status == 'mock_data':
                print(f"\n🎭 FALLBACK TO MOCK DATA")
                print(f"📊 Created {count} sample actions")
                print(f"💾 Mock data saved to corporate_actions.json")
            else:
                print(f"\n⚠️  PARTIAL SUCCESS")
                print(f"📊 Status: {status}")
                print(f"📈 Actions found: {count}")
        else:
            print(f"\n💥 COMPLETE FAILURE - No data created")
            exit(1)
            
    except Exception as e:
        print(f"\n💥 FATAL ERROR: {str(e)}")
        exit(1)