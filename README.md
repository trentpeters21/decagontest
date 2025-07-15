# Wealthsimple Corporate Actions Scraper

A robust Python scraper that extracts corporate actions data from Wealthsimple's corporate actions tracker page. The scraper can bypass Cloudflare protection and provides comprehensive analysis of the scraped data.

## Features

- **Multi-method scraping**: Uses standard requests, cloudscraper, and Selenium to handle different anti-bot protections
- **Comprehensive data extraction**: Extracts company names, tickers, action types, ratios, and detailed descriptions
- **Data analysis**: Built-in analysis tools with visualizations and statistics
- **Multiple output formats**: JSON, CSV, and visual charts
- **Error handling**: Graceful fallbacks and detailed error reporting

## Installation

1. Clone or download the project files
2. Install required dependencies:

```bash
pip3 install requests beautifulsoup4 cloudscraper selenium pandas matplotlib
```

## Usage

### Basic Scraping

```bash
python3 scrape_corporate_actions.py
```

This will:
- Scrape corporate actions from Wealthsimple
- Save data to `corporate_actions.json`
- Display progress and results

### Data Analysis

```bash
python3 analyze_corporate_actions.py
```

This will:
- Load the scraped data
- Generate comprehensive statistics
- Create visualizations (`corporate_actions_analysis.png`)
- Export data to CSV (`corporate_actions.csv`)

## Data Structure

The scraper extracts the following information for each corporate action:

```json
{
  "date": "2025-07-15",
  "company": "United",
  "ticker": "UNC",
  "action_type": "Stock Split",
  "ratio": "10 for 1",
  "action_details": "United (UNC) performed a 10 for 1 stock split..."
}
```

### Action Types Supported

- **Consolidation**: Reverse stock splits
- **Stock Split**: Forward stock splits
- **Merger**: Company mergers and acquisitions
- **Liquidation**: Fund liquidations
- **Name Change**: Company name changes
- **Spinoff**: Corporate spinoffs
- **Redemption**: Security redemptions
- **Reorganization**: Corporate reorganizations
- **Acquisition**: Company acquisitions
- **Dividend**: Dividend distributions
- **Rights Distribution**: Rights offerings
- **Other**: Miscellaneous corporate actions

## Sample Analysis Results

From the latest scrape (1,787 actions):

- **Most common action**: Consolidation (48.7%)
- **Date range**: June 2024 - July 2025 (407 days)
- **Most active company**: Wheeler Real Estate Investment Trust Inc (6 actions)
- **Most common ratio**: 1 for 10 (188 times)
- **Stock splits vs consolidations**: 1:8.3 ratio

## Files Generated

- `corporate_actions.json`: Raw scraped data in JSON format
- `corporate_actions.csv`: Data exported to CSV for spreadsheet analysis
- `corporate_actions_analysis.png`: Visual charts and graphs
- `README.md`: This documentation file

## Technical Details

### Scraping Methods

1. **Standard Requests**: Basic HTTP requests with realistic headers
2. **Cloudscraper**: Specialized library for bypassing Cloudflare protection
3. **Selenium**: Headless browser automation for complex JavaScript-heavy pages

### Error Handling

- Automatic fallback between scraping methods
- Detailed error logging and reporting
- Mock data generation for testing when scraping fails
- Graceful handling of network timeouts and rate limiting

### Performance

- Respectful scraping with random delays
- Session management for efficient requests
- Parallel processing capabilities for large datasets

## Troubleshooting

### Common Issues

1. **403 Forbidden Error**: The scraper automatically tries multiple methods to bypass this
2. **No data found**: Check if the source page structure has changed
3. **Selenium errors**: Ensure Chrome/Chromium is installed for headless browsing

### Dependencies

- Python 3.7+
- Chrome/Chromium browser (for Selenium)
- Internet connection for scraping

## Legal and Ethical Considerations

- This scraper is for educational and research purposes
- Respect the website's robots.txt and terms of service
- Use reasonable delays between requests
- Consider the impact on the target server

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve the scraper.

## License

This project is provided as-is for educational purposes. Please ensure compliance with applicable laws and website terms of service when using this tool. 