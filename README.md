
# BAFA Advisor Data Scraper

A robust Python web scraper designed to collect advisor data from the BAFA (Bundesamt für Wirtschaft und Ausfuhrkontrolle) portal. Built with modern Python practices using Scrapy, Pydantic, and Loguru.

## Features

- 🔄 Automated data extraction from BAFA portal
- ✅ Data validation using Pydantic V2
- 📊 Real-time progress tracking
- 📝 Comprehensive logging with Loguru
- 🔁 Retry mechanism with exponential backoff
- 🧪 Test mode for development
- 🐛 Debug mode with detailed logging

## Installation

### Prerequisites
- Python 3.11 or higher
- pip or uv package manager

### Setup

1. Clone the repository:
```bash
git clone [repository-url]
cd bafa-scraper
```

2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install scrapy pandas tqdm openpyxl loguru pydantic tenacity
```

## Usage

### Basic Usage
```bash
python bafa.py
```

### Available Options
- Test Mode (limited to 5 entries):
  ```bash
  python bafa.py --test
  ```

- Debug Mode (detailed logging):
  ```bash
  python bafa.py --debug
  ```

- Combined Mode:
  ```bash
  python bafa.py --test --debug
  ```

## Data Structure

### Collected Information
```python
{
    'Beratername': str,      # Advisor name
    'Beraterfirma': str,     # Company name
    'Strasse': str,          # Street address
    'PLZ': str,              # Postal code (validated)
    'Ort': str,             # City
    'Telefon': str,         # Phone number
    'Fax': str,             # Fax number
    'Email_Vorhanden': str, # Email presence (Ja/Nein)
    'Email_Image_ID': str,  # Email image reference
    'Website': str,         # Company website
    'BFEE_ID': str,        # BFEE identifier
    'Detail_URL': str       # Source URL
}
```

## Output Files

### Directory Structure
```
project/
├── logs/
│   ├── bafa_spider_[timestamp].log
│   └── errors_[timestamp].log
└── output/
    └── bafa_results_[mode]_[timestamp].xlsx
```

### Log Files
- Main log: `logs/bafa_spider_[timestamp].log`
  - Rotation: 500 MB
  - Retention: 10 days
  - Format: `{time} | {level} | {module}:{function}:{line} | {message}`

- Error log: `logs/errors_[timestamp].log`
  - Contains detailed error information
  - Created only when errors occur

### Data Output
- Format: Excel (.xlsx)
- Location: `output/bafa_results_[mode]_[timestamp].xlsx`
- Sheet name: 'BAFA_Advisors'

## Configuration

### Spider Settings
```python
{
    'CONCURRENT_REQUESTS': 32,
    'DOWNLOAD_DELAY': 0.25,
    'RETRY_TIMES': 3,
    'DOWNLOAD_TIMEOUT': 15
}
```

### Retry Mechanism
- Maximum attempts: 3
- Exponential backoff: 4-10 seconds
- Automatic retry on failure

## Error Handling

The scraper implements comprehensive error handling:
- Network request retries
- Data validation
- File operations
- Progress tracking
- Detailed error logging

## Statistics

After each run, the scraper provides detailed statistics:
- Total records collected
- Entries with email
- Entries with website
- Unique cities
- Failed items
- Success rate

## Development

### Running Tests
Use test mode for development:
```bash
python bafa.py --test --debug
```

### Code Structure
- `SpiderConfig`: Configuration management
- `AdvisorData`: Data validation model
- `ProgressStatsCollector`: Progress tracking
- `BAFASpider`: Main scraping logic



## Disclaimer

This tool is for educational purposes. Ensure compliance with BAFA's terms of service when using this scraper.
```

This README provides:
- Clear installation and usage instructions
- Detailed feature documentation
- Complete configuration options
- Directory structure explanation
- Error handling information
- Statistics overview
- Development guidelines

