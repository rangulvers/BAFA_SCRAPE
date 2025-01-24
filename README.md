# BAFA Advisor Data Scraper

A Python-based web scraper using Scrapy to collect advisor data from the BAFA (Bundesamt für Wirtschaft und Ausfuhrkontrolle) website.

## Features

- Scrapes advisor information including:
  - Name
  - Company
  - Address details
  - Contact information
  - Email presence
  - Website
  - BFEE ID
- Progress bar tracking
- Configurable test mode for development
- Debug logging capabilities
- Excel output with timestamp
- Error handling and retry mechanisms

## Requirements

```bash
python >= 3.11
scrapy >= 2.12.0
pandas
tqdm
openpyxl
```

## Installation

1. Clone the repository:
```bash
git clone [your-repo-url]
cd BAFA_SCRAPE
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
# Using pip
pip install scrapy pandas tqdm openpyxl

# Or using uv (recommended)
uv pip install scrapy pandas tqdm openpyxl
```

## Usage

The scraper can be run in different modes:

### Full Mode
Scrapes all available advisor entries:
```bash
python bafa.py
```

### Test Mode
Scrapes only 5 entries (useful for testing):
```bash
python bafa.py --test
```

### Debug Mode
Enables detailed logging:
```bash
python bafa.py --debug
```

### Test + Debug Mode
Combines test and debug modes:
```bash
python bafa.py --test --debug
```

## Output Files

- Data file: `bafa_results_[mode]_[timestamp].xlsx`
  - Contains all scraped advisor information
  - Mode indicates whether it was a test or full run
  - Timestamp format: YYYYMMDD_HHMMSS

- Debug log (when in debug mode): `bafa_spider_debug.log`
  - Contains detailed execution information
  - Includes errors and warnings
  - Useful for troubleshooting

## Data Structure

The scraped data includes the following fields:
- `Beratername`: Advisor name
- `Beraterfirma`: Company name
- `Strasse`: Street address
- `PLZ`: Postal code
- `Ort`: City
- `Telefon`: Phone number
- `Fax`: Fax number
- `Email_Vorhanden`: Email presence indicator (Ja/Nein)
- `Email_Image_ID`: Email image reference ID
- `Website`: Company website
- `BFEE_ID`: BFEE identifier
- `Detail_URL`: Source URL for detailed information

## Configuration

The spider's behavior can be customized through the `SpiderConfig` class:
- `test_mode`: Limits scraping to 5 entries
- `debug_mode`: Enables detailed logging
- `items_per_page`: Number of items per request
- `page`: Starting page number

## Error Handling

The scraper includes comprehensive error handling:
- Request retries (up to 3 times)
- Connection timeout handling
- Data extraction error recovery
- Progress tracking protection
- File operation error handling

## Logging

Logging levels are configured based on the mode:
- Normal mode: Only shows progress bar and critical errors
- Debug mode: Shows detailed information including:
  - Request details
  - Processing steps
  - Errors and warnings
  - Statistics

## Performance

The scraper is configured for optimal performance with:
- Concurrent requests: 32
- Download delay: 0.25 seconds
- Connection timeout: 15 seconds
- Retry enabled for failed requests
