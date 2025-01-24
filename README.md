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