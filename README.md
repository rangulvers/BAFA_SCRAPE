# BAFA Advisor Scraper

A Python web scraper that collects information about energy advisors from the BAFA (Bundesamt für Wirtschaft und Ausfuhrkontrolle) portal.

## Description

This scraper collects detailed information about energy advisors listed on the BAFA portal, including their contact details, locations, and professional information. It processes both the main listing and individual detail pages for each advisor.

## Features

- Scrapes advisor information from the BAFA portal
- Handles pagination and detail pages automatically
- Collects comprehensive information including:
  - Advisor name
  - Company name
  - Address (Street, Postal Code, City)
  - Contact information (Phone, Fax)
  - Website
  - BFEE ID
  - Detail page URLs
- Detects presence of email addresses (shown as images on the website)
- Provides progress bar during scraping
- Generates statistics about the collected data
- Exports data to Excel format

## Requirements

```bash
python >= 3.11
scrapy
pandas
tqdm
openpyxl