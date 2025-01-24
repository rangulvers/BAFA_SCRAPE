import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy import signals
import pandas as pd
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import re
from tqdm import tqdm
from datetime import datetime
from typing import Optional, Dict, List, Any
from pathlib import Path
from dataclasses import dataclass
from loguru import logger
import sys

# Configure loguru logger
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="INFO")  # Add stderr handler for normal output
logger.add(
    "bafa_spider_{time}.log",
    rotation="500 MB",
    retention="10 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    filter=lambda record: record["level"].name == "DEBUG"
)

@dataclass
class SpiderConfig:
    """Configuration settings for the BAFA spider."""
    test_mode: bool = False
    debug_mode: bool = False
    items_per_page: int = 9999
    page: int = 0
    
    def get_url(self) -> str:
        """Generate the URL based on configuration."""
        base_url = 'https://elan1.bafa.bund.de/bafa-portal/audit-suche/showErgebnis'
        params = {
            'resultsPerPage': 5 if self.test_mode else self.items_per_page,
            'page': self.page
        }
        return f"{base_url}?{urlencode(params)}"

class ProgressStatsCollector:
    """Handles progress tracking and statistics collection during crawling."""
    
    def __init__(self, debug_mode: bool) -> None:
        self.total_items: int = 0
        self.processed_items: int = 0
        self.start_time: Optional[datetime] = None
        self.pbar: Optional[tqdm] = None
        self.debug_mode = debug_mode

    def set_total(self, total: int) -> None:
        """Initialize progress bar with total items."""
        try:
            self.total_items = total
            self.start_time = datetime.now()
            self.pbar = tqdm(total=total, desc="Processing advisors", disable=False)
            logger.debug(f"Starting to process {total} items")
        except Exception as e:
            logger.error(f"Failed to set progress bar: {str(e)}")

    def increment(self) -> None:
        """Increment progress counter."""
        if self.pbar:
            try:
                self.processed_items += 1
                self.pbar.update(1)
            except Exception as e:
                logger.error(f"Failed to update progress: {str(e)}")

    def finish(self) -> None:
        """Complete progress tracking and display statistics."""
        if self.pbar:
            try:
                self.pbar.close()
                duration = (datetime.now() - self.start_time).total_seconds()
                logger.debug(f"Processing completed in {duration:.2f} seconds")
                logger.debug(f"Total items processed: {self.processed_items}")
            except Exception as e:
                logger.error(f"Failed to finish progress tracking: {str(e)}")

class BAFASpider(Spider):
    """Spider for crawling BAFA advisor data."""
    
    name = 'bafa_spider'
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
        'DOWNLOAD_DELAY': 0.25,
        'LOG_LEVEL': 'INFO',
        'COOKIES_ENABLED': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'DOWNLOAD_TIMEOUT': 15,
        'REACTOR_THREADPOOL_MAXSIZE': 20,
    }

    def __init__(self, test_mode: bool = False, debug_mode: bool = False, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.config = SpiderConfig(test_mode=test_mode)
        self.stats_collector = ProgressStatsCollector(debug_mode)
        self.items: List[Dict[str, str]] = []
        self.start_urls = [self.config.get_url()]
        
        # Update logger level based on debug mode
        if debug_mode:
            logger.remove()
            logger.add(sys.stderr, level="DEBUG")
        
        mode_str = []
        if test_mode:
            mode_str.append("TEST")
        if debug_mode:
            mode_str.append("DEBUG")
        if not mode_str:
            mode_str.append("FULL")
        
        logger.info(f"Running spider in {' + '.join(mode_str)} mode")

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """Clean and normalize text content."""
        if not text:
            return ''
        return ' '.join(text.replace('&nbsp;', ' ').strip().split())

    def extract_row_data(self, row: scrapy.selector.Selector) -> Dict[str, str]:
        """Extract data from a table row."""
        try:
            columns = row.xpath('.//td')
            if len(columns) >= 4:
                return {
                    'Beratername': self.clean_text(columns[0].xpath('.//text()').get()),
                    'Beraterfirma': self.clean_text(columns[1].xpath('.//text()').get()),
                    'Beratersitz': self.clean_text(columns[2].xpath('.//text()').get()),
                    'Strasse': '',
                    'PLZ': '',
                    'Ort': '',
                    'Telefon': '',
                    'Fax': '',
                    'Email_Vorhanden': 'Nein',
                    'Email_Image_ID': '',
                    'Website': '',
                    'BFEE_ID': '',
                    'Detail_URL': ''
                }
            return {}
        except Exception as e:
            logger.error(f"Error extracting row data: {str(e)}")
            return {}

    def parse(self, response: scrapy.http.Response) -> Any:
        """Parse the main page with advisor listings."""
        try:
            rows = response.xpath('//table[@class="ergebnisListe"]/tbody/tr[position()>1]')
            total_rows = len(rows)
            
            if self.config.test_mode:
                rows = rows[:5]
                total_rows = 5
                logger.debug("TEST MODE: Limited to 5 entries")
            
            logger.debug(f"Found {total_rows} rows to process")
            self.stats_collector.set_total(total_rows)

            for row in rows:
                try:
                    item = self.extract_row_data(row)
                    if item:
                        detail_url = row.xpath('.//td[4]//a/@href').get()
                        if detail_url:
                            full_url = urljoin(response.url, detail_url)
                            item['Detail_URL'] = full_url
                            yield scrapy.Request(
                                full_url,
                                callback=self.parse_details,
                                meta={'item': item},
                                errback=self.handle_error
                            )
                except Exception as e:
                    logger.error(f"Error processing row: {str(e)}")

        except Exception as e:
            logger.error(f"Error parsing main page: {str(e)}")

    def parse_details(self, response: scrapy.http.Response) -> Dict[str, str]:
        """Parse detailed advisor information."""
        try:
            item = response.meta['item']
            detail_texts = response.xpath('//div[@class="bereich"]//text()').getall()
            detail_texts = [self.clean_text(text) for text in detail_texts if self.clean_text(text)]
            
            if detail_texts:
                content = ' '.join(detail_texts)
                self.extract_contact_details(item, detail_texts, content, response)
                logger.debug(f"Processed details for advisor: {item['Beratername']}")

            self.items.append(item)
            self.stats_collector.increment()
            return item

        except Exception as e:
            logger.error(f"Error parsing details page: {str(e)}")
            return response.meta['item']

    def extract_contact_details(self, item: Dict[str, str], detail_texts: List[str], 
                              content: str, response: scrapy.http.Response) -> None:
        """Extract contact details from the response."""
        try:
            # Extract address
            for i, text in enumerate(detail_texts):
                if re.match(r'^\d{5}', text):
                    item['PLZ'] = text[:5]
                    item['Ort'] = text[5:].strip()
                    if i > 0:
                        item['Strasse'] = detail_texts[i-1]
                    break

            # Extract phone and fax
            phone_match = re.search(r'Tel\.: ([^F]+)', content)
            if phone_match:
                item['Telefon'] = self.clean_text(phone_match.group(1))
            
            fax_match = re.search(r'Fax: ([^E]+)', content)
            if fax_match:
                item['Fax'] = self.clean_text(fax_match.group(1))

            # Extract email information
            email_img = response.xpath('//div[@class="bereich"]//img[contains(@src, "m2i")]')
            if email_img:
                item['Email_Vorhanden'] = 'Ja'
                img_src = email_img.xpath('@src').get()
                if img_src:
                    nr_match = re.search(r'nr=(\d+)', img_src)
                    if nr_match:
                        item['Email_Image_ID'] = nr_match.group(1)

            # Extract website
            website = response.xpath('//div[@class="bereich"]//a[contains(@href, "http")]/@href').get()
            if website and 'bafa.bund.de' not in website:
                item['Website'] = website

            # Extract BFEE ID
            bfee_match = re.search(r'id=(\d+)', response.url)
            if bfee_match:
                item['BFEE_ID'] = bfee_match.group(1)

            logger.debug(f"Extracted contact details for: {item['Beratername']}")

        except Exception as e:
            logger.error(f"Error extracting contact details: {str(e)}")

    def handle_error(self, failure: Any) -> None:
        """Handle request failures."""
        logger.error(f"Request failed: {failure.value}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BAFASpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider: Spider) -> None:
        """Handle spider closure and save results."""
        self.stats_collector.finish()
        self.save_results()

    def save_results(self) -> None:
        """Save collected data to Excel file."""
        if not self.items:
            logger.warning("No data was collected!")
            return

        try:
            df = pd.DataFrame(self.items)
            df = df.replace({None: '', 'None': '', 'nan': ''})
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            columns_order = [
                'Beratername', 'Beraterfirma', 'Strasse', 'PLZ', 'Ort', 
                'Telefon', 'Fax', 'Email_Vorhanden', 'Email_Image_ID',
                'Website', 'BFEE_ID', 'Detail_URL'
            ]
            df = df[columns_order]
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            mode = "test" if self.config.test_mode else "full"
            output_file = Path(f'bafa_results_{mode}_{timestamp}.xlsx')
            
            df.to_excel(output_file, index=False)
            logger.info(f"Results saved to: {output_file}")
            self.log_statistics(df)
            
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")

    def log_statistics(self, df: pd.DataFrame) -> None:
        """Log data collection statistics."""
        logger.info("Collection Statistics:")
        logger.info(f"Total records saved: {len(df)}")
        logger.info(f"Entries with email: {len(df[df['Email_Vorhanden'] == 'Ja'])}")
        logger.info(f"Entries with website: {len(df[df['Website'].str.len() > 0])}")
        logger.info(f"Unique cities: {df['Ort'].nunique()}")

def run_spider(test_mode: bool = False, debug_mode: bool = False) -> None:
    """Run the BAFA spider."""
    try:
        logger.info(f"Starting BAFA advisor data collection...")
        
        process = CrawlerProcess({
            'TELNETCONSOLE_ENABLED': False,
            'LOG_LEVEL': 'DEBUG' if debug_mode else 'ERROR'
        })
        
        process.crawl(BAFASpider, test_mode=test_mode, debug_mode=debug_mode)
        process.start()
    except Exception as e:
        logger.error(f"Failed to run spider: {str(e)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='BAFA Advisor Data Scraper')
    parser.add_argument('--test', 
                       action='store_true',
                       help='Run in test mode (only scrape 5 entries)')
    parser.add_argument('--debug',
                       action='store_true',
                       help='Run in debug mode (show detailed logs)')
    
    args = parser.parse_args()
    run_spider(test_mode=args.test, debug_mode=args.debug)