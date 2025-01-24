import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import Spider
from scrapy import signals
from scrapy.exceptions import DropItem
import pandas as pd
from urllib.parse import urljoin
import re
from tqdm import tqdm
from datetime import datetime
from typing import Optional, Dict, List, Any
from pathlib import Path
import sys
from loguru import logger
from pydantic import BaseModel, Field, field_validator
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure loguru logger
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add(
    "logs/bafa_spider_{time}.log",
    rotation="500 MB",
    retention="10 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} | {message}",
    filter=lambda record: record["level"].name == "DEBUG",
    backtrace=True,
    diagnose=True
)

class SpiderConfig(BaseModel):
    """Configuration settings for the BAFA spider."""
    test_mode: bool = Field(default=False, description="Run in test mode with limited entries")
    debug_mode: bool = Field(default=False, description="Enable detailed logging")
    items_per_page: int = Field(default=9999, ge=1, description="Number of items per page")
    page: int = Field(default=0, ge=0, description="Starting page number")
    max_retries: int = Field(default=3, ge=0, description="Maximum number of retry attempts")
    delay_between_requests: float = Field(default=0.25, ge=0, description="Delay between requests in seconds")
    output_dir: Path = Field(default=Path("output"), description="Directory for output files")
    log_dir: Path = Field(default=Path("logs"), description="Directory for log files")

    class Config:
        arbitrary_types_allowed = True

    def get_url(self) -> str:
        """Generate the URL based on configuration."""
        from urllib.parse import urlencode
        base_url = 'https://elan1.bafa.bund.de/bafa-portal/audit-suche/showErgebnis'
        params = {
            'resultsPerPage': 5 if self.test_mode else self.items_per_page,
            'page': self.page
        }
        return f"{base_url}?{urlencode(params)}"

    def setup_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.output_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)

class AdvisorData(BaseModel):
    """Data model for advisor information."""
    Beratername: str = Field(..., min_length=1)
    Beraterfirma: str = Field(default="")
    Strasse: str = Field(default="")
    PLZ: str = Field(default="")
    Ort: str = Field(default="")
    Telefon: str = Field(default="")
    Fax: str = Field(default="")
    Email_Vorhanden: str = Field(default="Nein")
    Email_Image_ID: str = Field(default="")
    Website: str = Field(default="")
    BFEE_ID: str = Field(default="")
    Detail_URL: str = Field(default="")

    @field_validator('PLZ')
    @classmethod
    def validate_plz(cls, v: str) -> str:
        if v and not re.match(r'^\d{5}$', v):
            logger.warning(f"Invalid PLZ format: {v}")
            return ""
        return v

    @field_validator('Email_Vorhanden')
    @classmethod
    def validate_email_vorhanden(cls, v: str) -> str:
        return "Ja" if v == "Ja" else "Nein"

    @field_validator('Website')
    @classmethod
    def validate_website(cls, v: str) -> str:
        if v and not v.startswith(('http://', 'https://')):
            return f"https://{v}"
        return v

class SpiderException(Exception):
    """Base exception for spider-related errors."""
    pass

class DataExtractionError(SpiderException):
    """Raised when data extraction fails."""
    pass

class ValidationError(SpiderException):
    """Raised when data validation fails."""
    pass

class ProgressStatsCollector:
    """Handles progress tracking and statistics collection during crawling."""
    
    def __init__(self, debug_mode: bool) -> None:
        self.total_items: int = 0
        self.processed_items: int = 0
        self.failed_items: int = 0
        self.start_time: Optional[datetime] = None
        self.pbar: Optional[tqdm] = None
        self.debug_mode = debug_mode
        self.errors: List[str] = []

    def set_total(self, total: int) -> None:
        """Initialize progress bar with total items."""
        try:
            self.total_items = total
            self.start_time = datetime.now()
            self.pbar = tqdm(total=total, desc="Processing advisors", disable=False)
            logger.debug(f"Starting to process {total} items")
        except Exception as e:
            logger.error(f"Failed to set progress bar: {str(e)}")
            raise SpiderException("Progress bar initialization failed") from e

    def increment(self, success: bool = True) -> None:
        """Increment progress counter."""
        if self.pbar:
            try:
                self.processed_items += 1
                if not success:
                    self.failed_items += 1
                self.pbar.update(1)
            except Exception as e:
                logger.error(f"Failed to update progress: {str(e)}")

    def add_error(self, error: str) -> None:
        """Add error to collection."""
        self.errors.append(error)
        if self.debug_mode:
            logger.debug(f"Error collected: {error}")

    def finish(self) -> None:
        """Complete progress tracking and display statistics."""
        if self.pbar:
            try:
                self.pbar.close()
                duration = (datetime.now() - self.start_time).total_seconds()
                logger.debug(f"Processing completed in {duration:.2f} seconds")
                logger.debug(f"Total items processed: {self.processed_items}")
                logger.debug(f"Successful items: {self.processed_items - self.failed_items}")
                logger.debug(f"Failed items: {self.failed_items}")
                if self.errors:
                    logger.warning(f"Total errors encountered: {len(self.errors)}")
            except Exception as e:
                logger.error(f"Failed to finish progress tracking: {str(e)}")

class BAFASpider(Spider):
    """Spider for crawling BAFA advisor data."""
    
    name = 'bafa_spider'
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'CONCURRENT_REQUESTS': 32,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
        'COOKIES_ENABLED': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'DOWNLOAD_TIMEOUT': 15,
        'REACTOR_THREADPOOL_MAXSIZE': 20,
    }

    def __init__(self, test_mode: bool = False, debug_mode: bool = False, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.config = SpiderConfig(
            test_mode=test_mode,
            debug_mode=debug_mode
        )
        self.config.setup_directories()
        self.stats_collector = ProgressStatsCollector(debug_mode)
        self.items: List[AdvisorData] = []
        self.start_urls = [self.config.get_url()]
        
        # Update custom settings based on config
        self.custom_settings['DOWNLOAD_DELAY'] = self.config.delay_between_requests
        self.custom_settings['RETRY_TIMES'] = self.config.max_retries

        logger.info(f"Spider initialized in {'TEST' if test_mode else 'FULL'} mode")
        if debug_mode:
            logger.info("Debug logging enabled")

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """Clean and normalize text content."""
        if not text:
            return ''
        return ' '.join(text.replace('&nbsp;', ' ').strip().split())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry_error_callback=lambda retry_state: logger.error(f"Retry failed after {retry_state.attempt_number} attempts")
    )
    def extract_row_data(self, row: scrapy.selector.Selector) -> Optional[Dict[str, str]]:
        """Extract data from a table row."""
        try:
            columns = row.xpath('.//td')
            if len(columns) >= 4:
                return {
                    'Beratername': self.clean_text(columns[0].xpath('.//text()').get()),
                    'Beraterfirma': self.clean_text(columns[1].xpath('.//text()').get()),
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
            return None
        except Exception as e:
            logger.error(f"Error extracting row data: {str(e)}")
            self.stats_collector.add_error(f"Row data extraction failed: {str(e)}")
            raise DataExtractionError("Failed to extract row data") from e

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
                    item_data = self.extract_row_data(row)
                    if item_data:
                        detail_url = row.xpath('.//td[4]//a/@href').get()
                        if detail_url:
                            full_url = urljoin(response.url, detail_url)
                            item_data['Detail_URL'] = full_url
                            yield scrapy.Request(
                                full_url,
                                callback=self.parse_details,
                                meta={'item': item_data},
                                errback=self.handle_error,
                                dont_filter=True
                            )
                except Exception as e:
                    logger.error(f"Error processing row: {str(e)}")
                    self.stats_collector.increment(success=False)

        except Exception as e:
            logger.error(f"Error parsing main page: {str(e)}")
            raise SpiderException("Main page parsing failed") from e

    def parse_details(self, response: scrapy.http.Response) -> Optional[Dict[str, str]]:
        """Parse detailed advisor information."""
        try:
            item_data = response.meta['item']
            detail_texts = response.xpath('//div[@class="bereich"]//text()').getall()
            detail_texts = [self.clean_text(text) for text in detail_texts if self.clean_text(text)]
            
            if detail_texts:
                content = ' '.join(detail_texts)
                self.extract_contact_details(item_data, detail_texts, content, response)
                
                try:
                    # Validate data using Pydantic model
                    advisor_data = AdvisorData(**item_data)
                    # Convert back to dict for Scrapy
                    validated_data = advisor_data.dict()
                    self.items.append(advisor_data)
                    self.stats_collector.increment(success=True)
                    logger.debug(f"Successfully processed advisor: {advisor_data.Beratername}")
                    return validated_data  # Return dict instead of Pydantic model
                except ValidationError as e:
                    logger.error(f"Data validation failed: {str(e)}")
                    self.stats_collector.increment(success=False)
                    return None

        except Exception as e:
            logger.error(f"Error parsing details page: {str(e)}")
            self.stats_collector.increment(success=False)
            return None

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
            raise DataExtractionError("Failed to extract contact details") from e

    def handle_error(self, failure: Any) -> None:
        """Handle request failures."""
        logger.error(f"Request failed: {failure.value}")
        self.stats_collector.add_error(str(failure.value))
        self.stats_collector.increment(success=False)

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
        """Save collected data to Excel file with validation."""
        if not self.items:
            logger.warning("No data was collected!")
            return

        try:
            # Convert items to dict for DataFrame
            items_dict = [item.dict() for item in self.items]
            df = pd.DataFrame(items_dict)
            
            # Additional data cleaning
            df = df.replace({None: '', 'None': '', 'nan': ''})
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            mode = "test" if self.config.test_mode else "full"
            output_file = self.config.output_dir / f'bafa_results_{mode}_{timestamp}.xlsx'
            
            # Save with index and sheet name
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(
                    writer,
                    index=False,
                    sheet_name='BAFA_Advisors'
                )
            
            logger.info(f"Results saved to: {output_file}")
            self.log_statistics(df)
            
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            raise SpiderException("Failed to save results") from e

    def log_statistics(self, df: pd.DataFrame) -> None:
        """Log data collection statistics."""
        stats = {
            "Total records": len(df),
            "Entries with email": len(df[df['Email_Vorhanden'] == 'Ja']),
            "Entries with website": len(df[df['Website'].str.len() > 0]),
            "Unique cities": df['Ort'].nunique(),
            "Failed items": self.stats_collector.failed_items,
            "Success rate": f"{((len(df) - self.stats_collector.failed_items) / len(df) * 100):.2f}%"
        }
        
        logger.info("Collection Statistics:")
        for key, value in stats.items():
            logger.info(f"{key}: {value}")

        if self.stats_collector.errors:
            error_file = self.config.log_dir / f"errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            with open(error_file, 'w') as f:
                for error in self.stats_collector.errors:
                    f.write(f"{error}\n")
            logger.warning(f"Errors have been saved to: {error_file}")

def run_spider(test_mode: bool = False, debug_mode: bool = False) -> None:
    """Run the BAFA spider with error handling."""
    try:
        logger.info(f"Starting BAFA advisor data collection...")
        
        # Create CrawlerProcess with settings
        process = CrawlerProcess({
            'TELNETCONSOLE_ENABLED': False,
            'LOG_LEVEL': 'DEBUG' if debug_mode else 'ERROR',
            'COOKIES_ENABLED': False,
            'DOWNLOAD_TIMEOUT': 15,
            'RETRY_ENABLED': True,
            'RETRY_TIMES': 3,
        })
        
        # Run the spider
        process.crawl(BAFASpider, test_mode=test_mode, debug_mode=debug_mode)
        process.start()
    except Exception as e:
        logger.error(f"Failed to run spider: {str(e)}")
        sys.exit(1)

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