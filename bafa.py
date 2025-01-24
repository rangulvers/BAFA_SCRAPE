import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
from urllib.parse import urljoin
import re
from tqdm import tqdm
from datetime import datetime
from scrapy.spiders import CrawlSpider
from scrapy import signals
from scrapy.utils.project import get_project_settings

# Move ProgressStatsCollector class definition before BAFASpider
class ProgressStatsCollector:
    def __init__(self):
        self.total_items = 0
        self.processed_items = 0
        self.start_time = None
        self.pbar = None

    def set_total(self, total):
        self.total_items = total
        self.start_time = datetime.now()
        self.pbar = tqdm(total=total, desc="Processing advisors")

    def increment(self):
        if self.pbar:
            self.processed_items += 1
            self.pbar.update(1)

    def finish(self):
        if self.pbar:
            self.pbar.close()
            end_time = datetime.now()
            duration = (end_time - self.start_time).total_seconds()
            print(f"\nProcessing completed in {duration:.2f} seconds")
            print(f"Total items processed: {self.processed_items}")

class BAFASpider(scrapy.Spider):
    name = 'bafa_spider'
    start_urls = ['https://elan1.bafa.bund.de/bafa-portal/audit-suche/showErgebnis?resultsPerPage=9999&page=0']
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
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

    def __init__(self, *args, **kwargs):
        super(BAFASpider, self).__init__(*args, **kwargs)
        self.stats_collector = ProgressStatsCollector()
        self.items = []

    def clean_text(self, text):
        if text is None:
            return ''
        return ' '.join(text.replace('&nbsp;', ' ').strip().split())

    def parse(self, response):
        rows = response.xpath('//table[@class="ergebnisListe"]/tbody/tr[position()>1]')
        total_rows = len(rows)
        print(f"Found {total_rows} rows in the table")
        self.stats_collector.set_total(total_rows)

        for row in rows:
            columns = row.xpath('.//td')
            
            if len(columns) >= 4:
                item = {
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
                
                detail_url = columns[3].xpath('.//a/@href').get()
                if detail_url:
                    full_url = urljoin(response.url, detail_url)
                    item['Detail_URL'] = full_url
                    yield scrapy.Request(
                        full_url,
                        callback=self.parse_details,
                        meta={'item': item}
                    )

    def parse_details(self, response):
        item = response.meta['item']
        
        detail_texts = response.xpath('//div[@class="bereich"]//text()').getall()
        detail_texts = [self.clean_text(text) for text in detail_texts if self.clean_text(text)]
        
        if detail_texts:
            content = ' '.join(detail_texts)
            
            for i, text in enumerate(detail_texts):
                if re.match(r'^\d{5}', text):
                    item['PLZ'] = text[:5]
                    item['Ort'] = text[5:].strip()
                    if i > 0:
                        item['Strasse'] = detail_texts[i-1]
                    break
            
            phone_match = re.search(r'Tel\.: ([^F]+)', content)
            if phone_match:
                item['Telefon'] = self.clean_text(phone_match.group(1))
            
            fax_match = re.search(r'Fax: ([^E]+)', content)
            if fax_match:
                item['Fax'] = self.clean_text(fax_match.group(1))
            
            email_img = response.xpath('//div[@class="bereich"]//img[contains(@src, "m2i")]')
            if email_img:
                item['Email_Vorhanden'] = 'Ja'
                img_src = email_img.xpath('@src').get()
                if img_src:
                    nr_match = re.search(r'nr=(\d+)', img_src)
                    if nr_match:
                        item['Email_Image_ID'] = nr_match.group(1)
            
            website = response.xpath('//div[@class="bereich"]//a[contains(@href, "http")]/@href').get()
            if website and 'bafa.bund.de' not in website:
                item['Website'] = website
            
            bfee_match = re.search(r'id=(\d+)', response.url)
            if bfee_match:
                item['BFEE_ID'] = bfee_match.group(1)

        self.items.append(item)
        self.stats_collector.increment()
        return item

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BAFASpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.stats_collector.finish()
        
        if self.items:
            df = pd.DataFrame(self.items)
            df = df.replace({None: '', 'None': '', 'nan': ''})
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            columns_order = [
                'Beratername', 'Beraterfirma', 'Strasse', 'PLZ', 'Ort', 
                'Telefon', 'Fax', 'Email_Vorhanden', 'Email_Image_ID',
                'Website', 'BFEE_ID', 'Detail_URL'
            ]
            df = df[columns_order]
            
            df.to_excel('bafa_results.xlsx', index=False)
            print(f"\nData saved to 'bafa_results.xlsx'")
            print(f"Total records saved: {len(df)}")
            
            print("\nStatistics:")
            print(f"Entries with email: {len(df[df['Email_Vorhanden'] == 'Ja'])}")
            print(f"Entries with website: {len(df[df['Website'].str.len() > 0])}")
            print(f"Unique cities: {df['Ort'].nunique()}")
        else:
            print("\nNo data was collected!")

def run_spider():
    print("Starting BAFA advisor data collection...")
    
    process = CrawlerProcess({
        'TELNETCONSOLE_ENABLED': False,
        'LOG_LEVEL': 'INFO'
    })
    
    process.crawl(BAFASpider)
    process.start()

if __name__ == "__main__":
    run_spider()