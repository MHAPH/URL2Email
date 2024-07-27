import scrapy
import re
import json
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings

class EmailSpider(scrapy.Spider):
    name = 'email_spider'
    allowed_domains = ['empiresteakhousenyc.com']
    start_urls = ['https://www.empiresteakhousenyc.com/']
    emails_found = set()

    # Initialize a LinkExtractor which will be used to extract links
    link_extractor = LinkExtractor()

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'DOWNLOAD_DELAY': 2,  # 2 seconds delay between requests
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 2,
        'AUTOTHROTTLE_MAX_DELAY': 60,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
        'ROBOTSTXT_OBEY': True,
        'RETRY_TIMES': 5,
        'LOG_LEVEL': 'INFO'  # Set logging level to INFO
    }

    def parse(self, response):
        # Extract emails and add them to the set after normalization
        emails_on_page = re.findall(r'[\w\.-]+@[\w\.-]+', response.text)
        for email in emails_on_page:
            normalized_email = email.strip().lower()  # Normalize email address
            self.emails_found.add(normalized_email)

        # Extract all links on the page and follow them
        links = self.link_extractor.extract_links(response)
        for link in links:
            yield scrapy.Request(url=link.url, callback=self.parse)

    def close(self, reason):
        output_data = {'emails': list(self.emails_found)} 
        with open('emails.json', 'w') as f:
            json.dump(output_data, f, indent=4)

if __name__ == "__main__":
    configure_logging()
    process = CrawlerProcess(get_project_settings())
    process.crawl(EmailSpider)
    process.start()
