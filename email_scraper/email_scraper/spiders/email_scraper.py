import scrapy
import re
import pandas as pd
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class EmailSpider(scrapy.Spider):
    name = 'email_spider'
    emails_found = {}

    def __init__(self, *args, **kwargs):
        super(EmailSpider, self).__init__(*args, **kwargs)
        # List of file paths to read
        file_paths = [
r"C:\Users\muham\Downloads\USA Hospital\40_omaha_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\39_colorado_springs_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\37_atlanta_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\38_kansas_city_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\36_mesa_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\35_sacramento_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\34_fresno_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\33_tucson_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\32_albuquerque_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\31_milwaukee_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\50_arlington_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\49_tampa_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\48_tulsa_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\47_bakersfield_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\45_oakland_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\46_minneapolis_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\44_long_beach_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\43_virginia_beach_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\42_miami_health.csv",
r"C:\Users\muham\Downloads\USA Hospital\41_raleigh_health.csv"

        ]

        # Read and concatenate all the dataframes
        self.df = pd.concat([pd.read_csv(file, dtype={'Phone': str}) for file in file_paths], ignore_index=True)
        self.output_file = 'output_data.csv'

        # Set up Selenium WebDriver with headless Chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Ensure GUI is off
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-logging")
        chrome_options.add_argument("--log-level=3")
        self.driver = webdriver.Chrome(service=Service('C:\\Windows\\chromedriver.exe'), options=chrome_options)

    def start_requests(self):
        self.allowed_domains = [urlparse(url).netloc for url in self.df['Website'].dropna().tolist()]
        urls = self.df['Website'].dropna().tolist()

        for url in urls:
            self.emails_found[url] = set()  # Use a set to ensure emails are unique per URL
            yield scrapy.Request(url=url, callback=self.parse, meta={'url': url})

    def parse(self, response):
        url = response.meta['url']
        if response.status == 200:
            self.log(f"Parsing URL: {url}")

            # Use Selenium to render the page
            self.driver.get(response.url)
            self.dynamic_wait(By.TAG_NAME, 'body')  # Wait for the body to load

            page_source = self.driver.page_source
            self.extract_emails(url, page_source)

            # Look for links to various potential pages where emails can be found
            relevant_links = self.driver.find_elements(By.XPATH, '//a[contains(@href, "contact") or contains(@href, "contacts") or contains(@href, "contact-us")]')

            for link in relevant_links:
                absolute_contact_url = response.urljoin(link.get_attribute('href'))
                yield scrapy.Request(url=absolute_contact_url, callback=self.parse_contact_page, meta={'url': url})
        else:
            self.log(f"Failed to retrieve {url}: Status {response.status}")

    def parse_contact_page(self, response):
        url = response.meta['url']
        if response.status == 200:
            self.driver.get(response.url)
            self.dynamic_wait(By.TAG_NAME, 'body')  # Wait for the body to load

            page_source = self.driver.page_source
            self.extract_emails(url, page_source)
        else:
            self.log(f"Failed to retrieve contact page {url}: Status {response.status}")

    def extract_emails(self, url, page_source):
        # Regular expression for emails
        email_regex = r'[\w\.-]+@(?:' + '|'.join(re.escape(domain) for domain in self.allowed_domains) + r'|gmail\.com|yahoo\.com|outlook\.com|hotmail\.com)'

        # Find emails directly in the page source
        emails_on_page = set(re.findall(email_regex, page_source))
        
        # Find emails in mailto links
        mailto_links = set(re.findall(r'mailto:([\w\.-]+@[\w\.-]+\.\w+)', page_source))
        emails_on_page.update(mailto_links)
        
        if emails_on_page:
            self.log(f"Found emails on {url}: {emails_on_page}")
            self.emails_found[url].update(emails_on_page)  # Use set to add emails, ensuring uniqueness

    def dynamic_wait(self, by, value):
        try:
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((by, value)))
        except Exception as e:
            self.log(f"Error waiting for element: {e}")

    def close(self, reason):
        rows = []

        for url, emails in self.emails_found.items():
            for email in emails:
                rows.append({
                    'Website': url,
                    'Email': email
                })

        df_emails = pd.DataFrame(rows).drop_duplicates()  # Ensure no duplicate rows
        df_merged = pd.merge(df_emails, self.df[['Website', 'Title', 'Phone', 'Industry']], on='Website', how='left')
        df_merged.to_csv(self.output_file, index=False)

        # Close the Selenium WebDriver
        self.driver.quit()

if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'LOG_LEVEL': 'INFO'
    })
    process.crawl(EmailSpider)
    process.start()