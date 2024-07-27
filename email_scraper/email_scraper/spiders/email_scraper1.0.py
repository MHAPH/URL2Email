import scrapy
import re
import pandas as pd
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse

class EmailSpider(scrapy.Spider):
    name = 'email_spider'
    emails_found = {}

    def __init__(self, *args, **kwargs):
        super(EmailSpider, self).__init__(*args, **kwargs)
        # Load the CSV file with dtype={'Phone': str} to handle phone numbers as strings
        self.df = pd.read_csv(r"C:\Users\muham\Downloads\new_york_restaurants.csv", dtype={'Phone': str})
        self.output_file = 'output_data.csv'
        self.allowed_domains = [urlparse(url).netloc for url in self.df['Website'].dropna().tolist()]
        # Compile the email regex pattern once during initialization
        self.email_regex = re.compile(
            r'[\w\.-]+@(?:' + '|'.join(re.escape(domain) for domain in self.allowed_domains) + r'|gmail\.com|yahoo\.com|outlook\.com|hotmail\.com)'
        )

    def start_requests(self):
        urls = self.df['Website'].dropna().tolist()
        for url in urls:
            self.emails_found[url] = set()
            yield scrapy.Request(url=url, callback=self.parse, meta={'url': url})

    def parse(self, response):
        url = response.meta['url']
        if response.status == 200:
            self.log(f"Parsing URL: {url}")
            # Use the compiled regex pattern to find email addresses on the page
            emails_on_page = set(self.email_regex.findall(response.text))
            # Extract emails from 'mailto:' links
            mailto_links = response.css('a[href^="mailto:"]').xpath('@href').getall()
            for link in mailto_links:
                email = link.split(':', 1)[-1]
                self.emails_found[url].add(email)

            if emails_on_page:
                self.log(f"Found emails on {url}: {emails_on_page}")
                self.emails_found[url].update(emails_on_page)

            # Look for links to contact pages
            contact_links = response.css('a[href*="contact"], a[href*="contacts"], a[href*="contact-us"]').xpath('@href').getall()
            for link in contact_links:
                absolute_contact_url = response.urljoin(link)
                yield scrapy.Request(url=absolute_contact_url, callback=self.parse_contact_page, meta={'url': url})
        else:
            self.log(f"Failed to retrieve {url}: Status {response.status}")

    def parse_contact_page(self, response):
        url = response.meta['url']
        if response.status == 200:
            # Use the compiled regex pattern to find email addresses on the contact page
            emails_on_contact = set(self.email_regex.findall(response.text))
            if emails_on_contact:
                self.log(f"Found emails on contact page {url}: {emails_on_contact}")
                self.emails_found[url].update(emails_on_contact)
        else:
            self.log(f"Failed to retrieve contact page {url}: Status {response.status}")

    def close(self, reason):
        rows = []

        # Create rows for each unique email found
        for url, emails in self.emails_found.items():
            if emails:
                for email in emails:
                    rows.append({
                        'Website': url,
                        'Email': email
                    })

        # Convert the rows list to a DataFrame
        df_emails = pd.DataFrame(rows)

        # Remove duplicates if any
        df_emails.drop_duplicates(subset=['Website', 'Email'], keep='first', inplace=True)

        # Merge with the original DataFrame to add Industry, Title, and Phone columns
        df_merged = pd.merge(df_emails, self.df[['Website', 'Title', 'Phone', 'Industry']], on='Website', how='left')

        # Write the DataFrame to the output file
        df_merged.to_csv(self.output_file, index=False)

if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'LOG_LEVEL': 'INFO',  # Set the log level to INFO to see more detailed logs
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 60,
        'ROBOTSTXT_OBEY': True,
    })
    process.crawl(EmailSpider)
    process.start()


"C:\Users\muham\Downloads\USA Interior Design\30_baltimore_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\29_memphis_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\28_louisville_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\27_portland_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\26_detroit_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\25_boston_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\24_las_vegas_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\23_el_paso_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\22_washington_id.csv",
r"C:\Users\muham\Downloads\USA Interior Design\21_nashville_id.csv"