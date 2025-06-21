import json
import os
import re
import urllib.parse
import scrapy
import html
from datetime import datetime, timezone
from html.parser import HTMLParser
from scrapy import Request
from concurrent.futures import ThreadPoolExecutor
import asyncio

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

class IndexSpider(scrapy.Spider):
    name = "linkedin_jobs"

    
    def strip_tags(self, html_text):
        s = MLStripper()
        s.feed(html_text)
        return s.get_data()

    def clean_text(self, text):
        if not text:
            return ""
            
        # First decode HTML entities
        text = html.unescape(text)
        
        # Remove HTML tags using HTMLParser
        text = self.strip_tags(text)
        
        # Clean up whitespace and formatting
        text = re.sub(r'[\r\n]+', '\n', text)  # Normalize line endings
        text = re.sub(r'\n\s*\n', '\n', text)  # Remove multiple empty lines
        text = re.sub(r'[ \t]+', ' ', text)  # Replace multiple spaces/tabs with single space
        text = re.sub(r'\n\s+', '\n', text)  # Remove leading spaces after newlines
        text = re.sub(r'\s+\n', '\n', text)  # Remove trailing spaces before newlines
        
        # Final cleanup
        text = text.strip()  # Remove leading/trailing whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)  # Limit consecutive newlines to 2
        
        return text

    def start_requests(self):
        csv_path = os.path.join(os.path.dirname(__file__), "../../uk_links_only.csv")
        with open(csv_path, "r") as f:
            search_urls = [line.strip() for line in f if line.strip()]
        
        for search_url in search_urls:
            yield scrapy.Request(
                url=search_url,
                callback=self.parse_search_results,
                meta={'base_url': search_url},
                priority=0
            )

    def parse_search_results(self, response):
        # Get total job count from the header
        job_count_text = response.xpath('//h1[contains(@class, "results-context-header__context")]//span[contains(@class, "results-context-header__job-count")]/text()').get()
        if job_count_text:
            job_count = int(re.sub(r'[^0-9]', '', job_count_text))
        else:
            self.logger.info(f"Could not find job count in the response for {response.url}")
            return
        
        blur_li_count = response.xpath('//div[@class="blurred-content blur"]//ul/li').getall()
        if blur_li_count:
            self.logger.warning(f"Blurred content detected for {response.url}")
            job_count = 1
        
        
        print(f"Job count: {job_count} For URL: {response.url}")
        parsed_url = urllib.parse.urlparse(response.meta['base_url'])
        query_params = urllib.parse.parse_qs(parsed_url.query)
        
        # Extract search parameters
        keywords = query_params.get('keywords', [''])[0]
        location = query_params.get('location', [''])[0]
        geo_id = query_params.get('geoId', [''])[0]
        
        # Create a list of start positions for concurrent requests
        start_positions = list(range(0, job_count, 25))
        
        # Generate paginated URLs concurrently
        for start in start_positions:
            paginated_url = (
                f"https://www.linkedin.com/jobs/search"
                f"?keywords={urllib.parse.quote(keywords)}"
                f"&location={urllib.parse.quote(location)}"
                f"&geoId={geo_id}"
                f"&trk=public_jobs_jobs-search-bar_search-submit"
                f"&start={start}"
            )
            
            yield scrapy.Request(
                url=paginated_url,
                callback=self.parse_job_listings,
                meta={
                    'base_url': response.meta['base_url'],
                    'start': start
                },
                priority=0,
                dont_filter=True  # Allow duplicate requests
            )

    def parse_job_listings(self, response):
        # Extract job links from the paginated results
        job_links = response.xpath("//a[contains(@class, 'base-card__full-link')]/@href").getall()
        if not job_links:
            job_links = response.xpath("//a[contains(@href, '/jobs/view/')]/@href").getall()
        
        # Create a list of requests for job details
        for job_url in job_links:
           
            yield scrapy.Request(
                url=job_url,
                callback=self.parse_job_detail,
                meta={'job_search_url': response.meta['base_url']},
                priority=10,
                dont_filter=True  # Allow duplicate requests
            )

    def parse_job_detail(self, response):
        job_search_url = response.meta.get("job_search_url")
        json_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        company_logo = response.xpath('//div[contains(@class, "top-card-layout__card")]//img[contains(@class, "artdeco-entity-image")]/@data-delayed-url').get()

        for script in json_scripts:
            try:
                data = json.loads(script)
                if isinstance(data, dict) and data.get("@type") == "JobPosting":
                    job = {
                        "job_search_url": job_search_url,
                        "linkedin_url": response.url,
                        "title": data.get("title"),
                        "company": data.get("hiringOrganization", {}).get("name"),
                        "company_logo": company_logo,
                        "location": (
                            data.get("jobLocation", {}).get("address", {}).get("addressLocality")
                            if isinstance(data.get("jobLocation"), dict) else None
                        ),
                        "country": (
                            data.get("jobLocation", {}).get("address", {}).get("addressCountry")
                            if isinstance(data.get("jobLocation"), dict) else None
                        ),
                        "region": (
                            data.get("jobLocation", {}).get("address", {}).get("addressRegion")
                            if isinstance(data.get("jobLocation"), dict) else None
                        ),
                        "date_posted": data.get("datePosted"),
                        "valid_through": data.get("validThrough"),
                        "employment_type": data.get("employmentType"),
                        "month_of_experience": (
                            data.get("experienceRequirements", {}).get('monthOfExperience')
                            if isinstance(data.get("experienceRequirements"), dict) else None
                        ),
                        "education": (
                            data.get("educationRequirements", {}).get("credentialCategory")
                            if isinstance(data.get("educationRequirements"), dict) else None
                        ),
                        "summary": self.clean_text(data.get("description", ""))
                    }

                    # Additional job criteria
                    criteria = {}
                    for item in response.xpath("//ul[contains(@class, 'description__job-criteria-list')]//li"):
                        title = item.xpath(".//h3/text()").get()
                        value = item.xpath(".//span/text()").get()
                        if title and value:
                            criteria[title.strip()] = value.strip()
                    job.update(criteria)

                    # Apply URL extraction
                    job["apply_url"] = ''
                    raw_code_html = response.xpath('//code[@id="applyUrl"]').get()
                    if raw_code_html:
                        match = re.search(r'<!--"(.*?)"-->', raw_code_html)
                        if match:
                            raw_url = match.group(1)
                            query_params = urllib.parse.parse_qs(urllib.parse.urlparse(raw_url).query)
                            encoded_url = query_params.get('url', [None])[0]
                            job["apply_url"] = urllib.parse.unquote(encoded_url) if encoded_url else ""

                    yield job
                    break
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON for {response.url}: {e}")