import json
import os
import re
import urllib.parse
import scrapy
import html
from datetime import datetime, timezone
from html.parser import HTMLParser

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
            urls = [line.strip() for line in f if line.strip()]
        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results,
                priority=0  # Lower priority for search pages
            )

    def parse_search_results(self, response):
        print(f"Parsing search results page: {response.url}")
        
        # Debug: Print the HTML to verify the structure
        
        # Try different XPath selectors
        job_links = response.xpath("//a[contains(@class, 'base-card__full-link')]/@href").getall()
        if not job_links:
            job_links = response.xpath("//a[contains(@href, '/jobs/view/')]/@href").getall()
        
        print(f"Found {len(job_links)} job links")
        
        for job_url in job_links:
            yield scrapy.Request(
                url=job_url,
                callback=self.parse_job_detail,
                meta={'job_search_url': response.url},
                priority=10
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