"""
Web scraper utility for extracting coaching information from NCAA school websites.
This module provides functions to scrape coaching data with fallback to AI parsing.
"""
import logging
import requests
from bs4 import BeautifulSoup
import time
import random
import os
import re
import json
from typing import Dict, List, Optional, Union, Any
from urllib.parse import urljoin, urlparse

# Import the AI parser
try:
    from ai_parser import AIParser, parse_with_fallback
except ImportError:
    # Define a placeholder if AIParser can't be imported
    print("Warning: AIParser module could not be imported. AI parsing features will be disabled.")
    AIParser = None
    parse_with_fallback = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NCAAScraper:
    """Class for scraping coaching information from NCAA school websites."""
    
    def __init__(self, ai_parser: Optional[AIParser] = None, headers: Optional[Dict] = None):
        """
        Initialize the scraper with optional AI parser and headers.
        
        Args:
            ai_parser: Optional AIParser instance for fallback parsing
            headers: Optional HTTP headers to use in requests
        """
        #self.ai_parser = ai_parser or AIParser()
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
    
    def get_page(self, url: str, params: Optional[Dict] = None, max_retries: int = 1) -> Optional[str]:
        """
        Fetch a page and return its HTML content.
        
        Args:
            url: The URL to fetch
            params: Optional query parameters
            max_retries: Maximum number of retry attempts
            
        Returns:
            HTML content as string or None if the request failed
        """
        for attempt in range(max_retries):
            try:
                # Add a small random delay to avoid overloading the server
                time.sleep(random.uniform(1, 3))
                
                logger.info(f"Fetching URL: {url} (attempt {attempt + 1}/{max_retries})")
                
                response = self.session.get(
                    url, 
                    headers=self.headers, 
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                logger.info(f"Successfully fetched URL: {url}")
                logger.info(f"Final URL after redirects: {response.url}")
                return response.text
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                
                if attempt == max_retries - 1:
                    # This was the last attempt
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
                
                # Wait before retrying (exponential backoff)
                time.sleep(2 ** attempt)
        
        return None
    
    def get_simulate_coaches_link(self, url: str) -> Optional[str]:
        """
        Try common coaches URL patterns when standard scraping fails.
        
        Args:
            url: The football program URL
            
        Returns:
            The coaches URL if found, None otherwise
        """
        try:
            logging.info(f"Trying simulated coaches links for: {url}")
            
            # Common coaches URL patterns
            patterns = [
                '/coaches',
                '/coaching-staff',
                '/football/coaches',
                '/football/staff',
                '/football/coaching-staff',
                '/football/coaches',
                '/sports/football/coaches',
                '/staff-directory'
            ]
            
            # Add headers to mimic a browser
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Try each pattern
            for pattern in patterns:
                test_url = urljoin(url, pattern)
                try:
                    logging.info(f"Testing URL: {test_url}")
                    response = requests.get(
                        test_url,
                        headers=headers,
                        timeout=5,
                        verify=False,
                        allow_redirects=True
                    )
                    
                    # Check if we got a successful response
                    if response.status_code == 200:
                        # Additional check to verify it's a coaches page
                        content_lower = response.text.lower()
                        if any(keyword in content_lower for keyword in ['coach', 'staff', 'coaching staff']):
    
                            logging.info(f"Found valid coaches page: {test_url}")
                            return test_url
                            
                except requests.exceptions.RequestException as e:
                    logging.debug(f"Failed to access {test_url}: {str(e)}")
                    continue
            
            logging.info("No valid coaches pages found through simulation")
            return None
            
        except Exception as e:
            logging.error(f"Error in get_simulate_coaches_link: {str(e)}")
            return None

    def get_simulate_roster_link(self, url: str) -> Optional[str]:
        """
        Try common roster URL patterns when standard scraping fails.
        
        Args:
            url: The football program URL
            
        Returns:
            The roster URL if found, None otherwise
        """
        try:
            logging.info(f"Trying simulated roster links for: {url}")
            
            # Common roster URL patterns
            patterns = [
                '/roster',
                '/team-roster',
                '/football/roster',
                '/football/team-roster',
                '/sports/football/roster',
                '/football/roster.html',
                '/football/team-roster.html',
    
            ]
            
            # Add headers to mimic a browser
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Try each pattern
            for pattern in patterns:
                test_url = urljoin(url, pattern)
                try:
                    logging.info(f"Testing URL: {test_url}")
                    response = requests.get(
                        test_url,
                        headers=headers,
                        timeout=5,
                        verify=False,
                        allow_redirects=True
                    )
                    
                    # Check if we got a successful response
                    if response.status_code == 200:
                        # Additional check to verify it's a roster page
                        content_lower = response.text.lower()
                        if any(keyword in content_lower for keyword in ['roster', 'players', 'team roster']):
                            logging.info(f"Found valid roster page: {test_url}")
                            return test_url
                            
                except requests.exceptions.RequestException as e:
                    logging.debug(f"Failed to access {test_url}: {str(e)}")
                    continue
            
            logging.info("No valid roster pages found through simulation")
            return None
            
        except Exception as e:
            logging.error(f"Error in get_simulate_roster_link: {str(e)}")
            return None

    def get_simulate_football_link(self, url: str) -> Optional[str]:
        """
        Try common football URL patterns when standard scraping fails.
        
        Args:
            url: The athletics website URL
            
        Returns:
            The football program URL if found, None otherwise
        """
        try:
            logging.info(f"Trying simulated football links for: {url}")
            
            # Common football URL patterns
            patterns = [
                '/team/football',
                '/sports/football',
                '/football-team',
                '/football-program',
                '/football/schedule',
                '/football/roster',
                '/football/coaches',
                '/football/home',
                '/football/index',
                '/football/team',
                '/football/overview',
            ]
            
            # Add headers to mimic a browser
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Try each pattern
            for pattern in patterns:
                test_url = urljoin(url, pattern)
                try:
                    logging.info(f"Testing URL: {test_url}")
                    response = requests.get(
                        test_url,
                        headers=headers,
                        timeout=5,
                        verify=False,
                        allow_redirects=True
                    )
                    
                    # Check if we got a successful response
                    if response.status_code == 200:
                        # Additional check to verify it's a football page
                        if any(keyword in response.text.lower() for keyword in ['football', 'fball', 'football team', 'football program']):
                            logging.info(f"Found valid football page: {test_url}")
                            return test_url
                            
                except requests.exceptions.RequestException as e:
                    logging.debug(f"Failed to access {test_url}: {str(e)}")
                    continue
            
            logging.info("No valid football pages found through simulation")
            return None
            
        except Exception as e:
            logging.error(f"Error in get_simulate_football_link: {str(e)}")
            return None

    def find_athletics_url(self, school_website: str) -> Optional[str]:
        """
        Find the athletics website URL from the main school website.
        
        Args:
            school_website: Main school website URL
            
        Returns:
            Athletics website URL or None if not found
        """
        html_content = self.get_page(school_website)
        
        if not html_content:
            logger.error(f"Could not fetch school website: {school_website}")
            return None
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Common patterns for athletics links
        athletics_keywords = [
            'athletics', 'sports', 'varsity', 'teams', 
            'intercollegiate', 'recreation', 'fitness'
        ]
        
        # Look for links containing athletics keywords
        for keyword in athletics_keywords:
            # Try to find links with the keyword in text
            links = soup.find_all('a', text=lambda text: text and keyword.lower() in text.lower())
            
            # Also try to find links with the keyword in href
            links.extend(soup.find_all('a', href=lambda href: href and keyword.lower() in href.lower()))
            
            if links:
                # Get the first matching link
                href = links[0].get('href', '')
                
                # Make sure it's an absolute URL
                if href.startswith('http'):
                    return href
                else:
                    return urljoin(school_website, href)
        
        # If standard parsing fails, try AI parsing
        if self.ai_parser:
            logger.info(f"Standard parsing failed to find athletics URL, trying AI parsing")
            
            prompt = f"""
            Find the URL for the athletics or sports department website from this school's main webpage.
            Return just the URL as a string.
            """
            
            result = self.ai_parser.parse_html(html_content, prompt)
            
            if 'error' not in result and 'url' in result:
                athletics_url = result['url']
                
                # Make sure it's an absolute URL
                if not athletics_url.startswith('http'):
                    athletics_url = urljoin(school_website, athletics_url)
                
                logger.info(f"AI parser found athletics URL: {athletics_url}")
                return athletics_url
        
        logger.warning(f"Could not find athletics URL for {school_website}")
        return None
    
    def find_football_team_page(self, athletics_url: str) -> Optional[str]:
        """
        Find the football team page from the athletics website.
        
        Args:
            athletics_url: Athletics website URL
            
        Returns:
            Football team page URL or None if not found
        """
        try:
            logger.info(f"Processing athletics URL: {athletics_url}")
            
            # Add https:// if not present and ensure it's a proper URL
            if not athletics_url.startswith(('http://', 'https://')):
                athletics_url = 'https://' + athletics_url
                logger.debug(f"Added https:// prefix: {athletics_url}")
            
            # Ensure the URL is properly formatted
            parsed_url = urlparse(athletics_url)
            if not parsed_url.netloc:
                logger.error(f"Invalid athletics URL format: {athletics_url}")
                return None
            
            # Get the page content using the existing method
            html_content = self.get_page(athletics_url)
            
            if not html_content:
                logger.error(f"Could not fetch athletics website: {athletics_url}")
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Keywords specific to football program pages
            keywords = ['football', 'football program', 'fball', 'football team', 'football home']
            negative_keywords = ['soccer', 'baseball', 'basketball', 'volleyball', 'softball', 'lacrosse', 
                               'tennis', 'golf', 'swimming', 'diving', 'track', 'field', 'cross country',
                               'wrestling', 'hockey', 'rowing', 'gymnastics', 'rugby', 'bowling', 'fencing',
                               'water polo', 'beach volleyball', 'field hockey', 'skiing', 'equestrian']
            
            potential_links = []
            
            # Look for navigation menus first
            nav_elements = soup.find_all(['nav', 'div', 'ul'], 
                                       class_=lambda x: x and any(word in x.lower() 
                                       for word in ['menu', 'nav', 'dropdown', 'sports', 'athletics']))
            
            # Check navigation elements
            for nav in nav_elements:
                for link in nav.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower()
                    
                    if not href:
                        continue
                        
                    # Skip if link contains negative keywords
                    if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                          for neg_keyword in negative_keywords):
                        continue
                        
                    if any(keyword in text.lower() or keyword in href.lower() 
                          for keyword in keywords):
                        full_url = urljoin(athletics_url, href)
                        potential_links.append(full_url)
            
            # Check all links if none found in navigation
            if not potential_links:
                for link in soup.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower()
                    
                    if not href:
                        continue
                        
                    # Skip if link contains negative keywords
                    if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                          for neg_keyword in negative_keywords):
                        continue
                        
                    if any(keyword in text.lower() or keyword in href.lower() 
                          for keyword in keywords):
                        full_url = urljoin(athletics_url, href)
                        potential_links.append(full_url)
            
            if potential_links:
                chosen_link = potential_links[0].split('?')[0]  # Remove query parameters
                logger.info(f"Found football program link: {chosen_link}")
                return chosen_link
            
            # If standard parsing fails, try AI parsing
            if self.ai_parser:
                logger.info(f"Standard parsing failed to find football team page, trying AI parsing")
                
                prompt = f"""
                Find the URL for the football team page from this athletics website.
                Return just the URL as a string.
                """
                
                result = self.ai_parser.parse_html(html_content, prompt)
                
                if result and result.startswith('http'):
                    logger.info(f"AI parsing found football team page: {result}")
                    return result
                elif result:
                    full_url = urljoin(athletics_url, result)
                    logger.info(f"AI parsing found football team page (converted to absolute URL): {full_url}")
                    return full_url
            
            logger.info(f"No football program link found for {athletics_url}")
            return None
            
        except Exception as e:
            logger.error(f"Error processing {athletics_url}: {str(e)}")
            return None
    
    def find_coaches_page(self, athletics_url: str) -> Optional[str]:
        """
        Find the coaches directory page or roster page from the athletics website.
        
        Args:
            athletics_url: Athletics website URL
            
        Returns:
            Coaches page URL, roster page URL, or None if not found
        """
        try:
            logger.info(f"Processing athletic website URL: {athletics_url}")
            
            # Add https:// if not present
            if not athletics_url.startswith(('http://', 'https://')):
                athletics_url = 'https://' + athletics_url
                logger.debug(f"Added https:// prefix: {athletics_url}")
            
            # Get the page content using the existing method
            html_content = self.get_page(athletics_url)
            
            if not html_content:
                logger.error(f"Could not fetch athletics website: {athletics_url}")
                return None
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Keywords for both coaches and roster pages
            negative_keywords = ['soccer', 'baseball', 'basketball', 'volleyball', 'softball', 'lacrosse', 
                               'tennis', 'golf', 'swimming', 'diving', 'track', 'field', 'cross country',
                               'wrestling', 'hockey', 'rowing', 'gymnastics', 'rugby', 'bowling', 'fencing',
                               'water polo', 'beach volleyball', 'field hockey', 'skiing', 'equestrian']
            coaches_keywords = ['coaches', 'coaching staff', 'staff directory', 'football staff', 
                              'coaches & staff', 'coaches and staff', 'staff', 'directory']
            roster_keywords = ['roster', 'team roster', 'football roster', 'players']
            
            potential_coaches_links = []
            potential_roster_links = []
            
            # Look for navigation menus first
            nav_elements = soup.find_all(['nav', 'div', 'ul'], 
                                       class_=lambda x: x and any(word in x.lower() 
                                       for word in ['menu', 'nav', 'dropdown', 'football', 'staff', 'roster']))
            
            # Check navigation elements
            for nav in nav_elements:
                for link in nav.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower()
                    
                    if not href:
                        continue
                        
                    # Skip if link contains negative keywords
                    if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                          for neg_keyword in negative_keywords):
                        continue
                        
                    if any(keyword in text.lower() or keyword in href.lower() 
                          for keyword in coaches_keywords):
                        full_url = urljoin(athletics_url, href)
                        logger.debug(f"Found potential coaches link: {full_url}")
                        potential_coaches_links.append(full_url)
                    elif any(keyword in text.lower() or keyword in href.lower() 
                            for keyword in roster_keywords):
                        full_url = urljoin(athletics_url, href)
                        logger.debug(f"Found potential roster link: {full_url}")
                        potential_roster_links.append(full_url)
            
            # Check all links if none found in navigation
            if not potential_coaches_links and not potential_roster_links:
                for link in soup.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower()
                    
                    if not href:
                        continue
                        
                    # Skip if link contains negative keywords
                    if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                          for neg_keyword in negative_keywords):
                        continue
                        
                    if any(keyword in text.lower() or keyword in href.lower() 
                          for keyword in coaches_keywords):
                        full_url = urljoin(athletics_url, href)
                        logger.debug(f"Found potential coaches link: {full_url}")
                        potential_coaches_links.append(full_url)
                    elif any(keyword in text.lower() or keyword in href.lower() 
                            for keyword in roster_keywords):
                        full_url = urljoin(athletics_url, href)
                        logger.debug(f"Found potential roster link: {full_url}")
                        potential_roster_links.append(full_url)
            
            # Filter links to only include those with football/coaches keywords
            filtered_coaches_links = [link for link in potential_coaches_links 
                            if 'football' in link.lower() or 'coaches' in link.lower()]
            
            if filtered_coaches_links:
                logger.info("Potential football coaches links found")
                chosen_link = filtered_coaches_links[0]  # Keep query parameters
                logger.info(f"Found coaches directory link: {chosen_link}")
                return chosen_link
            elif potential_roster_links:
                logger.info("Potential roster links found")
                chosen_link = potential_roster_links[0].split('?')[0]  # Remove query parameters
                logger.info(f"Found roster link: {chosen_link}")
                return chosen_link
            elif potential_coaches_links:
                logger.info("Potential non-football coaches links found")
                chosen_link = potential_coaches_links[0]  # Keep query parameters
                logger.info(f"Found general coaches directory link: {chosen_link}")
                return chosen_link
            
            # If standard parsing fails, try AI parsing
            if self.ai_parser:
                logger.info(f"Standard parsing failed to find coaches page, trying AI parsing")
                
                prompt = f"""
                Find the URL for the football coaches directory, staff directory, or team roster page from this athletics website.
                Return just the URL as a string.
                """
                
                result = self.ai_parser.parse_html(html_content, prompt)
                
                if result and result.startswith('http'):
                    logger.info(f"AI parsing found coaches/roster page: {result}")
                    return result
                elif result:
                    full_url = urljoin(athletics_url, result)
                    logger.info(f"AI parsing found coaches/roster page (converted to absolute URL): {full_url}")
                    return full_url
            
            logger.warning(f"Could not find coaches or roster page for {athletics_url}")
            return None
            
        except Exception as e:
            logger.error(f"Error processing {athletics_url}: {str(e)}")
            return None
    
    def extract_coach_listings_bs4(self, html_content: str, base_url: str) -> List[Dict[str, Any]]:
        """
        Extract coach listings from HTML using BeautifulSoup.
        
        Args:
            html_content: HTML content of the coaches page
            base_url: Base URL for resolving relative links
            
        Returns:
            List of dictionaries containing coach information
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        coaches = []


        # 1. Try to find a sidearm-table with coach rows
        sidearm_tables = soup.find_all('table', class_='sidearm-table')
        for table in sidearm_tables:
            rows = table.find_all('tr', class_='sidearm-coaches-coach')
            for row in rows:
                try:
                    cells = row.find_all(['td', 'th'])
                    # Image
                    image_url = None
                    img_tag = cells[0].find('img') if len(cells) > 0 else None
                    if img_tag and img_tag.has_attr('src'):
                        image_url = img_tag['src']
                    # Name and profile
                    name = None
                    profile_url = None
                    if len(cells) > 1:
                        name_cell = cells[1]
                        name_link = name_cell.find('a')
                        if name_link:
                            name = name_link.get_text(strip=True)
                            profile_url = urljoin(base_url, name_link['href'])
                        else:
                            name = name_cell.get_text(strip=True)
                    # Position
                    position = cells[2].get_text(strip=True) if len(cells) > 2 else None
                    # Email
                    email = None
                    if len(cells) > 3:
                        email_link = cells[3].find('a', href=lambda x: x and 'mailto:' in x)
                        if email_link:
                            email = email_link['href'].replace('mailto:', '')
                    # Phone
                    phone = None
                    if len(cells) > 4:
                        # Try <a href="tel:..."> first
                        phone_link = cells[4].find('a', href=lambda x: x and 'tel:' in x)
                        if phone_link:
                            phone = phone_link.get_text(strip=True)
                        else:
                            # Fallback to any text in the cell
                            phone = cells[4].get_text(strip=True)
                    # Build coach dict
                    coach = {
                        'name': name,
                        'position': position,
                        'sport': 'Football',
                        'profile_url': profile_url,
                        'email': email,
                        'phone': phone,
                        'image_url': image_url
                    }
                    # Only add if we have a name
                    if name:
                        coaches.append(coach)
                except Exception as e:
                    logger.warning(f"Error parsing sidearm coach row: {e}")
                    continue

        # If we found coaches in sidearm-table, return them immediately
        if coaches:
            return coaches

        
        # This is a generic implementation that tries various common patterns
        # In a real implementation, you might need to customize this based on the specific website structure
        
        # Try to find coach cards/containers
        coach_elements = []
        
        # Common patterns for coach containers
        container_selectors = [
            '.coach-card', '.staff-card', '.directory-item', '.staff-member',
            '.coach-profile', '.staff-profile', '.bio-card', '.personnel-card',
            '.coach', '.staff', '.directory-listing', '.staff-listing',
            'div.coach', 'div.staff', 'div.directory', 'div.personnel'
        ]
        
        # Try each selector until we find something
        for selector in container_selectors:
            elements = soup.select(selector)
            if elements:
                coach_elements = elements
                logger.info(f"Found {len(elements)} coach elements using selector: {selector}")
                break
        
        # If we didn't find any elements with the selectors, try a more generic approach
        if not coach_elements:
            # Look for elements that might contain coach information
            # This is a more aggressive approach that might return false positives
            logger.info("No coach elements found with standard selectors, trying generic approach")
            

            
            # Look for elements that contain common coach information patterns
            for element in soup.find_all(['div', 'li', 'article']):
                # Check if this element might be a coach card
                text = element.get_text().lower()
                if ('coach' in text or 'staff' in text) and ('email' in text or 'phone' in text or '@' in text):
                    coach_elements.append(element)
        
        logger.info(f"Found {len(coach_elements)} potential coach elements")
        
        # Process each coach element
        for element in coach_elements:
            try:
                # Extract name - look for headings first
                name_element = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
                name = name_element.get_text().strip() if name_element else None
                
                if not name:
                    # Try to find a name in the element's text
                    text_blocks = [t.strip() for t in element.stripped_strings]
                    if text_blocks:
                        name = text_blocks[0]  # Assume the first text block is the name
                
                # Extract position/title
                position = None
                position_keywords = ['head coach', 'assistant coach', 'coach', 'director', 'coordinator']
                
                for p in element.find_all(['p', 'div', 'span']):
                    text = p.get_text().lower().strip()
                    if any(keyword in text for keyword in position_keywords):
                        position = p.get_text().strip()
                        break
                
                # Extract sport
                sport = None
                sport_keywords = ['basketball', 'football', 'soccer', 'baseball', 'softball', 
                                 'volleyball', 'tennis', 'golf', 'track', 'swimming']
                
                for p in element.find_all(['p', 'div', 'span']):
                    text = p.get_text().lower().strip()
                    if any(keyword in text for keyword in sport_keywords):
                        for keyword in sport_keywords:
                            if keyword in text:
                                sport = keyword.title()
                                break
                        if sport:
                            break
                
                # Extract profile URL
                profile_url = None
                profile_link = element.find('a')
                
                if profile_link and 'href' in profile_link.attrs:
                    href = profile_link['href']
                    # Make sure it's an absolute URL
                    if href.startswith('http'):
                        profile_url = href
                    else:
                        profile_url = urljoin(base_url, href)
                
                # Extract email
                email = None
                email_element = element.find('a', href=lambda href: href and 'mailto:' in href)
                
                if email_element:
                    email = email_element['href'].replace('mailto:', '')
                
                # Extract phone
                phone = None
                phone_element = element.find('a', href=lambda href: href and 'tel:' in href)
                
                if phone_element:
                    phone = phone_element['href'].replace('tel:', '')
                
                # Only add if we have at least a name
                if name:
                    coach = {
                        'name': name,
                        'position': position,
                        'sport': sport,
                        'profile_url': profile_url,
                        'email': email,
                        'phone': phone
                    }
                    coaches.append(coach)
            
            except Exception as e:
                logger.warning(f"Error extracting coach data: {e}")
                continue
        
        return coaches
    
    def extract_coach_bio_bs4(self, html_content: str, coach_name: str, school_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract coach bio information from HTML using BeautifulSoup.
        
        Args:
            html_content: HTML content of the coach's profile page
            coach_name: Name of the coach for context
            school_name: Optional name of the school for context
            
        Returns:
            Dictionary containing coach bio information
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        bio_data = {}
        
        # This is a generic implementation that tries various common patterns
        # In a real implementation, you might need to customize this based on the specific website structure
        
        # Extract full bio text
        bio_selectors = [
            '.bio', '.biography', '.coach-bio', '.staff-bio', 
            '.profile-bio', '.about', '.description', '.coach-profile'
        ]
        
        for selector in bio_selectors:
            bio_element = soup.select_one(selector)
            if bio_element:
                bio_data['bio'] = bio_element.get_text().strip()
                break
        
        # If no bio found with selectors, try a more generic approach
        if 'bio' not in bio_data:
            # Look for paragraphs that might contain bio information
            paragraphs = []
            for p in soup.find_all('p'):
                text = p.get_text().strip()
                if len(text) > 100:  # Assume longer paragraphs are part of the bio
                    paragraphs.append(text)
            
            if paragraphs:
                bio_data['bio'] = '\n\n'.join(paragraphs)
        
        # Extract education
        education = []
        education_keywords = ['education', 'degree', 'graduate', 'university', 'college', 'school']
        
        # Look for education section
        for heading in soup.find_all(['h2', 'h3', 'h4', 'strong']):
            heading_text = heading.get_text().lower()
            if any(keyword in heading_text for keyword in education_keywords):
                # Found an education section, get the following elements
                education_section = []
                for sibling in heading.find_next_siblings():
                    if sibling.name in ['h2', 'h3', 'h4']:  # Stop at the next heading
                        break
                    if sibling.name in ['p', 'li', 'div']:
                        education_section.append(sibling.get_text().strip())
                
                if education_section:
                    education.extend(education_section)
                    break
        
        if education:
            bio_data['education'] = education
        
        # Extract experience
        experience = []
        experience_keywords = ['experience', 'career', 'coaching', 'history', 'previous']
        
        # Look for experience section
        for heading in soup.find_all(['h2', 'h3', 'h4', 'strong']):
            heading_text = heading.get_text().lower()
            if any(keyword in heading_text for keyword in experience_keywords):
                # Found an experience section, get the following elements
                experience_section = []
                for sibling in heading.find_next_siblings():
                    if sibling.name in ['h2', 'h3', 'h4']:  # Stop at the next heading
                        break
                    if sibling.name in ['p', 'li', 'div']:
                        experience_section.append(sibling.get_text().strip())
                
                if experience_section:
                    experience.extend(experience_section)
                    break
        
        if experience:
            bio_data['experience'] = experience
        
        # Extract contact information
        # Email
        email_element = soup.find('a', href=lambda href: href and 'mailto:' in href)
        if email_element:
            bio_data['email'] = email_element['href'].replace('mailto:', '')
        
        # Phone
        phone_element = soup.find('a', href=lambda href: href and 'tel:' in href)
        if phone_element:
            bio_data['phone'] = phone_element['href'].replace('tel:', '')
        
        return bio_data
    
    def scrape_coach_roster(self, school_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scrape the coaching roster for a school's football team.
        
        Args:
            school_data: Dictionary containing school information
            
        Returns:
            List of dictionaries containing coach information
        """
        school_name = school_data.get('school_name', '')
        athletics_url = school_data.get('athletics_url', '')
        
        if not athletics_url:
            logger.error(f"No athletics URL provided for {school_name}")
            return []
        
        logger.info(f"Scraping football coach roster for {school_name} from {athletics_url}")
        
        # Find the football team page
        football_url = self.find_football_team_page(athletics_url)
        
        if not football_url:
            logger.error(f"Could not find football team page for {school_name}")
            return []
        
        logger.info(f"Found football team URL: {football_url}")
        
        # Find the coaches page from the football team page
        coaches_url = self.find_coaches_page(football_url)
        
        if not coaches_url:
            logger.error(f"Could not find coaches page for {school_name} football team")
            return []
        
        logger.info(f"Found football coaches URL: {coaches_url}")
        
        # Get the coaches page content
        html_content = self.get_page(coaches_url)
        
        if not html_content:
            logger.error(f"Could not fetch coaches page: {coaches_url}")
            return []
        
        # Extract coach listings with fallback to AI
        try:
            # Try standard parsing first
            coaches = self.extract_coach_listings_bs4(html_content, coaches_url)
        except Exception as e:
            logger.error(f"Standard parsing failed: {str(e)}")
            if self.ai_parser:
                # Try AI parsing as fallback
                coaches = self.ai_parser.extract_coach_roster(html_content, school_name)
            else:
                logger.error("AI parsing not available")
                coaches = []
        
        logger.info(f"Found {len(coaches)} football coaches for {school_name}")
        
        # Add school information to each coach
        for coach in coaches:
            coach['school_id'] = school_data.get('school_id')
            coach['school_name'] = school_name
            coach['division'] = school_data.get('division')
            coach['conference'] = school_data.get('conference')
            coach['sport'] = 'Football'  # Explicitly set sport to Football
        
        return coaches
    
    def find_coaches_or_roster_link(self, url: str) -> str:
        """
        Searches an athletic website for links to football coaches directory or roster.
        
        Args:
            url: The athletic website URL
        
        Returns:
            The coaches directory URL if found, roster URL if no coaches found, "Not found" otherwise
        """
        try:
            logging.info(f"Processing athletic website URL: {url}")
            
            # Add https:// if not present
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
                logging.debug(f"Added https:// prefix: {url}")

            # Add headers to mimic a browser
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, 
                                timeout=60, 
                                headers=headers, 
                                verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Keywords for both coaches and roster pages
            coaches_keywords = ['coaches', 'coaching staff', 'staff directory', 'football staff', 
                            'coaches & staff', 'coaches and staff', 'staff', 'directory']
            
            roster_keywords = ['roster', 'team roster', 'football roster', 'players']
            negative_keywords = ['soccer', 'baseball', 'basketball', 'volleyball', 'softball', 'lacrosse', 
                            'tennis', 'golf', 'swimming', 'diving', 'track', 'field', 'cross country',
                            'wrestling', 'hockey', 'rowing', 'gymnastics', 'rugby', 'bowling', 'fencing',
                            'water polo', 'beach volleyball', 'field hockey', 'skiing', 'equestrian', 'esports',
                            'dance-team','cheerleading','spirit', 'news', 'article', 'press', 'release', 
                            'voice', 'tv', 'clinic', 'family', 'camp', 'youth', 'fan', 'shop', 'store', 'tickets',
                            'cross-country', 'department', 'cycling', 'wrest', 'equest', 'fh', 'secure'
                            'secure', 'administration', 'medicine', 'pdf', 'corner', 'resources'
                            'login', 'register', 'sign-up', 'sign in', 'sign-in', 'log in', 'log-in',
                            'mvball', 'wvball', 'm-baskbl', 'w-baskbl', 'm-soc', 'w-soc', 'm-lax', 'w-lax',
                            'wwrest', 'm-wrest', 'm-gym', 'w-gym', 'm-swim', 'w-swim', 'm-ten', 'w-ten',
                            'roger', 'access', 'roque', 'jc', 'sail', 'crew', 'crew', 'sailing',
                            ]
            
            # Add sport abbreviations to negative keywords
            sport_abbreviations = [
                'bsb', 'mbkb', 'wbkb', 'mxc', 'wxc', 'mlax', 'wlax', 'msoc', 'wsoc', 'sball',
                'mswimdive', 'wswimdive', 'mten', 'wten', 'wvball', 'w-baskbl', 'm-xc', 'w-xc',
                'w-softbl', 'm-baskbl', 'm-basebl', 'w-volley', 'c-swim', 'social',
                'w-baskbl', 'w-gym', 'w-swim', 'm-gym', 'm-swim', 'm-lax', 'w-lax', 'm-soc', 'w-soc',
                'm-ten', 'w-ten', 'm-golf', 'w-golf', 'm-track', 'w-track', 'm-fieldhockey',
                'w-fieldhockey', 'm-wrestling', 'w-wrestling', 'm-rugby', 'w-rugby', 'm-rowing',
                'w-rowing', 'm-gymnastics', 'w-gymnastics', 'm-equestrian', 'w-equestrian',
                'm-waterpolo', 'w-waterpolo', 'm-beachvolleyball', 'w-beachvolleyball',
                'm-skiing', 'w-skiing', 'm-fencing', 'w-fencing', 'm-bowling', 'w-bowling',
                'm-dance', 'w-dance', 'm-cheerleading', 'w-cheerleading', 'm-esports', 'w-esports',
                'm-voice', 'w-voice', 'm-tv', 'w-tv', 'm-clinic', 'w-clinic', 'm-family', 'w-family',
            ]
            negative_keywords.extend([abbr for abbr in sport_abbreviations])
            
            potential_coaches_links = []
            potential_roster_links = []
            
            # Look for navigation menus first
            nav_elements = soup.find_all(['nav', 'div', 'ul'], 
                                    class_=lambda x: x and any(word in x.lower() 
                                    for word in [
                                        'menu', 'nav', 'dropdown', 
                                        'coach', 'coaches', 
                                        'staff', 'football',
                                        'roster' , 'rosters'
                                    ]))
            
            # Check navigation elements
            for nav in nav_elements:
                for link in nav.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower()
                    
                    if not href:
                        continue
                        
                    # Skip if link contains negative keywords
                    if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                          for neg_keyword in negative_keywords):
                        continue

                    # Skip URLs with date patterns (like /2019/7/23/ or /2019-7-23/)
                    if re.search(r'/\d{4}/\d{1,2}/\d{1,2}/', href) or re.search(r'/\d{4}-\d{1,2}-\d{1,2}/', href):
                        continue

                    # More precise matching for coaches links
                    if any(keyword in text.lower() or keyword in href.lower() 
                        for keyword in coaches_keywords):
                        # Additional check to ensure it's a coaches page
                        if 'coach' in text.lower() or 'staff' in text.lower() or 'coach' in href.lower() or 'staff' in href.lower():
                            full_url = urljoin(url, href)
                            logging.info(f"Found potential coaches link: {full_url}")
                            potential_coaches_links.append(full_url)
                    # More precise matching for roster links
                    elif any(keyword in text.lower() or keyword in href.lower() 
                            for keyword in roster_keywords):
                        # Additional check to ensure it's a roster page
                        if 'roster' in text.lower() or 'roster' in href.lower() or 'players' in text.lower() or 'players' in href.lower():
                            full_url = urljoin(url, href)
                            logging.info(f"Found potential roster link: {full_url}")
                            potential_roster_links.append(full_url)
            
            # Check all links if none found in navigation
            if not potential_coaches_links and not potential_roster_links:
                for link in soup.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower()
                    
                    if not href:
                        continue

                    # Skip if link contains negative keywords
                    if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                          for neg_keyword in negative_keywords):
                        continue

                    # Skip URLs with date patterns (like /2019/7/23/ or /2019-7-23/)
                    if re.search(r'/\d{4}/\d{1,2}/\d{1,2}/', href) or re.search(r'/\d{4}-\d{1,2}-\d{1,2}/', href):
                        continue

                    # More precise matching for coaches links
                    if any(keyword in text.lower() or keyword in href.lower() 
                        for keyword in coaches_keywords):
                        # Additional check to ensure it's a coaches page
                        if 'coach' in text.lower() or 'staff' in text.lower() or 'coach' in href.lower() or 'staff' in href.lower():
                            full_url = urljoin(url, href)
                            logging.info(f"Found potential coaches link: {full_url}")
                            potential_coaches_links.append(full_url)
                    # More precise matching for roster links
                    elif any(keyword in text.lower() or keyword in href.lower() 
                            for keyword in roster_keywords):
                        # Additional check to ensure it's a roster page
                        if 'roster' in text.lower() or 'roster' in href.lower() or 'players' in text.lower() or 'players' in href.lower():
                            full_url = urljoin(url, href)
                            logging.info(f"Found potential roster link: {full_url}")
                            potential_roster_links.append(full_url)
            
            # Filter links to only include those with football/coaches keywords
            filtered_coaches_links = [link for link in potential_coaches_links 
                            if 'football' in link.lower() or 'coaches' in link.lower()
                        ]
                            
              
            if filtered_coaches_links:
                logging.info("\nPotential coaches links found:")
                for link in filtered_coaches_links:
                    logging.info(f"- {link}")
                chosen_link = filtered_coaches_links[0]
                logging.info(f"Found coaches directory link: {chosen_link}")
                return chosen_link
            else:
                # Try simulated links if no links found through standard scraping
                logging.info("No links found through standard scraping, trying simulated links")
                
                # Try coaches simulation first
                simulated_coaches_link = self.get_simulate_coaches_link(url)
                if simulated_coaches_link:
                    logging.info(f"Found coaches link through simulation: {simulated_coaches_link}")
                    return simulated_coaches_link
                
                
            if potential_roster_links:
                logging.info("\nPotential roster links found:")
                for link in potential_roster_links:
                    logging.info(f"- {link}")
                chosen_link = potential_roster_links[0].split('?')[0]
                logging.info(f"Found roster link: {chosen_link}")
                return chosen_link
            else:
                logging.info("No links found through standard scraping, trying simulated links")

                simulated_roster_link = self.get_simulate_roster_link(url)
                if simulated_roster_link:
                    logging.info(f"Found roster link through simulation: {simulated_roster_link}")
                    return simulated_roster_link
                
                
            logging.info(f"No coaches or roster link found for {url}")
            return "Not found"
            
        except Exception as e:
            logging.error(f"Error processing {url}: {str(e)}")
            return "Not found"

    def find_football_link(self, url: str) -> str:
        """
        Searches an athletics website for links to their football program page.
        
        Args:
            url: The athletics website URL
        
        Returns:
            The football program URL if found, "Not found" otherwise
        """
        try:
            logging.info(f"Processing athletics URL: {url}")
            
            # Add https:// if not present
            if url.startswith('//'):
                url = url.replace('//', '') #  "athleticWebUrl": "//lrtrojans.com", ????

            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
                logging.debug(f"Added https:// prefix: {url}")
                
            # Add headers to mimic a browser
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, 
                                timeout=60, 
                                headers=headers, 
                                verify=False)
            response.raise_for_status()
                    
            soup = BeautifulSoup(response.text, 'html.parser')
            
            final_url = response.url
            logging.info(f"Final redirected URL: {final_url}")
            
            if url != final_url:
                logging.info(f"Redirected from {url} to {final_url}")
                url = final_url
            
            # Check for splash page and skip link
            skip_link = soup.find('a', href=lambda href: href and 'skip=true' in href)
            if skip_link:
                skip_url = urljoin(url, skip_link['href'])
                logging.info(f"Found skip link, following: {skip_url}")
                return self.find_football_link(skip_url)
            
            # Keywords specific to football program pages
            keywords = ['football', 'football program', 'fball', 'football team', 'football home']
            negative_keywords = ['soccer', 'baseball', 'basketball', 'volleyball', 'softball', 'lacrosse', 
                            'tennis', 'golf', 'swimming', 'diving', 'track', 'field', 'cross country',
                            'wrestling', 'hockey', 'rowing', 'gymnastics', 'rugby', 'bowling', 'fencing',
                            'water polo', 'beach volleyball', 'field hockey', 'skiing', 'equestrian',
                            'tickets', 'season tickets', 'ticket office', 'box office', 'purchase tickets',
                            'buy tickets', 'ticket sales', 'ticket information', 'seasontickets', 
                            'podcast', 'Product', 'shop', 'store', 'tickets', 'team store', 'merchandise',
                            'php', 'women', 'vice', 'hype', 'season', 'schedule', 'news', 'article',
                            'output', 'sb'
                ]
            
            # Add sport abbreviations to negative keywords
            sport_abbreviations = [
                'bsb',      # baseball
                'mbkb',     # men's basketball
                'wbkb',     # women's basketball
                'mxc',      # men's cross country
                'wxc',      # women's cross country
                'mlax',     # men's lacrosse
                'wlax',     # women's lacrosse
                'msoc',     # men's soccer
                'wsoc',     # women's soccer
                'sball',    # softball
                'mswimdive', # men's swimming/diving
                'wswimdive', # women's swimming/diving
                'mten',     # men's tennis
                'wten',     # women's tennis
                'wvball',   # women's volleyball
                'w-baskbl', 'm-xc', 'w-xc', 'w-softbl', 'm-baskbl', 'm-basebl', 'w-volley', 'c-swim', 'social'
            ]
            negative_keywords.extend([abbr for abbr in sport_abbreviations])
            
            football_pattern = re.compile(r'\b(football|fball|football[\s\-]?home|football[\s\-]?team|football[\s\-]?program)\b', re.IGNORECASE)
            potential_links = []
            
            # Look for navigation menus first
            nav_elements = soup.find_all(['nav', 'div', 'ul'], 
                                    class_=lambda x: x and any(word in x.lower() 
                                    for word in ['menu', 'nav', 'dropdown', 'sports', 'athletics']))
            
            # Check navigation elements
            for nav in nav_elements:
                for link in nav.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower()
                    
                    if not href:
                        continue
                    
                    # Skip if link contains negative keywords
                    if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                        for neg_keyword in negative_keywords):
                        continue
                        
                    if any(keyword in text.lower() or keyword in href.lower() 
                        for keyword in keywords):
                        full_url = urljoin(url, href)
                        potential_links.append(full_url)

            # Check all links if none found in navigation
            if not potential_links:
                # First check for football pattern in the page
                if football_pattern.search(response.text):
                    print('Football Pattern Found')
                    # Look for links containing football
                    for link in soup.find_all('a'):
                        href = link.get('href')
                        text = link.get_text().lower()
                        
                        if not href:
                            continue

                        # Skip if link contains negative keywords
                        if any(neg_keyword in text.lower() or neg_keyword in href.lower() 
                            for neg_keyword in negative_keywords):
                            continue
                            
                        if any(keyword in text.lower() or keyword in href.lower() 
                            for keyword in keywords):
                            full_url = urljoin(url, href)
                            potential_links.append(full_url)
    
            if potential_links:
                chosen_link = potential_links[0].split('?')[0]  # Remove query parameters
                logging.info(f"Found football program link: {chosen_link}")
                return chosen_link
            
            # If no links found through standard scraping, try simulated links
            simulated_link = self.get_simulate_football_link(url)
            if simulated_link:
                return simulated_link
                    
            logging.info(f"No football program link found for {url}")
            return "Not found"
            
        except Exception as e:
            logging.error(f"Error processing {url}: {str(e)}")
            return "Not found"

    def scrape_coach_bios(self, coaches: List[Dict[str, Any]], school_name: str) -> List[Dict[str, Any]]:
        """
        Scrape detailed bios for each coach.
        
        Args:
            coaches: List of coach dictionaries with profile URLs
            school_name: Name of the school for context
            
        Returns:
            List of coach dictionaries with bio information added
        """
        coaches_with_bios = []
        
        for coach in coaches:
            name = coach.get('name', 'Unknown Coach')
            profile_url = coach.get('profile_url')
            
            if not profile_url:
                logger.warning(f"No profile URL for {name}, skipping bio scraping")
                coaches_with_bios.append(coach)
                continue
            
            logger.info(f"Scraping bio for {name} from {profile_url}")
            
            # Get the profile page content
            html_content = self.get_page(profile_url)
            
            if not html_content:
                logger.error(f"Could not fetch profile page: {profile_url}")
                coaches_with_bios.append(coach)
                continue
            
            # Extract bio information with fallback to AI
            bio_data = parse_with_fallback(
                html_content,
                self.extract_coach_bio_bs4,
                self.ai_parser.extract_coach_bio,
                name,        # coach_name for both parsers
                school_name  # school_name for AI parser
            )
            
            # Merge bio data with coach data
            coach_with_bio = {**coach, **bio_data}
            coaches_with_bios.append(coach_with_bio)
            
            logger.info(f"Successfully scraped bio for {name}")
            
            # Add a small delay to avoid overloading the server
            time.sleep(random.uniform(1, 3))
        
        return coaches_with_bios

# Convenience function for direct use
def scrape_school(school_data: Dict[str, Any], ai_endpoint_url: Optional[str] = None, 
                 ai_api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Scrape coach data for a school.
    
    Args:
        school_data: Dictionary containing school information
        ai_endpoint_url: Optional AI endpoint URL
        ai_api_key: Optional AI API key
        
    Returns:
        Dictionary containing the scraped data
    """
    # Initialize AI parser if credentials provided
    ai_parser = None
    if ai_endpoint_url and ai_api_key:
        ai_parser = AIParser(ai_endpoint_url, ai_api_key)
    
    # Initialize scraper
    scraper = NCAAScraper(ai_parser)
    
    # Scrape coach roster
    coaches = scraper.scrape_coach_roster(school_data)
    
    if not coaches:
        logger.error(f"No coaches found for {school_data.get('school_name', '')}")
        return {
            'school_data': school_data,
            'coaches': [],
            'success': False
        }
    
    # Scrape coach bios
    coaches_with_bios = scraper.scrape_coach_bios(coaches, school_data.get('school_name', ''))
    
    return {
        'school_data': school_data,
        'coaches': coaches_with_bios,
        'success': True
    }

# Add an alias for backward compatibility
CoachingScraper = NCAAScraper 

def scrape_coaches_from_roster_url(school_data: dict, roster_url: str, ai_endpoint_url: Optional[str] = None, ai_api_key: Optional[str] = None) -> dict:
    """
    Scrape coach data for a school using a pre-supplied coaches or roster URL.
    Args:
        school_data: Dictionary containing school information
        roster_url: The URL of the football roster/coaches page
        ai_endpoint_url: Optional AI endpoint URL
        ai_api_key: Optional AI API key
    Returns:
        Dictionary containing the scraped data (same format as scrape_school)
    """
    # Prefer coaches_url if present, otherwise use roster_url
    coaches_url = school_data.get('coaching_staff_url') or roster_url
    ai_parser = None
    if ai_endpoint_url and ai_api_key:
        ai_parser = AIParser(ai_endpoint_url, ai_api_key)
    scraper = NCAAScraper(ai_parser)
    # Get the page content directly from the chosen URL
    html_content = scraper.get_page(coaches_url)
    if not html_content:
        logger.error(f"Could not fetch coaches/roster page: {coaches_url}")
        return {
            'school_data': school_data,
            'coaches': [],
            'success': False
        }
    # Extract coach listings (skip URL discovery)
    coaches = scraper.extract_coach_listings_bs4(html_content, coaches_url)
    if not coaches:
        logger.error(f"No coaches found for {school_data.get('school_name', '')}")
        return {
            'school_data': school_data,
            'coaches': [],
            'success': False
        }
    # Scrape coach bios
    coaches_with_bios = scraper.scrape_coach_bios(coaches, school_data.get('school_name', ''))
    return {
        'school_data': school_data,
        'coaches': coaches_with_bios,
        'success': True
    }
