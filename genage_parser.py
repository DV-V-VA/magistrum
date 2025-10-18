import os
import re
import time
import requests
import urllib.request
from urllib.error import URLError
from bs4 import BeautifulSoup
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GenAgeParser:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def prepare_html(self, hgnc_gene_name: str) -> str:
        """Download and save gene HTML page"""
        filename = f'{hgnc_gene_name}.html'
        try:
            url = f'https://genomics.senescence.info/genes/entry.php?hgnc={hgnc_gene_name}'
            target = urllib.request.Request(url, headers=self.headers)
            time.sleep(2)
            
            with urllib.request.urlopen(target) as f:
                with open(filename, 'wb') as of:
                    of.write(f.read())

            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                return filename
            else:
                raise ValueError('Downloaded file is empty or was not created')

        except URLError as e:
            raise ValueError(f"Network error: {e}")
        except Exception as error:
            print(f"Error downloading {hgnc_gene_name}: {error}")
            return None

    def _clean_citation_text(self, text: str) -> str:
        """Clean and format citation text"""
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[.,;:]$', '', text)
        if len(text) > 100:
            text = text[:97] + "..."
        return text

    def _extract_citation_from_page(self, soup: BeautifulSoup, citation_url: str) -> str:
        """Extract citation information from citation page"""
        try:

            highlight_box = soup.select_one('p.highlight-box')
            if highlight_box:
                citation_text = highlight_box.get_text(strip=True)

                author_year_match = re.search(r'^([A-Za-z][^\(]+?\s*(?:et al\.\s*)?\(\d{4}\))', citation_text)
                if author_year_match:
                    return self._clean_citation_text(author_year_match.group(1))


            entry_details = soup.find('h1', string='Entry details')
            if entry_details:
                next_p = entry_details.find_next('p')
                if next_p:
                    citation_text = next_p.get_text(strip=True)
                    author_year_match = re.search(r'^([^\(]+\(\d{4}\))', citation_text)
                    if author_year_match:
                        return self._clean_citation_text(author_year_match.group(1))

            return f"Source: {citation_url}"

        except Exception as e:
            print(f"Error extracting citation: {e}")
            return f"Source: {citation_url}"

    def _fetch_citation_source(self, citation_url: str, max_retries: int = 2) -> str:
        """Fetch citation source with SSL verification disabled"""
        for attempt in range(max_retries):
            try:
                time.sleep(1)
                

                response = self.session.get(
                    citation_url, 
                    headers=self.headers, 
                    timeout=30,
                    verify=False  #disables SSL certificate verification
                )
                
                if response.status_code != 200:
                    print(f"HTTP {response.status_code} for {citation_url}")
                    return f"HTTP {response.status_code}"

                soup = BeautifulSoup(response.content, 'html.parser')
                source_info = self._extract_citation_from_page(soup, citation_url)
                return source_info

            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {citation_url}: {e}")
                if attempt == max_retries - 1:
                    return f"Failed to fetch: {citation_url}"
                time.sleep(2)

        return f"Failed to fetch: {citation_url}"

    def save_info(self, filename: str):
        """Extract gene information with resolved citations"""
        if not filename or not os.path.exists(filename):
            print(f"File not found: {filename}")
            return None

        try:
            with open(filename, 'r', encoding='utf-8') as fp:
                soup = BeautifulSoup(fp, "html.parser")

            title_div = soup.find('h2', class_='section-header', 
                                string='Potential relevance to the human ageing process')
            if not title_div:
                print("Aging relevance section not found!")
                return None

            section_entry = title_div.find_next('dl', class_='section-entry')
            if not section_entry:
                print("Section entry not found!")
                return None

            description_dt = section_entry.find('dt', string='Description')
            if not description_dt:
                print("Description section not found!")
                return None

            description_dd = description_dt.find_next_sibling('dd')
            if not description_dd:
                print("Description content not found!")
                return None

            description_copy = BeautifulSoup(str(description_dd), 'html.parser')
            citation_links = description_copy.find_all('a', href=re.compile(r'entries/entry/\d+'))
            citation_map = {}

            for link in citation_links:
                citation_number = link.get_text(strip=True)
                if citation_number.isdigit():
                    href = link.get('href', '')
                    full_url = href if href.startswith('http') else f"https://libage.ageing-map.org/{href.lstrip('/')}"

                    #print(f"Processing citation [{citation_number}]: {full_url}")
                    source_info = self._fetch_citation_source(full_url)
                    citation_map[citation_number] = source_info


                    source_span = BeautifulSoup(f' [{source_info}]', 'html.parser')
                    link.replace_with(source_span)


            description_text = description_copy.get_text(" ", strip=True)
            

            description_text = re.sub(r'\s+([.,!?;])', r'\1', description_text)
            description_text = re.sub(r'([.,!?;])([A-Z])', r'\1 \2', description_text)
            description_text = re.sub(r'\s+', ' ', description_text).strip()
            description_text = description_text.replace(' [ [', ' [').replace('] ]', ']')

            return description_text

        except Exception as error:
            print(f"Error processing {filename}: {error}")
            return None


if __name__ == "__main__":
    gene_name = 'ABL1'
    parser = GenAgeParser()
    filename = parser.prepare_html(gene_name)
    print(parser.save_info(filename))
    
    # debug print
    #if filename:
    #    result = parser.save_info(filename)
    #    if result:
    #        description, citations = result
    #        print("Description:", description)
    #        if citations:
    #            print("\nCitations:")
    #            for num, source in citations.items():
    #                print(f"  [{num}]: {source}")