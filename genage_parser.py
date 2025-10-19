import os
import re
import time
import xml.etree.ElementTree as ET
from xml.dom import minidom

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import PATH_TO_GENAGE_MODEL_GENES

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GenAgeParser:
    def __init__(self, file_type: str, output_dir: str = "./"):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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
        self.output_dir = output_dir
        self.file_type = file_type

    def get_gene_info(self, hgnc_gene_name: str, model_organism: str = None) -> dict:
        """Get gene information and return as structured data"""
        try:
            if self.file_type == "human":
                url = f"https://genomics.senescence.info/genes/entry.php?hgnc={hgnc_gene_name}"

            else:
                url = f"https://genomics.senescence.info/genes/details.php?gene={hgnc_gene_name}&organism={model_organism}"
                print(url)

            time.sleep(2)
            response = self.session.get(url, headers=self.headers, verify=False)
            response.raise_for_status()

            return self._parse_gene_html(response.content, hgnc_gene_name)

        except Exception as error:
            print(f"Error processing {hgnc_gene_name}: {error}")
            return None

    def save_gene_info_to_xml(
        self, hgnc_gene_name: str, filename: str, organism: str = None
    ) -> bool:
        """Save gene information to XML file in a preprocessing-resistant format"""
        if self.file_type == "human":
            gene_info = self.get_gene_info(hgnc_gene_name)
        else:
            gene_info = self.get_gene_info(hgnc_gene_name, organism)
        if not gene_info:
            return False

        try:
            if filename is None:
                filename = os.path.join(self.output_dir, f"{hgnc_gene_name}.xml")
            else:
                if not os.path.isabs(filename):
                    filename = os.path.join(self.output_dir, filename)
            root = ET.Element("gene_info")

            name_elem = ET.SubElement(root, "gene_name")
            name_elem.text = hgnc_gene_name

            desc_elem = ET.SubElement(root, "description")
            desc_elem.text = gene_info["description"]

            # citations as separate elements for preservation
            citations_elem = ET.SubElement(root, "citations")
            for i, citation in enumerate(gene_info.get("citations", [])):
                cite_elem = ET.SubElement(citations_elem, "citation")
                cite_elem.set("id", f"cit{i + 1}")
                cite_elem.text = citation

            raw_text_elem = ET.SubElement(root, "raw_text")
            raw_text_elem.text = gene_info["description"]

            ET.ElementTree(root)

            # Pretty print the XML
            rough_string = ET.tostring(root, encoding="utf-8")
            reparsed = minidom.parseString(rough_string)
            pretty_xml = reparsed.toprettyxml(indent="  ")

            lines = pretty_xml.split("\n")
            clean_lines = [line for line in lines if line.strip()]

            with open(filename, "w", encoding="utf-8") as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                for line in clean_lines[
                    1:
                ]:  # Skip the first line (default declaration)
                    f.write(line + "\n")

            print(f"Successfully saved gene info to {filename}")
            return True

        except Exception as error:
            print(f"Error saving XML for {hgnc_gene_name}: {error}")
            return False

    def _clean_citation_text(self, text: str) -> str:
        """Clean and format citation text"""
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"[.,;:]$", "", text)
        if len(text) > 100:
            text = text[:97] + "..."
        return text

    def _extract_citation_from_page(
        self, soup: BeautifulSoup, citation_url: str
    ) -> str:
        """Extract citation information from citation page"""
        try:
            highlight_box = soup.select_one("p.highlight-box")
            if highlight_box:
                citation_text = highlight_box.get_text(strip=True)
                author_year_match = re.search(
                    r"^([A-Za-z][^\(]+?\s*(?:et al\.\s*)?\(\d{4}\))", citation_text
                )
                if author_year_match:
                    return self._clean_citation_text(author_year_match.group(1))

            entry_details = soup.find("h1", string="Entry details")
            if entry_details:
                next_p = entry_details.find_next("p")
                if next_p:
                    citation_text = next_p.get_text(strip=True)
                    author_year_match = re.search(r"^([^\(]+\(\d{4}\))", citation_text)
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
                    citation_url, headers=self.headers, timeout=30, verify=False
                )

                if response.status_code != 200:
                    print(f"HTTP {response.status_code} for {citation_url}")
                    return f"HTTP {response.status_code}"

                soup = BeautifulSoup(response.content, "html.parser")
                source_info = self._extract_citation_from_page(soup, citation_url)
                return source_info

            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {citation_url}: {e}")
                if attempt == max_retries - 1:
                    return f"Failed to fetch: {citation_url}"
                time.sleep(2)

        return f"Failed to fetch: {citation_url}"

    def _parse_gene_html(self, html_content: bytes, gene_name: str) -> dict:
        """Parse gene HTML content and extract information with resolved citations"""
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            if self.file_type == "human":
                title_div = soup.find(
                    "h2",
                    class_="section-header",
                    string="Potential relevance to the human ageing process",
                )
                if not title_div:
                    print(f"Aging relevance section not found for {gene_name}!")
                    return None

                section_entry = title_div.find_next("dl", class_="section-entry")
                if not section_entry:
                    print(f"Section entry not found for {gene_name}!")
                    return None

                description_dt = section_entry.find("dt", string="Description")
                if not description_dt:
                    print(f"Description section not found for {gene_name}!")
                    return None

                description_dd = description_dt.find_next_sibling("dd")
                if not description_dd:
                    print(f"Description content not found for {gene_name}!")
                    return None

                description_copy = BeautifulSoup(str(description_dd), "html.parser")
                citation_links = description_copy.find_all(
                    "a", href=re.compile(r"entries/entry/\d+")
                )
                citations = []

                for link in citation_links:
                    citation_number = link.get_text(strip=True)
                    if citation_number.isdigit():
                        href = link.get("href", "")
                        full_url = (
                            href
                            if href.startswith("http")
                            else f"https://libage.ageing-map.org/{href.lstrip('/')}"
                        )

                        source_info = self._fetch_citation_source(full_url)
                        citations.append(source_info)

                        source_span = BeautifulSoup(f" [{source_info}]", "html.parser")
                        link.replace_with(source_span)

                description_text = description_copy.get_text(" ", strip=True)

                description_text = description_text.replace("[[", "[").replace(
                    "]]", "]"
                )
                description_text = re.sub(r"\s+([.,!?;])", r"\1", description_text)
                description_text = re.sub(
                    r"([.,!?;])([A-Z])", r"\1 \2", description_text
                )
                description_text = re.sub(r"\s+", " ", description_text).strip()
                description_text = description_text.replace("[[", "[").replace(
                    "]]", "]"
                )
                return {
                    "gene_name": gene_name,
                    "description": description_text,
                    "citations": citations,
                }

            else:
                title_div = soup.find(
                    "h2",
                    class_="section-header",
                    string="Potential relevance to longevity and/or ageing",
                )
                if not title_div:
                    print(f"Aging relevance section not found for {gene_name}!")
                    return None
                section_entries = title_div.find_all_next("dl", class_="section-entry")
                if not section_entries:
                    print(f"Section entries not found for {gene_name}!")
                    return None

                all_observations = []
                citations = []

                for section_entry in section_entries:
                    observations_dt = section_entry.find("dt", string="Observations")
                    if observations_dt:
                        observations_dd = observations_dt.find_next_sibling("dd")
                        if observations_dd:
                            observations_text = observations_dd.get_text(
                                " ", strip=True
                            )

                            observations_text += " [GenAge]"

                            all_observations.append(observations_text)

                if not all_observations:
                    print(f"No observations found for {gene_name}!")
                    return None

                combined_description = " ".join(all_observations)

                combined_description = re.sub(
                    r"\s+([.,!?;])", r"\1", combined_description
                )
                combined_description = re.sub(
                    r"([.,!?;])([A-Z])", r"\1 \2", combined_description
                )
                combined_description = re.sub(r"\s+", " ", combined_description).strip()
                combined_description = re.sub(
                    r"\s*\[\s*GenAge\s*\]\s*", " [GenAge] ", combined_description
                )
                combined_description = re.sub(r"\s+", " ", combined_description).strip()

                return {
                    "gene_name": gene_name,
                    "description": combined_description,
                    "citations": ["GenAge"],
                }

        except Exception as error:
            print(f"Error parsing HTML for {gene_name}: {error}")
            return None

    def _preprocess_xml(self, xml_content: str) -> str:
        """Preprocess XML content - this function will now preserve your text"""
        try:
            soup = BeautifulSoup(xml_content, "xml")

            description = soup.find("description")
            if description:
                text = description.get_text(separator=" ", strip=True)
            else:
                raw_text = soup.find("raw_text")
                if raw_text:
                    text = raw_text.get_text(separator=" ", strip=True)
                else:
                    text = soup.get_text(separator=" ", strip=True)

            text = re.sub(r"\s+", " ", text)

            # remove citation numbers
            # text = re.sub(r'\[\d+\]', '', text)  # Remove this line

            text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

            return text.strip()

        except Exception as error:
            print(f"Error in preprocessing files! {error}")
            return xml_content


def get_gene_names(csv_file_path: str, file_type: str) -> list:
    df = pd.read_csv(csv_file_path)
    gene_names = df["symbol"].tolist()
    organisms = df["organism"].tolist()

    if file_type == "model":
        organism_mapping = {
            "Caenorhabditis elegans": "elegans",
            "Drosophila melanogaster": "melanogaster",
            "Mus musculus": "musculus",
            "Saccharomyces cerevisiae": "cerevisiae",
        }
        shortened_organisms = []
        for org in organisms:
            org_stripped = org.strip()
            shortened_organisms.append(organism_mapping.get(org_stripped, org))

        return gene_names, shortened_organisms
    else:
        return gene_names


if __name__ == "__main__":
    # for human genes uncomment
    # gene_list = get_gene_names(PATH_TO_GENAGE_HUMAN_GENES, file_type='human')

    gene_list, org_list = get_gene_names(PATH_TO_GENAGE_MODEL_GENES, file_type="model")

    # gene_name = 'aak-2'
    # organism = 'elegans'
    parser = GenAgeParser(file_type="model", output_dir="./data/data_genage_model/")
    for gene_name, organism in zip(gene_list, org_list):
        parser.save_gene_info_to_xml(gene_name, f"{gene_name}.xml", organism)

    # debug
    # if success:
    #    with open(f'{gene_name}.xml', 'r', encoding='utf-8') as f:
    #        xml_content = f.read()
    #
    #    processed_text = parser._preprocess_xml(xml_content)
    #    print("Processed text:")
    #    print(processed_text)
