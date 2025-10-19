import concurrent.futures
import logging
import os
import re
import shutil
from concurrent.futures import ProcessPoolExecutor

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from llama_index.core import Settings, StorageContext, VectorStoreIndex
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import Document
from llama_index.embeddings.nebius import NebiusEmbedding
from llama_index.llms.nebius import NebiusLLM
from llama_index.vector_stores.milvus import MilvusVectorStore
from pymilvus import MilvusClient

from config import (
    PATH_TO_GENAGE_PARSED_GENES,
    PATH_TO_LOGS,
    PATH_TO_PARSED_TEXTS,
    PATH_TO_RAG,
)
from logging_config import setup_logging

# PATH_TO_LOGS = os.path.join(tempfile.gettempdir(), "aging_llm.log")
setup_logging(PATH_TO_LOGS)
logger = logging.getLogger(__name__)


class AgingLLM:
    def __init__(self, gene_name):
        self.gene_name = gene_name
        self.EMBEDDING_MODEL = "Qwen/Qwen3-Embedding-8B"
        self.EMBEDDING_LENGTH = 4096
        self.DB_URI = f"{PATH_TO_RAG}/{gene_name}/rag.db"
        self.COLLECTION_NAME = f"{gene_name}_rag"
        self._cached_index = None

    def _check_context_usage(self, index: VectorStoreIndex) -> None:
        """Test if model uses context by asking unrelated question"""
        query_engine = index.as_query_engine()
        test_response = query_engine.query("When was the moon landing?")
        logger.info(f"Context test: {test_response}")

    def _create_gene_prompt(self) -> str:
        """Create structured prompt for gene analysis"""
        return f"""
        You are a genomics expert analyzing documents for gene-aging relationships.
        Extract and summarize all information about the gene {self.gene_name} from the
        provided context.
        Focus primarily on its relation to aging, longevity, or age-related processes.

        Structure your response exactly as follows:
        1. Gene Overview: Full name, function, location (chromosome),
        protein product, and key pathways involved.
        2. Variants/Alleles: Common isoforms (e.g., SNPs like rsID),
        alleles and their prevalence in populations.
        3. Relation to Aging/Longevity:
           - Mechanisms: How it influences aging (e.g., via oxidative stress,
           inflammation, DNA repair, cellular senescence, or epigenetic changes).
           - Positive Effects: Evidence of promotion of longevity (e.g.,
           in centenarians, model organisms like C. elegans or mice).
           - Negative Effects: Links to accelerated aging, age-related diseases
           (e.g., Alzheimer's, cardiovascular disease, cancer), or reduced lifespan.
           - Key Studies: Summarize findings from human GWAS, cohort studies
           (e.g., Framingham Heart Study), or animal models. Include effect sizes
           (e.g., odds ratios, hazard ratios) if mentioned.
           - Biomarkers/Expression: Changes in expression levels with age,
           or as a biomarker for biological age.
        4. Interactions: With other genes (e.g., FOXO3, SIRT1),
        environment (diet, exercise), or interventions
        (e.g.,rapamycin, metformin effects on this gene).
        5. Gaps/Uncertainty: If data is limited or conflicting, note it.
        Suggest related genes for further query.

        Base everything on the retrieved document context—
        do not hallucinate external knowledge.
        If no info on aging,state "No direct relation to aging found in context."
        Be concise, use bullet points for clarity, and cite context snippets
        (e.g., [Source: Document X, Page Y]).
        """

    def _preprocess_xml(self, xml_content: str) -> str:
        try:
            soup = BeautifulSoup(xml_content, "xml")
            text = soup.get_text(separator=" ", strip=True)
            text = re.sub(r"\s+", " ", text)
            text = re.sub(r"\[\d+\]", "", text)
            text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")

        except Exception as error:
            logger.error(f"Error in preprocessing files!{error}")
        return text.strip()

    def text_rag(self, path_to_data: str) -> str:
        """Process documents and create RAG index with mp"""
        load_dotenv()
        try:
            logger.info(f"Starting text_rag for {self.gene_name}")
            if not os.getenv("NEBIUS_API_KEY"):
                raise ValueError("NEBIUS_API_KEY not found in environment")

            if not os.path.exists(path_to_data):
                raise FileNotFoundError(f"Data directory not found: {path_to_data}")

            xml_files = [f for f in os.listdir(path_to_data) if f.endswith(".xml")]

            genage_file = f"{PATH_TO_GENAGE_PARSED_GENES}/{self.gene_name}.xml"
            if os.path.exists(genage_file):
                shutil.copy(genage_file, path_to_data)
                logger.info(f"Downloaded additional info for {self.gene_name}")

            documents = []
            if xml_files:
                documents = self._load_xml_documents_parallel(path_to_data)
            logger.info(f"Loaded {len(documents)} document chunks")

            Settings.embed_model = NebiusEmbedding(
                model_name=self.EMBEDDING_MODEL,
                embed_batch_size=50,  # как вариант ещё тут увеличить
                api_key=os.getenv("NEBIUS_API_KEY"),
            )

            db_dir = os.path.dirname(self.DB_URI)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

            index = self._create_index_parallel(documents, self.DB_URI)
            self._cached_index = index

            logger.info(f"Created and saved index to: {self.DB_URI}")
            logger.info(f"Completed text_rag for {self.gene_name}")
            return self.DB_URI

        except Exception as error:
            logger.error(f"Error in text_rag: {error}")
            raise

    def _load_xml_documents_parallel(self, path_to_data) -> list:
        """Parallel document loading"""
        xml_files = [f for f in os.listdir(path_to_data) if f.endswith(".xml")]

        if not xml_files:
            return []

        logger.info(f"Processing {len(xml_files)} XML files with {4} workers")

        with ProcessPoolExecutor(max_workers=4) as executor:
            future_to_file = {
                executor.submit(
                    self._process_single_xml, path_to_data, filename
                ): filename
                for filename in xml_files
            }

            documents = []
            for future in concurrent.futures.as_completed(future_to_file):
                filename = future_to_file[future]
                try:
                    result = future.result()
                    if result is not None:
                        documents.append(result)
                except Exception as error:
                    logger.error(f"File {filename} generated an exception: {error}")

        return documents

    def _process_single_xml(self, path_to_data, filename):
        """Process single XML file"""
        filepath = os.path.join(path_to_data, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                xml_content = f.read()

            clean_text = self._preprocess_xml(xml_content)

            if clean_text:
                # print(Document(text=clean_text, doc_id=filename))
                return Document(text=clean_text, doc_id=filename)

        except Exception as error:
            logger.error(f"Error processing {filename}: {error}")
            return None

    def _create_index_parallel(self, documents, db_uri):
        """Create vector index with parallel processing"""

        milvus_client = MilvusClient(db_uri)
        logger.info(f"Connected to Milvus instance: {db_uri}")

        if milvus_client.has_collection(collection_name=self.COLLECTION_NAME):
            milvus_client.drop_collection(collection_name=self.COLLECTION_NAME)
            logger.info(f"Cleared existing collection: {self.COLLECTION_NAME}")

        vector_store = MilvusVectorStore(
            uri=db_uri,
            dim=self.EMBEDDING_LENGTH,
            collection_name=self.COLLECTION_NAME,
            overwrite=True,
        )

        storage_context = StorageContext.from_defaults(vector_store=vector_store)
        logger.info("Connected Llama-index to Milvus instance")

        logger.info("Creating index from documents with parallel processing...")

        text_splitter = SentenceSplitter(
            chunk_size=1024,
            chunk_overlap=256,
        )

        index = VectorStoreIndex.from_documents(
            documents,
            storage_context=storage_context,
            transformations=[text_splitter],
        )

        return index

    def llm_response(self, test_context: bool = False) -> str:
        """Generate LLM response for gene analysis. VPN is required."""
        load_dotenv()
        try:
            if not os.getenv("NEBIUS_API_KEY"):
                raise ValueError("NEBIUS_API_KEY not found in environment")

            if self._cached_index is not None:
                logger.info("Using cached index")
                index = self._cached_index
            else:
                vector_store = MilvusVectorStore(
                    uri=self.DB_URI,
                    dim=self.EMBEDDING_LENGTH,
                    collection_name=self.COLLECTION_NAME,
                    overwrite=False,  # don't overwrite when loading
                )

                index = VectorStoreIndex.from_vector_store(vector_store=vector_store)
                logger.info(f"Loaded index from vector db: {self.DB_URI}")

            Settings.llm = NebiusLLM(
                model="openai/gpt-oss-120b", api_key=os.getenv("NEBIUS_API_KEY")
            )

            # context testing if needed
            if test_context:
                self._check_context_usage(index)

            query_engine = index.as_query_engine()
            prompt = self._create_gene_prompt()

            logger.info(f"🔍 Querying about gene: {self.gene_name}")
            response = query_engine.query(prompt)
            logger.info("\n" + "=" * 60)
            logger.info("GENE ANALYSIS RESULT:")
            logger.info("=" * 60)
            logger.info(str(response))
            logger.info("=" * 60)

            return str(response)

        except Exception as error:
            logger.info(f"Error in llm_response: {error}")
            raise


def run_llm(gene_name):
    aging_llm = AgingLLM(gene_name)
    db_path = aging_llm.text_rag(
        path_to_data=f"{PATH_TO_PARSED_TEXTS}/{gene_name}/triage/fulltext_xml/"
    )
    # db_path = aging_llm.text_rag("./data/test_data")
    if db_path:
        result = aging_llm.llm_response(test_context=False)
    return result


if __name__ == "__main__":
    # debug xml content
    # with open("output.txt", 'w', encoding='utf-8') as f:
    #    documents = aging_llm._load_xml_documents()
    #    for i, doc in enumerate(documents):
    #        f.write(f"=== Document {i+1}: {doc.doc_id} ===\n")
    #        #80 characters per line
    #        wrapped_text = textwrap.fill(doc.text, width=80, break_long_words=False)
    #        f.write(wrapped_text)
    #        f.write("\n\n" + "="*80 + "\n\n")

    # proxychains curl https://ifconfig.me - check vpn

    gene_name = "NRF2"
    results = run_llm(gene_name)
    for gene_name, result in results:
        print(f"\n{'=' * 60}")
        print(f"FINAL RESULT FOR {gene_name}:")
        print(f"{'=' * 60}")
        print(result)
