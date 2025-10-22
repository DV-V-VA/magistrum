import concurrent.futures
import logging
import os
import re
import shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from llama_index.core import (
    Settings,
    StorageContext,
    VectorStoreIndex,
    load_index_from_storage,
)
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import Document
from llama_index.embeddings.nebius import NebiusEmbedding
from llama_index.llms.nebius import NebiusLLM
from llama_index.vector_stores.milvus import MilvusVectorStore
from pymilvus import MilvusClient
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import (
    PATH_TO_GENAGE_PARSED_GENES,
    PATH_TO_LOGS,
    PATH_TO_RAG,
    RATE_LIMIT_NEBIUS,
)
from logging_config import setup_logging
from utils import download_rate_limiter

# PATH_TO_LOGS = os.path.join(tempfile.gettempdir(), "aging_llm.log")
setup_logging(PATH_TO_LOGS)
logger = logging.getLogger(__name__)


class AgingLLM:
    def __init__(self, gene_name):
        self.gene_name = gene_name
        self.EMBEDDING_MODEL = "Qwen/Qwen3-Embedding-8B"
        self.EMBEDDING_LENGTH = 4096
        # self.DB_URI = f"{PATH_TO_RAG}/{gene_name}/rag.db"
        self.DB_URI = f"{PATH_TO_RAG}/{gene_name}"
        self.COLLECTION_NAME = f"{gene_name}_rag"

    def _check_context_usage(self, index: VectorStoreIndex) -> None:
        """Test if model uses context by asking unrelated question"""
        query_engine = index.as_query_engine()
        test_response = query_engine.query("When was the moon landing?")
        logger.info(f"Context test: {test_response}")

    def _create_gene_prompt(self) -> str:
        """Create structured prompt for gene analysis"""
        return f"""
        You are a genomics expert specializing in extracting and
        analyzing gene-related information
        with a primary focus on relationships to aging, longevity,
        and age-related processes.
        Extract and summarize all information about the gene {self.gene_name}
        solely from the provided context.
        Focus primarily on its relation to aging, longevity, or age-related processes.
        Do not introduce external knowledge, assumptions, or hallucinations—
        rely strictly on the content in the context.

        Structure your response exactly as follows:
        1. Gene Overview:  Provide the full name, primary function(s),
        chromosomal location, protein product (if available),
        and key pathways or biological processes involved.
        If information is not available, just omit it from the section.

        2. Variants/Alleles: List common isoforms (e.g., SNPs with rsID),
        alleles and their prevalence in populations (e.g., allele frequencies, common
        neutral variants).
           - For each variant, isoform, or allele, describe its specific function,
           role, or effects in a separate bullet.
            - If no variants or alleles are available, omit this section or
            include a single bullet stating: "Information on variants or alleles
            is limited; the gene's role is summarized as follows:
            [briefly summarize any general gene function or context if applicable]."
            - Cite all info according to citations requirements further!
            - If there is no specific information about variant or allele,
            conclude that it is neutral.

        3. Relation to Aging/Longevity:
           - Mechanisms: Describe how the gene influences aging processes (e.g.,
           oxidative stress, inflammation, DNA repair, cellular senescence,
           epigenetic regulation).
           - Positive Effects: Summarize evidence linking the gene to extended
           longevity,
           healthy aging, or protective effects (e.g., in centenarians,
           long-lived populations, or model organisms like C. elegans, mice,
           rat, drosofila or yeast).
           - Negative Effects: Summarize links to accelerated aging, reduced lifespan,
           or age-related diseases (e.g., Alzheimer's, cardiovascular disease, cancer,
           diabetes).
           - Key Studies: Provide concise summaries of relevant studies
           from the context,
           such as human GWAS, cohort studies (e.g., Framingham Heart Study,UK Biobank),
           or animal models. Include specific findings like effect sizes
           (e.g., odds ratios, hazard ratios, p-values) if provided.
           - Biomarkers/Expression: Describe changes in gene expression with age,
           its role as a biomarker for biological age, or related metrics
           (e.g., methylation patterns)
           - If a subsection lacks information, state:
           "Current data is limited for this aspect."
        4. Interactions: Detail interactions with other genes (e.g.,FOXO3, SIRT1, IGF1),
        environmental factors (e.g., diet, exercise, stress),
        or interventions (e.g., effects of drugs like rapamycin,
        metformin, or caloric restriction on this gene).
            - Use separate bullets for each type of interaction.
            - If no interactions are available, omit this section
            or include a single bullet stating:
            "Interactions with other genes or factors are not detailed; the gene's
            role is summarized as follows:
            [briefly summarize any general gene function or context if applicable]."
        5. Gaps/Uncertainty:  Highlight limitations, conflicting data, sparse evidence,
        or uncertainties in the context. If data is robust, state:
        "Data in the provided context is consistent with no significant gaps."
        Suggest 1-3 related genes or topics for further investigation
        if implied by the context (e.g., "Consider querying IGF1 for pathway overlaps").

        Key Instructions:
    - Source Fidelity: Base every claim exclusively on the provided context.
    Do not infer, generalize, or add details not explicitly stated.
    - Conciseness: Keep summaries brief and factual—aim for 1-3 sentences per bullet.
    Avoid redundancy across sections.
    - Handling Multiples: List and describe each variant, study, or interaction
    separately without grouping unless the context does so.
    - Neutrality: Report information objectively, without bias or speculation.
    - Edge Cases: If the context is ambiguous, note it in Section 5.
    If the gene is mentioned but not in an aging context,
    extract general information and note the absence
    of aging-related data in Section 3.
    - Tone: Use professional, clear language suitable for a descriptive output,
    avoiding overly technical jargon unless directly supported by the context.
    "not described," or "no data available," focusing on available data or seamlessly
    omitting unavailable sections.
    - Missing Data Handling: For Variants/Alleles and Interactions,
    if no data is available, either omit the section
    or provide a neutral summary of the gene's role (if applicable)
    to maintain a positive, informative tone.
    If no information exists for the gene, use the
    introductory statement to redirect focus constructively.

    Metadata (for reference only—do not include in response):
    - Current date and time: 03:01 PM CEST, Tuesday, October 21, 2025.

    **SPECIAL INSTRUCTIONS FOR VARIANTS:**
    - Describe EACH variant/isoform separately with its unique characteristics
    - Compare functional differences between isoforms when multiple are mentioned
    - Highlight any isoform-specific aging associations

    **SOURCE CITATION REQUIREMENTS:**
    - For each factual claim, cite the SPECIFIC article title from the context
    - Use format: [Source: Article Title]. Don't use table titles!
    - When multiple sources support a claim, cite all relevant article titles

    If the context contains NO information whatsoever about {self.gene_name},
    respond with:
    "No information available about {self.gene_name} in the provided documents."
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
            print("path_to_data:", path_to_data)
            xml_files = [f for f in os.listdir(path_to_data) if f.endswith(".xml")]
            print("xml_files:", xml_files)

            genage_file = f"{PATH_TO_GENAGE_PARSED_GENES}/{self.gene_name}.xml"
            print(genage_file)
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

            # Create directory for index storage
            db_dir = self.DB_URI  # This should be a directory path, not file
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

            # index = self._create_index_parallel(documents, self.DB_URI)
            self._create_index_file_storage(documents, db_dir)

            logger.info(f"Created and saved index to: {self.DB_URI}")
            logger.info(f"Completed text_rag for {self.gene_name}")
            return self.DB_URI

        except Exception as error:
            logger.error(f"Error in text_rag: {error}")
            raise

    def _create_index_file_storage(self, documents, persist_dir):
        """Create vector index with file storage"""

        text_splitter = SentenceSplitter(
            chunk_size=1024,
            chunk_overlap=256,
        )

        # Create index with file storage
        index = VectorStoreIndex.from_documents(
            documents,
            transformations=[text_splitter],
        )

        index.storage_context.persist(persist_dir=persist_dir)
        logger.info(f"Persisted index to: {persist_dir}")

        return index

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

        print(documents)

        return documents

    def _process_single_xml(self, path_to_data, filename):
        """Process single XML file"""
        filepath = os.path.join(path_to_data, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                xml_content = f.read()

            clean_text = self._preprocess_xml(xml_content)

            if clean_text:
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

    def load_index_parallel_sync(self):
        """Simpler parallel loading without async complexity"""
        with ThreadPoolExecutor(max_workers=4) as executor:
            storage_context_future = executor.submit(
                StorageContext.from_defaults, persist_dir=self.DB_URI
            )
            index_future = executor.submit(
                load_index_from_storage, storage_context_future.result()
            )
            return index_future.result()

    @download_rate_limiter("nebius", RATE_LIMIT_NEBIUS)
    def llm_response(self, gene_name, rag_path, test_context: bool = False) -> str:
        """Generate LLM response for gene analysis. VPN is required."""
        load_dotenv()
        self.gene_name = gene_name
        self.DB_URI = rag_path  # f"{PATH_TO_RAG}/{gene_name}"
        try:
            if not os.getenv("NEBIUS_API_KEY"):
                raise ValueError("NEBIUS_API_KEY not found in environment")

            Settings.embed_model = NebiusEmbedding(
                model_name=self.EMBEDDING_MODEL,
                embed_batch_size=50,
                api_key=os.getenv("NEBIUS_API_KEY"),
            )

            if not os.path.exists(self.DB_URI):
                raise FileNotFoundError(
                    f"Index file not found: {self.DB_URI}. Run text_rag first."
                )
            # Load index from file
            logger.info(
                f"Loading index from file with parallel execution: {self.DB_URI}"
            )
            index = self.load_index_parallel_sync()
            index._embed_model = Settings.embed_model  # type: ignore

            # storage_context = StorageContext.from_defaults(persist_dir=self.DB_URI)
            # index = load_index_from_storage(storage_context)
            # index._embed_model = Settings.embed_model
            logger.info(f"Loaded index from file: {self.DB_URI}")
            Settings.llm = NebiusLLM(
                model="meta-llama/Llama-3.3-70B-Instruct-fast",
                api_key=os.getenv("NEBIUS_API_KEY"),
            )

            # context testing if needed
            if test_context:
                self._check_context_usage(index)  # type: ignore

            query_engine = index.as_query_engine()
            prompt = self._create_gene_prompt()

            logger.info(f"🔍 Querying about gene: {self.gene_name}")
            # response = query_engine.query(prompt)
            response = self._llm_query_with_retry(query_engine, prompt)

            logger.info("\n" + "=" * 60)
            logger.info("GENE ANALYSIS RESULT:")
            logger.info("=" * 60)
            logger.info(str(response))
            logger.info("=" * 60)

            return str(response)

        except Exception as error:
            logger.info(f"Error in llm_response: {error}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def _llm_query_with_retry(self, query_engine, prompt):
        """LLM query with retry logic"""
        return query_engine.query(prompt)


if __name__ == "__main__":
    gene_name = "APOE"
    aging_llm = AgingLLM(gene_name)
    # aging_llm.text_rag(f"{PATH_TO_PARSED_TEXTS}/{gene_name}/triage/fulltext_xml")
    print(aging_llm.llm_response(gene_name, f"{PATH_TO_RAG}/{gene_name}"))
