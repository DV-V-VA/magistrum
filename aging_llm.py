import os, re
from dotenv import load_dotenv
import pprint, textwrap  #debug
from llama_index.core import Settings, VectorStoreIndex, StorageContext
from llama_index.embeddings.nebius import NebiusEmbedding
from pymilvus import MilvusClient
from llama_index.vector_stores.milvus import MilvusVectorStore
from llama_index.llms.nebius import NebiusLLM
from llama_index.core.schema import Document
from bs4 import BeautifulSoup


class AgingLLM:
    def __init__(self):
        self.EMBEDDING_MODEL = 'Qwen/Qwen3-Embedding-8B'
        self.EMBEDDING_LENGTH = 4096
        self.DB_URI = './rag.db'
        self.COLLECTION_NAME = 'rag'

    #этот метод можно закоментить в итоговом варианте
    def _check_context_usage(self, index: VectorStoreIndex) -> None:
        """Test if model uses context by asking unrelated question"""
        query_engine = index.as_query_engine()
        test_response = query_engine.query("When was the moon landing?")
        print("Context test:", test_response)

    # тут не забудь стереть хардкод АПОЕ
    def _create_gene_prompt(self, gene_name:str) -> str:
        """Create structured prompt for gene analysis"""
        return f"""
        You are a genomics expert analyzing documents for gene-aging relationships. Extract and summarize all information about the gene {gene_name} from the provided context. Focus primarily on its relation to aging, longevity, or age-related processes.

        Structure your response exactly as follows:
        1. **Gene Overview**: Full name, function, location (chromosome), protein product, and key pathways involved.
        2. **Variants/Alleles**: Common isoforms (e.g., SNPs like rsID) and their prevalence in populations.
        3. **Relation to Aging/Longevity**:
           - Mechanisms: How it influences aging (e.g., via oxidative stress, inflammation, DNA repair, cellular senescence, or epigenetic changes).
           - Positive Effects: Evidence of promotion of longevity (e.g., in centenarians, model organisms like C. elegans or mice).
           - Negative Effects: Links to accelerated aging, age-related diseases (e.g., Alzheimer's, cardiovascular disease, cancer), or reduced lifespan.
           - Key Studies: Summarize findings from human GWAS, cohort studies (e.g., Framingham Heart Study), or animal models. Include effect sizes (e.g., odds ratios, hazard ratios) if mentioned.
           - Biomarkers/Expression: Changes in expression levels with age, or as a biomarker for biological age.
        4. **Interactions**: With other genes (e.g., FOXO3, SIRT1), environment (diet, exercise), or interventions (e.g., rapamycin, metformin effects on this gene).
        5. **Gaps/Uncertainty**: If data is limited or conflicting, note it. Suggest related genes for further query.

        Base everything on the retrieved document context—do not hallucinate external knowledge. If no info on aging, state "No direct relation to aging found in context." Be concise, use bullet points for clarity, and cite context snippets (e.g., [Source: Document X, Page Y]).
        """
    

    def _preprocess_xml(self, xml_content:str) -> str:
        try:
            soup = BeautifulSoup(xml_content, 'xml')
            text = soup.get_text(separator=' ', strip=True)
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'\[\d+\]', '', text)
            text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')

        except Exception as error:
            print(f"Error in preprocessing files!{error}")
        return text.strip()

    def _load_xml_documents(self, path_to_data:str='./data/test_data')-> str:
        documents = []
        for filename in os.listdir(path_to_data):
            if filename.endswith('.xml'):
                filepath = os.path.join(path_to_data, filename)
                try:
                    with open(filepath, 'r') as f:
                        xml_content = f.read()

                    clean_text = self._preprocess_xml(xml_content)
                    if clean_text:
                        documents.append(Document(text=clean_text, doc_id=filename))

                except Exception as error:
                    print(f"Error in loading xml files!{error}")

        return documents
        
    def text_rag(self, path_to_data: str = './data/test_data') -> str:
        """Process documents and create RAG index. VPN is required"""
        load_dotenv()
        try:

            if not os.getenv('NEBIUS_API_KEY'):
                raise ValueError("❌ NEBIUS_API_KEY not found in environment")


            if not os.path.exists(path_to_data):
                raise FileNotFoundError(f"Data directory not found: {path_to_data}")
                
            #documents = SimpleDirectoryReader(input_dir=path_to_data).load_data()

            xml_files = [f for f in os.listdir(path_to_data) if f.endswith('.xml')]
            if xml_files:
                documents = self._load_xml_documents(path_to_data)
            print(f"✅ Loaded {len(documents)} document chunks")

            #if documents:
                #print("Sample document ID:", documents[0].doc_id)
                #pprint.pprint(documents[0].dict(), indent=2, depth=2)

            Settings.embed_model = NebiusEmbedding(
                model_name=self.EMBEDDING_MODEL,
                embed_batch_size=50,
                api_key=os.getenv("NEBIUS_API_KEY")
            )


            milvus_client = MilvusClient(self.DB_URI)
            print("✅ Connected to Milvus instance:", self.DB_URI)

            if milvus_client.has_collection(collection_name=self.COLLECTION_NAME):
                milvus_client.drop_collection(collection_name=self.COLLECTION_NAME)
                print('✅ Cleared existing collection:', self.COLLECTION_NAME)

            vector_store = MilvusVectorStore(
                uri=self.DB_URI,
                dim=self.EMBEDDING_LENGTH,
                collection_name=self.COLLECTION_NAME,
                overwrite=True
            )
            
            storage_context = StorageContext.from_defaults(vector_store=vector_store)
            print("✅ Connected Llama-index to Milvus instance")

            print("⚙️ Creating index from documents...")
            index = VectorStoreIndex.from_documents(
                documents, 
                storage_context=storage_context,
                show_progress=True
            )
            
            print("✅ Created index:", index)
            print("✅ Saved index to db:", self.DB_URI)
            return self.DB_URI

        except Exception as error:
            print(f"❌ Error in text_rag: {error}")
            raise

    def llm_response(self, gene_name: str, test_context: bool = False) -> str:
        """Generate LLM response for gene analysis. VPN is required."""
        load_dotenv()
        try:
            if not os.getenv('NEBIUS_API_KEY'):
                raise ValueError("❌ NEBIUS_API_KEY not found in environment")

            vector_store = MilvusVectorStore(
                uri=self.DB_URI,
                dim=self.EMBEDDING_LENGTH,
                collection_name=self.COLLECTION_NAME,
                overwrite=False  #don't overwrite when loading
            )
            
            index = VectorStoreIndex.from_vector_store(vector_store=vector_store)
            print("✅ Loaded index from vector db:", self.DB_URI)


            Settings.llm = NebiusLLM(
                model='openai/gpt-oss-120b',
                api_key=os.getenv("NEBIUS_API_KEY")
            )

            #context testing if needed
            if test_context:
                self._check_context_usage(index)


            query_engine = index.as_query_engine()
            prompt = self._create_gene_prompt(gene_name)
            
            print(f"🔍 Querying about gene: {gene_name}")
            response = query_engine.query(prompt)
            
            print("\n" + "="*60)
            print("GENE ANALYSIS RESULT:")
            print("="*60)
            print(response)
            print("="*60)
            
            return str(response)

        except Exception as error:
            print(f"❌ Error in llm_response: {error}")
            raise


if __name__ == "__main__":
    aging_llm = AgingLLM()


    # debug xml content
    #with open("output.txt", 'w', encoding='utf-8') as f:
    #    documents = aging_llm._load_xml_documents()
    #    for i, doc in enumerate(documents):
    #        f.write(f"=== Document {i+1}: {doc.doc_id} ===\n")
#
    #        # Wrap text to 80 characters per line for readability
    #        wrapped_text = textwrap.fill(doc.text, width=80, break_long_words=False)
    #        f.write(wrapped_text)
    #        f.write("\n\n" + "="*80 + "\n\n")

    #proxychains curl https://ifconfig.me - check vpn


    # Create index from documents
    db_path = aging_llm.text_rag('./data/test_data')
    
    # Query with context testing
    result = aging_llm.llm_response(test_context=False, gene_name='APOE')