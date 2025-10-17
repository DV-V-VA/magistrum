import os
from dotenv import load_dotenv
load_dotenv()

if os.getenv('NEBIUS_API_KEY'):
    print ("✅ Found NEBIUS_API_KEY in environment, using it")
else:
    raise ValueError("❌ NEBIUS_API_KEY not found in environment. Please set it in .env file before running this script.")


load_dotenv()
if os.getenv('OPENAI_API_KEY'):
    print ("✅ Found OPENAI_API_KEY in environment, using it")
else:
    raise ValueError("❌ OPENAI_API_KEY not found in environment. Please set it in .env file before running this script.")


from llama_index.core import SimpleDirectoryReader
import pprint


documents = SimpleDirectoryReader(
    input_dir = './test_data',
).load_data()

print (f"Loaded {len(documents)} chunks")

# print("Document [0].doc_id:", documents[0].doc_id)
# pprint.pprint (documents[0], indent=4)


from llama_index.core import Settings

# Option 1: Running embedding models on Nebius cloud
from llama_index.embeddings.nebius import NebiusEmbedding
EMBEDDING_MODEL = 'Qwen/Qwen3-Embedding-8B'  # 8B params
EMBEDDING_LENGTH = 4096  # Length of the embedding vector
Settings.embed_model = NebiusEmbedding(
                        model_name=EMBEDDING_MODEL,
                        embed_batch_size=50,  # Batch size for embedding (default is 10)
                        api_key=os.getenv("NEBIUS_API_KEY") # if not specfified here, it will get taken from env variable
                       )

## Option 2: Running embedding models locally
# from llama_index.embeddings.huggingface import HuggingFaceEmbedding
# os.environ['HF_ENDPOINT'] = 'https://hf-mirror.com'
# Settings.embed_model = HuggingFaceEmbedding(
#     # model_name = 'sentence-transformers/all-MiniLM-L6-v2' # 23 M params
#     model_name = 'BAAI/bge-small-en-v1.5'  # 33M params
#     # model_name = 'Qwen/Qwen3-Embedding-0.6B'  # 600M params
#     # model_name = 'BAAI/bge-en-icl'  # 7B params
#     #model_name = 'intfloat/multilingual-e5-large-instruct'  # 560M params
# )

from pymilvus import MilvusClient

DB_URI = './rag.db'  # For embedded instance
COLLECTION_NAME = 'rag'

milvus_client = MilvusClient(DB_URI)
print ("✅ Connected to Milvus instance: ", DB_URI)

# if we already have a collection, clear it first
if milvus_client.has_collection(collection_name = COLLECTION_NAME):
    milvus_client.drop_collection(collection_name = COLLECTION_NAME)
    print ('✅ Cleared collection :', COLLECTION_NAME)


# connect to vector db
from llama_index.core import VectorStoreIndex, StorageContext
from llama_index.vector_stores.milvus import MilvusVectorStore

vector_store = MilvusVectorStore(
    uri = DB_URI ,
    dim = EMBEDDING_LENGTH ,
    collection_name = COLLECTION_NAME,
    overwrite=True
)
storage_context = StorageContext.from_defaults(vector_store=vector_store)

print ("✅ Connected Llama-index to Milvus instance: ", DB_URI )

from llama_index.core import VectorStoreIndex

print ("⚙️ Creating index from documents...")
index = VectorStoreIndex.from_documents(
    documents, storage_context=storage_context
)
print ("✅ Created index:", index )
print ("✅ Saved index to db ", DB_URI )


from llama_index.core import VectorStoreIndex

index = VectorStoreIndex.from_vector_store(
    vector_store=vector_store, storage_context=storage_context)

print ("✅ Loaded index from vector db:", DB_URI )


from llama_index.llms.nebius import NebiusLLM
from llama_index.core import Settings

Settings.llm = NebiusLLM(
                model='openai/gpt-oss-120b',
                #model='Qwen/Qwen3-30B-A3B',
                # model='deepseek-ai/DeepSeek-R1-0528',
                api_key=os.getenv("NEBIUS_API_KEY") # if not specfified, it will get taken from env variable
    )


query_engine = index.as_query_engine()
gene_name = "APOE"
prompt = f"""
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

res = query_engine.query(prompt)
print(res)

#response_object = query_engine.query("What was Uber's revenue for 2020?")

# Print the response string (might be empty)
#print("Final Answer:", str(response_object))
#
## 🔬 CRITICAL: Check what information was actually retrieved
#print("\n--- Retrieved Source Nodes ---")
#for i, node in enumerate(response_object.source_nodes):
#    print(f"Node {i+1}:")
#    print(f"Score (Similarity): {node.score}")
#    print(f"Text Content: {node.node.text[:500]}...") # Print first 500 chars
#    print("------")

# %% [markdown]
# ## Making sure the model uses context
# 
# Let's ask a generic factual question "When was the moon landing".
# 
# Now the model should know this generic factual answer.
# 
# But since we are querying documents, we want to the model to find answers from within the documents.
# 
# It should come back with something like "provided context does not have information about moon landing"

query_engine = index.as_query_engine()
res = query_engine.query("When was the moon landing?")
print(res)




