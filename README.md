# MagistrUM: a service to retrieve gene-related info from public databases

This is an intergrated service for possible (who knows) assistance in protein aging related research. MagistrUM contains two subparts: a data-retrieval pipeline for and web application for data displaying.

### How it works:
1) Pipeline:
    - **public protein databases parsing**
        - accepts gene name query and tries to match provided gene name with synonym list extracting unique gene symbol identifier (example: `alpha-2-macroglobulin` will be resolved to `A2M`)
        - gene symbol obtained from previous step is the queried accross public databases via API to obtain:
            - possible orthologs from species of interest (can be configured in `config.py`)
            - additional gene synonyms
            - protein sequences of the target gene along with sequences of orthologs
        - protein sequences are then aligned in multiple alignment
        - a query for public articles search is generated for the next step
    - **article parsing**
        - public article databases are queried by gene name and its synonyms to get broader overview of published data
        - articles are then scored to keep only the most relevant for topic of interes
        - then articles are downloaded and prepared for the next step
    - **LLM**
        - each article is sliced into overlapping chunks and converted into RAG-database
        - resulting RAG-database is queried by a prompt to obtain information of interest
        - data is stored for future access from web app

2) Web app 
    - available at https://gendvva.ru/
    - displays information obtained by pipeline
    - can trigger pipeline to start if is queried by the gene name that is currently not obtained by the pipeline


Pipeline can be imported and run solely

### Requirements
- working conda installation
- `API_KEY` to access LLM
- Downloaded HGNC dataset in `.json` format (can be obtained from [here](https://storage.googleapis.com/public-download-files/hgnc/json/json/non_alt_loci_set.json))
- NCBI_API_KEY - may come in handy when processing huge amounts of data
- *(optional)* clustalO aligner installed
- *(optional)* ncbi datasets installed


TODO

### Installation
- Unfortunately, we suggest using a combination of conda and pip environment. This has a weird ratio based on the fact that not all packages can be installed either by conda or by pip. First, install conda environment via `conda env create -f environment.yml` - this will create conda env named `magistrum_pipeline`
- Then this env can be activated via `conda activate magistrum_pipeline`
- After that we suggest using `pip install -rrequirements.txt` to install all the packages not available on conda
- After that we suggest creating a `.env` file and placing the following data in there:
    - `OPENAI_API_BASE` - url to your LLM API
    - `NEBIUS_API_KEY` - key to your API provider (we here used Nebius as you have probably already guessed)
    - `OPENAI_API_KEY` - the same key as before as we were not sure where one or another is used. Sorry.
    - `NCBI_API_KEY` - key to NCBI datasets
- when you have managed all the keys, it might be a good idea to go and check `config.py` file to see if all config parameters suit you well

### Pipeline quickstart
If you made up to here - congratulations. Now you can:
    - import funtion `run_pipeline` into any python-executable script and run it with your desired gene name

## License
This project is licensed under the [MIT License](LICENSE).  
© 2025 DVVA Team. All rights reserved.