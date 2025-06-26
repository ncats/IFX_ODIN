# IFX_ODIN
Ontology and Data Integration Network (ODIN)

ODIN is a modular, standards-driven platform developed at NCATS to support scalable data integration, curation, and harmonization across internal and external biomedical datasets. Built around FAIR principles (Findable, Accessible, Interoperable, and Reproducible), ODIN enables semi-automated mapping and normalization of data using established biomedical ontologies. It plays a central role in supporting translational science platforms such as Pharos, RaMP-DB, GSRS, and CURE ID by ensuring data consistency, provenance tracking, and interoperability across systems.

Rather than a single database or user-facing tool, ODIN functions as the infrastructure layer for harmonizing diverse knowledge sources—powering NCATS’s efforts to build trustworthy, AI/ML-ready datasets for downstream discovery and collaboration.

For more information or to explore collaboration opportunities, please contact Jessica Maine or Keith Kelleher.

make virtual environment
* `python -m venv .venv`
* `source .venv/bin/activate`
* `export PYTHONPATH=/Users/kelleherkj/IdeaProjects/NCATS_ODIN:$PYTHONPATH`
  * update with your correct path
* install dependencies in your python environment
  * `pip install -r requirements.txt`

start local memgraph
* navigate to the container
* `docker compose up -d`
  * if you already have memgraph running locally, you can update the ports in the docker-compose.yml file so they don't clash 
    * "7474:7474" # update the first numbers to something else i.e. "27474:7474"
    * "7687:7687" # i.e. "27687:7687"
* point your browser to "http://localhost:7474" (or the port you mapped i.e. 27474)
* point your bolt address to the other one
* successfully see your graph

save your credentials into "src/use_cases/secrets/local_credentials.yaml"
```
url: bolt://localhost:7687
user: neo4j
password: password
```
