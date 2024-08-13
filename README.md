# IFX_ODIN
Ontology and Data Integration Network

We are currently enhancing our data integration capabilities (NCATS and other open-source biomedical datasets) by transitioning from traditional relational databases to an interoperable graph database using the Neo4j platform, structured around the BioLink Data Model, https://github.com/ncats/IFX_ODIN. This integration seamlessly incorporates comprehensive gene/protein information, disease associations, and drug data, enabling more advanced AI/ML applications. A public Neo4j instance is coming soon.For more information about our initiatives or to collaborate, please contact Jessica Maine or Keith Kelleher

make virtual environment
* `python -m venv .venv`
* `source .venv/bin/activate`
* `export PYTHONPATH=/Users/kelleherkj/IdeaProjects/NCATS_ODIN:$PYTHONPATH`
  * update with your correct path
* install dependencies in your python environment
  * `pip install -r requirements.txt`

start local neo4j
* navigate to neo4j-container
* `docker compose up -d`
  * if you already have neo4j running locally, you can update the ports in the docker-compose.yml file so they don't clash 
    * "7474:7474" # update the first numbers to something else i.e. "27474:7474"
    * "7687:7687" # i.e. "27687:7687"
* point your browser to "http://localhost:7474" (or the port you mapped i.e. 27474)
* point your bolt address to the other one
* successfully see your graph

save your neo4j credentials into "src/use_cases/secrets/local_neo4j.py"
it should look something like this:
`from src.shared.db_credentials import DBCredentials
local_neo4j_credentials = DBCredentials(url="bolt://localhost:7687", user="neo4j", password="password")
alt_neo4j_credentials = DBCredentials(url="bolt://localhost:17687", user="neo4j", password="password")`