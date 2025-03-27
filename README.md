# IFX_ODIN
Ontology and Data Integration Network

We are currently enhancing our data integration capabilities (NCATS and other open-source biomedical datasets) by 
transitioning from traditional relational databases to an interoperable graph database, structured around the BioLink 
Data Model, https://github.com/ncats/IFX_ODIN. This integration seamlessly incorporates comprehensive gene/protein 
information, disease associations, and drug data, enabling more advanced AI/ML applications. A public graph database 
is coming soon.For more information about our initiatives or to collaborate, please contact Jessica Maine or Keith 
Kelleher

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
