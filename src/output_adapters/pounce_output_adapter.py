from src.models.pounce.data import SampleFactorRelationship, Biospecimen, Sample
from src.output_adapters.neo4j_output_adapter import Neo4jOutputAdapter


class PounceOutputAdapter(Neo4jOutputAdapter):

    def preprocess_objects(self, objects):
        # for index, obj in enumerate(objects):
        #     if (isinstance(obj, SampleFactorRelationship)):
        #         start_node = obj.start_node
        #         end_node = obj.end_node
        #         if (isinstance(end_node, Biospecimen)):
        #             new_obj = Sample(id=start_node.id)
        #             new_obj.link_biospecimen = end_node.id
        #             new_obj.entity_resolution = obj.entity_resolution
        #             new_obj.provenance = obj.provenance
        #             objects[index] = new_obj
        return objects