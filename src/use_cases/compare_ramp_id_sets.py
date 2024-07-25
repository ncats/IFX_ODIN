from typing import List

from src.core.etl import ETL
from src.input_adapters.sqlite_ramp._compare_id_sets import MetaboliteSetRelationshipAdapter
from src.input_adapters.sqlite_ramp.analyte_pathway_relationship_adapter import MetabolitePathwayRelationshipAdapter
from src.input_adapters.sqlite_ramp.analyte_synonym_adapter import MetaboliteSynonymAdapter
from src.input_adapters.sqlite_ramp.metabolite_adapter import MetaboliteAdapter
from src.input_adapters.sqlite_ramp.metabolite_chem_props_adapter import MetaboliteChemPropsAdapter
from src.input_adapters.sqlite_ramp.metabolite_class_adapter import MetaboliteClassAdapter
from src.input_adapters.sqlite_ramp.metabolite_class_relationship_adapter import MetaboliteClassRelationshipAdapter
from src.input_adapters.sqlite_ramp.pathway_adapter import PathwayAdapter
from src.interfaces.input_adapter import InputAdapter

from src.interfaces.labeler import AuxLabeler, ComparingLabeler, Labeler
from src.output_adapters.neo4j_output_adapter import Neo4jOutputAdapter
from src.use_cases.secrets.local_neo4j import alt_neo4j_credentials

class build_db_for_comparing_ramp_ids:
    left_db: str
    left_label: str
    right_db: str
    right_label: str
    third_db: str
    third_label: str
    output_adapter: Neo4jOutputAdapter
    etl: ETL
    def __init__(self, left_db: str, left_label: str,
                 right_db: str, right_label: str,
                 third_db: str = None, third_label: str = None):
        self.left_db = left_db
        self.left_label = left_label
        self.right_db = right_db
        self.right_label = right_label
        self.third_db = third_db
        self.third_label = third_label

        self.output_adapter = Neo4jOutputAdapter(credentials=alt_neo4j_credentials)
        self.etl = ETL(input_adapters=[], output_adapters=[self.output_adapter])


    def truncate_old_db(self):
        self.etl.create_or_truncate_datastores()

    def do_etl(self):
        # Common Input Adapters
        pathway_adapter = PathwayAdapter(sqlite_file=self.left_db)
        metabolite_class_adapter = MetaboliteClassAdapter(sqlite_file=self.left_db)

        self.do_etl_step([
            pathway_adapter,
            metabolite_class_adapter
        ])

        # Input Adapters
        left_met_adapter = MetaboliteAdapter(sqlite_file=self.left_db)
        left_metabolite_synonym_list_adapter = MetaboliteSynonymAdapter(sqlite_file=self.left_db)
        left_met_chem_props_adapter = MetaboliteChemPropsAdapter(sqlite_file=self.left_db)
        left_metabolite_pathway_relationship_adapter = MetabolitePathwayRelationshipAdapter(sqlite_file=self.left_db)
        left_metabolite_class_relationship_adapter = MetaboliteClassRelationshipAdapter(sqlite_file=self.left_db)

        left_labeler = AuxLabeler(self.left_label)

        self.do_etl_step([
            left_met_adapter,
            left_metabolite_synonym_list_adapter,
            left_met_chem_props_adapter,
            left_metabolite_pathway_relationship_adapter,
            left_metabolite_class_relationship_adapter
        ], left_labeler)

        right_met_adapter = MetaboliteAdapter(sqlite_file=self.right_db)
        right_metabolite_synonym_list_adapter = MetaboliteSynonymAdapter(sqlite_file=self.right_db)
        right_met_chem_props_adapter = MetaboliteChemPropsAdapter(sqlite_file=self.right_db)
        right_metabolite_pathway_relationship_adapter = MetabolitePathwayRelationshipAdapter(sqlite_file=self.right_db)
        right_metabolite_class_relationship_adapter = MetaboliteClassRelationshipAdapter(sqlite_file=self.right_db)

        right_labeler = AuxLabeler(self.right_label)

        self.do_etl_step([
            right_met_adapter,
            right_metabolite_synonym_list_adapter,
            right_met_chem_props_adapter,
            right_metabolite_pathway_relationship_adapter,
            right_metabolite_class_relationship_adapter
        ], right_labeler)

        metabolite_set_relationship_adapter = (MetaboliteSetRelationshipAdapter()
                                               .set_left(self.left_db).set_right(self.right_db))
        self.do_etl_step([
            metabolite_set_relationship_adapter
        ], ComparingLabeler().set_left_labeler(left_labeler).set_right_labeler(right_labeler))



        if self.third_db:
            third_met_adapter = MetaboliteAdapter(sqlite_file=self.third_db)
            third_metabolite_synonym_list_adapter = MetaboliteSynonymAdapter(sqlite_file=self.third_db)
            third_met_chem_props_adapter = MetaboliteChemPropsAdapter(sqlite_file=self.third_db)
            third_metabolite_pathway_relationship_adapter = MetabolitePathwayRelationshipAdapter(sqlite_file=self.third_db)
            third_metabolite_class_relationship_adapter = MetaboliteClassRelationshipAdapter(sqlite_file=self.third_db)

            third_labeler = AuxLabeler(self.third_label)

            self.do_etl_step([
                third_met_adapter,
                third_metabolite_synonym_list_adapter,
                third_met_chem_props_adapter,
                third_metabolite_pathway_relationship_adapter,
                third_metabolite_class_relationship_adapter
            ], third_labeler)

            metabolite_set_relationship_adapter = MetaboliteSetRelationshipAdapter().set_left(self.left_db).set_right(self.third_db)
            self.do_etl_step([
                metabolite_set_relationship_adapter,
            ], ComparingLabeler().set_left_labeler(left_labeler).set_right_labeler(third_labeler)
            )

            metabolite_set_relationship_adapter = MetaboliteSetRelationshipAdapter().set_left(self.right_db).set_right(self.third_db)
            self.do_etl_step([
                metabolite_set_relationship_adapter
            ], ComparingLabeler().set_left_labeler(right_labeler).set_right_labeler(third_labeler)
            )


    def do_etl_step(self, input_list: List[InputAdapter], labeler: Labeler = Labeler()):
        self.etl.set_labeler(labeler)
        self.etl.input_adapters = input_list
        self.etl.do_etl()

released_ramp =               "/Users/kelleherkj/IdeaProjects/RaMP-DB-clean/db/RaMP_SQLite_v2.5.4.sqlite"
new_ramp =                    "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0.sqlite"
new_ramp_no_generics =        "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_no_generics.sqlite"
new_ramp_with_collapse =      "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_with_inchi_collapse.sqlite"
new_ramp_with_two_collapses = "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_with_two_inchi_collapses.sqlite"
forward =                     "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_recur_forward.sqlite"
reverse =                     "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_recur_reverse.sqlite"
merge_all =                   "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_merge_all.sqlite"
merge_all_no_generics =       "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_merge_all_no_generics.sqlite"
merge_all_no_kegg =           "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v2.6.0_merge_all_no_kegg.sqlite"

build_engine = build_db_for_comparing_ramp_ids(merge_all, "merge_all", merge_all_no_generics, "merge_all_no_generics")
build_engine.truncate_old_db()
build_engine.do_etl()