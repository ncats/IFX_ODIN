from sqlalchemy import String

from src.input_adapters.pharos_mysql.tables import create_classes
from src.models.go_term import GoType

mysql_classes = create_classes(protein_id_type=String(18))
TCRDBase = mysql_classes['Base']
mysql_Protein = mysql_classes['Protein']
mysql_Target = mysql_classes['Target']
mysql_t2tc = mysql_classes['T2TC']
mysqlGO = mysql_classes['GO']
mysqlGOParent = mysql_classes['GOParent']


def goterm_converter(obj: dict) -> mysqlGO:
    def get_namespace(obj: dict):
        if obj['type'] == GoType.Process:
            return 'biological_process'
        if obj['type'] == GoType.Function:
            return 'molecular_function'
        if obj['type'] == GoType.Component:
            return 'cellular_component'
        return 'unknown'
    return mysqlGO(go_id=obj['id'], name=obj['term'], namespace=get_namespace(obj), def_=obj['definition'],
                   provenance = obj['provenance'])


def goterm_parent_converter(obj: dict) -> mysqlGOParent:
    return mysqlGOParent(go_id=obj['start_id'], parent_id=obj['end_id'],
                         provenance = obj['provenance'])


def protein_converter(obj: dict) -> mysql_Protein:
    return mysql_Protein(
        id = obj['id'],
        description = obj['name'],
        uniprot = obj['uniprot_id'],
        sym = obj['symbol'],
        seq = obj['sequence'],
        provenance = obj['provenance']
    )


def t2tc_converter(obj: dict) -> mysql_t2tc:
    return mysql_t2tc(target_id=obj['id'], protein_id=obj['id'],
                      provenance = obj['provenance'])


def target_converter(obj: dict) -> mysql_Target:
    return mysql_Target(
        id = obj['id'],
        name = obj['name'],
        ttype = 'Single Protein',
        fam = obj['idg_family'],
        provenance = obj['provenance']
    )
