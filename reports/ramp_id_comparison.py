from typing import List

from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://ifxdev.ncats.nih.gov:8046", auth=("neo4j", "password"))

class Recombination:
    left_nodes: List[dict]
    right_nodes: List[dict]
    def __init__(self, left_nodes, right_nodes):
        self.left_nodes = left_nodes
        self.right_nodes = right_nodes

    def __repr__(self):
        return_string = ""
        return_string += ",".join([node['id'] for node in self.left_nodes])
        return_string += " -> "
        return_string += ",".join([node['id'] for node in self.right_nodes])
        return return_string


class RecombinationList:
    recombinations: List[Recombination]

    def get_all_ids(self, field):
        ids = []
        for recomb in self.recombinations:
            for node in getattr(recomb, field):
                ids.append(node['id'])
        return ids

    def get_all_left_ids(self):
        return self.get_all_ids('left_nodes')

    def get_all_right_ids(self):
        return self.get_all_ids('right_nodes')

    def __init__(self):
        self.recombinations = []

    def add(self, left_nodes, right_nodes):
        self.recombinations.append(Recombination(left_nodes, right_nodes))

    def get_mergers(self):
        mergers = [recomb for recomb in self.recombinations if len(recomb.left_nodes) > 1 and len(recomb.right_nodes) == 1]
        sorted_mergers = sorted(mergers, key=lambda recomb: len(recomb.left_nodes), reverse=True)
        return sorted_mergers

    def get_unmergers(self):
        mergers = [recomb for recomb in self.recombinations if len(recomb.left_nodes) == 1 and len(recomb.right_nodes) > 1]
        sorted_mergers = sorted(mergers, key=lambda recomb: len(recomb.right_nodes), reverse=True)
        return sorted_mergers

    def get_recombinations(self):
        mergers = [recomb for recomb in self.recombinations if len(recomb.left_nodes) > 1 and len(recomb.right_nodes) > 1]
        sorted_mergers = sorted(mergers, key=lambda recomb: len(recomb.right_nodes) + len(recomb.left_nodes), reverse=True)
        return sorted_mergers

    def merge_all(self):
        recombinations = []
        left_dict = {}
        right_dict = {}
        for recomb in self.recombinations:
            existing_recombination = None
            for left in recomb.left_nodes:
                if left['id'] in left_dict:
                    existing_recombination = left_dict[left['id']]
            for right in recomb.right_nodes:
                if right['id'] in right_dict:
                    existing_recombination = right_dict[right['id']]
            if existing_recombination is None:
                recombinations.append(recomb)
                for left in recomb.left_nodes:
                    left_dict[left['id']] = recomb
                for right in recomb.right_nodes:
                    right_dict[right['id']] = recomb
            else:
                existing_left_nodes = [node['id'] for node in existing_recombination.left_nodes]
                existing_right_nodes = [node['id'] for node in existing_recombination.right_nodes]
                for left in recomb.left_nodes:
                    if left['id'] not in existing_left_nodes:
                        existing_recombination.left_nodes.append(left)
                for right in recomb.right_nodes:
                    if right['id'] not in existing_right_nodes:
                        existing_recombination.right_nodes.append(right)
        self.recombinations = recombinations


    def __repr__(self):
        return_string = f"{len(self.recombinations)}\n"

        for recomb in self.recombinations:
            return_string += f"{recomb}\n"

        return return_string

    def generate_html_view(self, left_prop_dict, right_prop_dict,
                           left_path_dict, right_path_dict,
                           left_class_dict, right_class_dict):

        def get_node_table(nodes, prop_dict, path_dict, class_dict):
            node_text = f"""
            <table class="styled-table"><tbody>
                    <tr>
                        <th>RampID</th><th>IDs</th><th>Synonyms</th>
                        <th>Pathways</th>
                        <th>Subclasses</th>
                        <th>MWs</th><th>inchi Keys</th>
                    </tr>
                    """
            for node in nodes:
                if node['id'] in class_dict:
                    classes = class_dict[node['id']]
                else:
                    classes = []
                if node['id'] in path_dict:
                    pathways = path_dict[node['id']]
                else:
                    pathways = []
                if node['id'] in prop_dict:
                    props = prop_dict[node['id']]
                    MWs = [str(p['mw']) for p in props if p['mw'] is not None]
                    InchiKeys = [str(p['inchi_key']) for p in props if p['inchi_key'] is not None]
                else:
                    MWs = []
                    InchiKeys = []
                node_text += f"""
                        <tr>
                            <td>{node['id']}</td>
                            <td>{'<br />'.join(sorted(node['xref']))}</td>
                            <td>{'<br />'.join(sorted(node['synonyms'] if 'synonyms' in node else []))}</td>
                            <td>-- {'<br />-- '.join(sorted(pathways))}</td>
                            <td>-- {'<br />-- '.join(sorted(classes))}</td>
                            <td>{'<br />'.join(MWs)}</td>
                            <td>{'<br />'.join(InchiKeys)}</td>
                        </tr>"""
            node_text += """</tbody></table>"""
            return node_text
        def create_table(recombs, table_name):
            rows = ""
            for i, recomb in enumerate(recombs):
                left_nodes = f"""<h3>{len(recomb.left_nodes)} nodes</h3>"""
                right_nodes = f"""<h3>{len(recomb.right_nodes)} nodes</h3>"""
                if len(recomb.left_nodes) == 1:
                    left_nodes += ', '.join(node['id'] for node in recomb.left_nodes)
                    left_nodes += '<br /><br />'
                    left_nodes += '<br />'.join(sorted(recomb.left_nodes[0]['xref']))
                else:
                    left_nodes += get_node_table(recomb.left_nodes, left_prop_dict, left_path_dict, left_class_dict)
                if len(recomb.right_nodes) == 1:
                    right_nodes += ', '.join(node['id'] for node in recomb.right_nodes)
                    right_nodes += '<br /><br />'
                    right_nodes += '<br />'.join(sorted(recomb.right_nodes[0]['xref']))
                else:
                    right_nodes += get_node_table(recomb.right_nodes, right_prop_dict, right_path_dict, right_class_dict)
                row_class = "odd-row" if i % 2 == 0 else "even-row"
                rows += f"<tr class='{row_class}'><td>{left_nodes}</td><td>{right_nodes}</td></tr>"

            return f"""
                <h2 id="{table_name}">{table_name} ({len(recombs)})</h2>
                <a href="">Return to top</a>
                <table class="styled-table">
                    <thead>
                        <tr>
                            <th style="max-width: 50%;">Left Nodes</th>
                            <th style="max-width: 50%;">Right Nodes</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
                """

        mergers_html = create_table(self.get_mergers(), "Mergers")
        unmergers_html = create_table(self.get_unmergers(), "Unmergers")
        recombinations_html = create_table(self.get_recombinations(), "Recombinations")

        html_content = f"""
        <html>
        <head>
            <title>RaMP Recombination Report</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                }}
                h2 {{
                    color: #333;
                }}
                .styled-table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 25px 0;
                    font-size: 0.9em;
                    box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);
                }}
                .styled-table thead tr {{
                    background-color: #009879;
                    color: #ffffff;
                    text-align: left;
                }}
                .styled-table th, .styled-table td {{
                    vertical-align: top;
                    padding: 12px 15px;
                    border-bottom: 1px solid #dddddd;
                }}
                .styled-table tbody tr:nth-of-type(even) {{
                    background-color: #f3f3f3;
                }}
                .styled-table tbody tr:nth-of-type(odd) {{
                    background-color: #d9e7e7;
                }}
                .styled-table tbody tr:last-of-type {{
                    border-bottom: 2px solid #009879;
                }}
                .styled-table tbody tr:hover {{
                    border: 3px solid;
                }}
            </style>
        </head>
        <body>


            <table class="styled-table"><tbody>
            <tr>
            <th>Left Nodes</th><th>Right Nodes</th>
            </tr>
            </tbody></table>

            <div class="nav-links">
                <a href="#Unmergers">Unmergers ({len(self.get_unmergers())})</a>
                <a href="#Mergers">Mergers ({len(self.get_mergers())})</a>
                <a href="#Recombinations">Recombinations ({len(self.get_recombinations())})</a>
            </div>

            {unmergers_html}
            {mergers_html}
            {recombinations_html}
        </body>
        </html>
        """

        with open("recombination_report.html", "w") as file:
            file.write(html_content)
        print("HTML file generated: recombination_report.html")


with driver.session() as session:

    mergings = session.run(
        """match (n)-[r:SharesMetaboliteIDrelationship]->(other)
            with collect(n) as old_nodes, other as new_node
            where size(old_nodes) > 1
            return old_nodes, new_node""")

    recombinations = RecombinationList()

    for record in mergings:
        recombinations.add(record['old_nodes'], [record['new_node']])

    unmergings = session.run(
        """match (n)-[r:SharesMetaboliteIDrelationship]->(other)
            with n as old_node, collect(other) as new_nodes
            where size(new_nodes) > 1
            return old_node, new_nodes""")

    for record in unmergings:
        recombinations.add([record['old_node']], record['new_nodes'])

    old_count = 0
    count = len(recombinations.recombinations)
    while old_count != count:
        old_count = count
        print(f'merging {old_count} recombinations')
        recombinations.merge_all()
        count = len(recombinations.recombinations)
    print(f'converged on {len(recombinations.recombinations)} recombinations')

    print(f"found {len(recombinations.get_mergers())} pure mergings")

    count = 0
    for merge in recombinations.get_unmergers():
        count += len(merge.right_nodes)
    print('total unmerged nodes: ', count)
    print(f"found {len(recombinations.get_unmergers())} pure unmergings")
    print(f"found {len(recombinations.get_recombinations())} recombinations")

    left_chem_props = session.run(
        f"""match (node:released_ramp_Metabolite)-[r:MetaboliteChemPropsRelationship]->(prop)
        where node.id in {recombinations.get_all_left_ids()}
            return node.id as id, collect(prop) as props
            """)
    left_prop_dict = {rec['id']: rec['props'] for rec in left_chem_props}

    right_chem_props = session.run(
        f"""match (node:ramp_with_refmet_Metabolite)-[r:MetaboliteChemPropsRelationship]->(prop)
        where node.id in {recombinations.get_all_right_ids()}
            return node.id as id, collect(prop) as props
            """)
    right_prop_dict = {rec['id']: rec['props'] for rec in right_chem_props}

    left_pathways = session.run(
        f"""match (node)-[r:AnalytePathwayRelationship]->(path)
        where node.id in {recombinations.get_all_left_ids()}
            return node.id as id, collect(path.name) as pathways"""
    )
    left_path_dict = {rec['id']: list(set(rec['pathways'])) for rec in left_pathways}
    right_pathways = session.run(
        f"""match (node)-[r:AnalytePathwayRelationship]->(path)
        where node.id in {recombinations.get_all_right_ids()}
            return node.id as id, collect(path.name) as pathways"""
    )
    right_path_dict = {rec['id']: list(set(rec['pathways'])) for rec in right_pathways}

    left_classes = session.run(
        f"""match (node)-[r:MetaboliteClassRelationship]->(class)
        where node.id in {recombinations.get_all_left_ids()}
        and ( class.level = "ClassyFire_sub_class" or class.level = "LipidMaps_sub_class" )
            return node.id as id, collect(class.name) as classes"""
    )
    left_class_dict = {rec['id']: list(set(rec['classes'])) for rec in left_classes}

    right_classes = session.run(
        f"""match (node)-[r:MetaboliteClassRelationship]->(class)
        where node.id in {recombinations.get_all_right_ids()}
        and ( class.level = "ClassyFire_sub_class" or class.level = "LipidMaps_sub_class" )
            return node.id as id, collect(class.name) as classes"""
    )
    right_class_dict = {rec['id']: list(set(rec['classes'])) for rec in right_classes}

    prov_nodes = session.run(
        f"""match (n)-[:SharesMetaboliteIDrelationship]-(o)
            return n,o
            limit 1"""
    )

    recombinations.generate_html_view(
        left_prop_dict, right_prop_dict,
        left_path_dict, right_path_dict,
        left_class_dict, right_class_dict)


