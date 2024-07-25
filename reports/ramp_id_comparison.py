from neo4j import GraphDatabase

from src.use_cases.secrets.local_neo4j import alt_neo4j_credentials

print(alt_neo4j_credentials)

driver = GraphDatabase.driver(alt_neo4j_credentials.url, auth=(alt_neo4j_credentials.user, alt_neo4j_credentials.password))

with driver.session() as session:

    mergings = session.run(
        """MATCH (n)<-[r:SharesMetaboliteIDrelationship]-()
            WITH n, COUNT(r) AS relCount
            WHERE relCount > 1
            MATCH (n)<-[r:SharesMetaboliteIDrelationship]-(connectedNode)
            RETURN n, relCount, COLLECT(connectedNode) AS connectedNodes
            ORDER BY relCount DESC""")
    mergings_left_nodes = 0
    mergings_right_nodes = 0

    merged_data = []
    for record in mergings:
        mergings_right_nodes += 1
        node = record['n']
        rel_count = record['relCount']
        connected_nodes = record['connectedNodes']

        demerge_obj = {
            'id': node['id'],
            'nodes': []
        }
        right_label = next(iter(node.labels))
        right_prov = node['provenance']
        for connected in connected_nodes:
            mergings_left_nodes += 1
            left_label = next(iter(connected.labels))
            left_prov = connected['provenance']
            syn_list = []
            for syn in sorted(connected['synonyms']):
                syn_list.append(syn)
            id_list = []
            for id in sorted(connected['equivalent_ids']):
                id_list.append(id)
            demerge_obj['nodes'].append({
                "synonyms": syn_list,
                "ids": id_list
            })
        merged_data.append(demerge_obj)

    unmergings = session.run(
        """
            MATCH (n)-[r:SharesMetaboliteIDrelationship]->()
            WITH n, COUNT(r) AS relCount
            WHERE relCount > 1
            MATCH (n)-[r:SharesMetaboliteIDrelationship]->(connectedNode)
            RETURN n, relCount, COLLECT(connectedNode) AS connectedNodes
            ORDER BY relCount DESC
        """)
    unmergings_left_nodes = 0
    unmergings_right_nodes = 0

    demerged_data = []
    for record in unmergings:
        unmergings_left_nodes += 1
        node = record['n']
        rel_count = record['relCount']
        connected_nodes = record['connectedNodes']

        demerge_obj = {
            'id': node['id'],
            'nodes': []
        }
        left_label = next(iter(node.labels))
        left_prov = node['provenance']
        for connected in connected_nodes:
            unmergings_right_nodes += 1
            right_label = next(iter(connected.labels))
            right_prov = connected['provenance']
            syn_list = []
            for syn in sorted(connected['synonyms']):
                syn_list.append(syn)
            id_list = []
            for id in sorted(connected['equivalent_ids']):
                id_list.append(id)
            demerge_obj['nodes'].append({
                "synonyms": syn_list,
                "ids": id_list
            })
        demerged_data.append(demerge_obj)

def get_merged_table(data_list):
    html_content = """
    <h3 id="merged">Merged nodes: """ + f"{mergings_left_nodes} -> {mergings_right_nodes}" + """</h3>
    <table>
        <tr>
            <th>
                <p>Unmerged Node</p>
            </th>
            <th>
                <p>Merged Nodes</p>
            </th>
        </tr>
    """
    for data in data_list:
        id = data['id']
        nodes = data['nodes']
        html_content += f"""
        <tr>
                <td>
                    <table class="inner-table">
                    <tr>
                        <th>IDs</th>
                        <th>Synonyms</th>
                    </tr>
                </td>
        """
        for node in nodes:
            ids = "<br />".join(node['ids'])
            synonyms = "<br />".join(node['synonyms'])
            html_content += f"""
                    <tr>
                        <td>{ids}</td>
                        <td>{synonyms}</td>
                    </tr>
                """
        html_content += f"""
                </table>
            </td>
            <td>
                {len(nodes)} nodes merged into 1
            </td>
        </tr>
        """
    html_content += f"""
        </td>
    </tr>
    </table>
    """
    return html_content

def get_demerged_table(data_list):
    html_content = """
    <h3 id="unmerged">Unmerged nodes: """ + f"{unmergings_left_nodes} -> {unmergings_right_nodes}" + """</h3>
    <table>
        <tr>
            <th>
                <p>Merged Node</p>
            </th>
            <th>
                <p>Unmerged Nodes</p>
            </th>
        </tr>
    """
    for data in data_list:
        id = data['id']
        nodes = data['nodes']

        html_content += f"""
                <tr>
                    <td>
                    demerged into {len(nodes)} nodes
                    </td>
                    <td>
                        <table class="inner-table">
                            <tr>
                                <th>IDs</th>
                                <th>Synonyms</th>
                            </tr>
            """
        for node in nodes:
            ids = "<br />".join(node['ids'])
            synonyms = "<br />".join(node['synonyms'])
            html_content += f"""
                            <tr>
                                <td>{ids}</td>
                                <td>{synonyms}</td>
                            </tr>
                """
        html_content += """
                        </table>
                    </td>
                </tr>
            """
    html_content += """
            </table>
        """
    return html_content

def generate_html_report(demerged_data, merged_data):
    html_content = """
    <html>
    <head>
        <title>Merged Nodes Report</title>
        <style>
            body {
                font-family: 'verdana', sans-serif;
            }
            table {
                border-collapse: collapse;
                width: 100%;
            }
            tr {
                vertical-align: top;
            }
            th, td {
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #feffdf;
            }
            tr:nth-child(even) {
                background-color: #dde0ab;
            }
            tr:nth-child(odd) {
                background-color: #ffffff;
            }
            .inner-table tr:nth-child(even) {
                background-color: #b7dbb9;
            }
            .inner-table tr:nth-child(odd) {
                background-color: #ffffff;
            }        </style>
    </head>
    <body>
        <h1>Merged Nodes Report</h1>
        <table>
        <tr><th>Database</th><th>Provenance</th></tr>
        <tr><td>Left: """ + f"{left_label}" + """</td><td>""" + "<br />".join(left_prov) + """</td></tr>
        <tr><td>Right: """ + f"{right_label}" + """</td><td>""" + "<br />".join(right_prov) + """</td></tr>
        </table>
        """
    html_content += f"""
        <a href="#merged"><h3>Total merged nodes: """ + f"{mergings_left_nodes} -> {mergings_right_nodes}" + """</h3></a>   
    """
    html_content += f"""
        <a href="#unmerged"><h3>Total unmerged nodes: """ + f"{unmergings_left_nodes} -> {unmergings_right_nodes}" + """</h3></a>   
    """
    html_content += get_merged_table(merged_data)


    html_content += get_demerged_table(demerged_data)

    html_content += """
    </body>
    </html>
    """

    with open(f"merged_nodes_report_{left_label}-{right_label}.html", "w") as file:
        file.write(html_content)


# Generate the HTML report
generate_html_report(demerged_data, merged_data)


