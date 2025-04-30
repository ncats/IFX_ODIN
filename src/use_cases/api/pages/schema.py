import streamlit as st
from pyvis.network import Network
import streamlit.components.v1 as components

from src.use_cases.build_from_yaml import HostDashboardFromYaml

params = st.query_params
yaml_file = params.get("api")
dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
api = dashboard.api_adapter

G = api.get_graph_representation(False)

# Create a PyVis network
net = Network(height="500px", width="100%", notebook=False, directed=True)
net.repulsion(spring_length=300)
net.from_nx(G)

# Save and read as HTML
net.save_graph("graph.html")
with open("graph.html", 'r', encoding='utf-8') as f:
    html = f.read()

st.header('Labeled graph')
components.html(html, height=600, scrolling=True)

G = api.get_graph_representation(True)

# Create a PyVis network
net = Network(height="500px", width="100%", notebook=False, directed=True)
net.repulsion(spring_length=300)
net.from_nx(G)

# Save and read as HTML
net.save_graph("graph.html")
with open("graph.html", 'r', encoding='utf-8') as f:
    html = f.read()

# Display in Streamlit
st.header('Class graph')
components.html(html, height=600, scrolling=True)
