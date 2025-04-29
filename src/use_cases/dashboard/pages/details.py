import streamlit as st
st.set_page_config(layout="wide")

from src.use_cases.build_from_yaml import HostDashboardFromYaml

params = st.query_params
item_id = params.get("id")
model = params.get("model")
yaml_file = params.get("api")


dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
config = dashboard.configuration.config_dict['dashboard']
api = dashboard.api_adapter

st.title(f"Data Browser: {api.label}")

if api.credentials.internal_url != api.credentials.url:
    st.write(f"{api.credentials.url} ({api.credentials.internal_url})")
else:
    st.write(api.credentials.url)

result = api.get_details(model, item_id)
details = result.details

st.header(f"{model} Details")
st.write(details)

edge_collections = api.get_edge_types(model)

if len(edge_collections['outgoing']) > 0:
    st.subheader("Outgoing Edges")
    for edge in edge_collections['outgoing']:
        st.write(edge['edge_collection'])
        edge_list = api.get_edge_list(model, edge['edge_collection'], start_id=item_id)
        if edge_list is not None:
            edges = [row['edge'] for row in edge_list]
            nodes = [row['node'] for row in edge_list]
            col1, col2 = st.columns(2)
            with col1:
                st.dataframe(edges)
            with col2:
                st.dataframe(nodes)
        else:
            st.write(None)

if len(edge_collections['incoming']) > 0:
    st.subheader("Incoming Edges")
    for edge in edge_collections['incoming']:
        st.write(edge)
        edge_list = api.get_edge_list(model, edge['edge_collection'], end_id=item_id)
        if edge_list is not None:
            edges = [row['edge'] for row in edge_list]
            nodes = [row['node'] for row in edge_list]
            col1, col2 = st.columns(2)
            with col1:
                st.dataframe(edges)
            with col2:
                st.dataframe(nodes)
        else:
            st.write(None)
