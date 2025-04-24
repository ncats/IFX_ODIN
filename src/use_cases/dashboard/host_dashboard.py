from src.use_cases.build_from_yaml import HostDashboardFromYaml

dashboard = HostDashboardFromYaml(yaml_file="./src/use_cases/dashboard/pharos_dashboard.yaml")
config = dashboard.configuration.config_dict['dashboard']
api = dashboard.api_adapter

import streamlit as st
st.set_page_config(layout="wide")

st.title(f"Data Browser: {api.label}")

model_names = api.list_data_models()
model_names = sorted(
    model_names,
    key=lambda x: (
        config['tab_order'].index(x) if x in config['tab_order'] else float('inf')
    )
)
if 'others' not in config['tab_order']:
    model_names = [model_name for model_name in model_names if model_name in config['tab_order']]


def get_filter(st, data_model):
    filters = {}
    for key, value in st.session_state.items():
        if value and '|' in key:  # Ensure it's a checkbox key
            filter_model, field, value = key.split('|')
            if filter_model != data_model:
                continue
            if field not in filters:
                filters[field] = []
            if value == 'True':
                filters[field].append(True)
            elif value == 'False':
                filters[field].append(False)
            else:
                filters[field].append(value)
    return filters

tabs = st.container()
with tabs:
    st.markdown(
        """
        <style>
        .stTabs [data-baseweb="tab-list"] {
            display: flex;
            justify-content: space-between;
            width: 100%;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    selected_tab = st.tabs(model_names)
    for i, t in enumerate(selected_tab):
        with t:
            col_left, col_right = st.columns([1, 3])  # Adjust
            with col_left:
                configured_facets = [facet['facets'] for facet in config['facets'] if facet['class'] == model_names[i]]
                if len(configured_facets) == 0:
                    st.write("No facets configured for this model")
                else:
                    facet_list = configured_facets[0]
                    for facet in facet_list:
                        st.markdown(f"<div style='font-size: 2em;'>{facet}</div>", unsafe_allow_html=True)
                        with st.expander(facet, expanded=True):

                            facet_values = api.get_facet_values(model_names[i], facet, get_filter(st, model_names[i]))
                            if facet_values:
                                for value in facet_values:
                                    col1, col2, col3 = st.columns([2, 1, 1])
                                    col1.write(value['facet'])
                                    col2.write(value['count'])
                                    value['selected'] = col3.checkbox("Filter", key=f"{model_names[i]}|{facet}|{value['facet']}", label_visibility="collapsed")
                            else:
                                st.write("No data available for this facet.")
            with col_right:
                total_count = api.get_count(model_names[i], get_filter(st, model_names[i]))

                st.markdown(f"<div style='font-size: 2em;'>Total Count: {total_count}</div>", unsafe_allow_html=True)
                filters = get_filter(st, model_names[i])

                st.write("Selected filters:")
                if not filters:
                    st.write("None")
                else:
                    for key, values in filters.items():
                        st.write(f"{key}: {', '.join([str(val) for val in values])}")


                # Initialize skip in session state if not already set
                if f"skip_{model_names[i]}" not in st.session_state:
                    st.session_state[f"skip_{model_names[i]}"] = 0

                page_size = 10

                # Add navigation buttons
                col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
                with col1:
                    if st.button("First Page", key=f"first_{model_names[i]}"):
                        st.session_state[f"skip_{model_names[i]}"] = 0
                with col2:
                    if st.button("Previous", key=f"prev_{model_names[i]}") and st.session_state[f"skip_{model_names[i]}"] > 0:
                        st.session_state[f"skip_{model_names[i]}"] -= page_size  # Adjust page size as needed
                with col4:
                    if st.button("Next", key=f"next_{model_names[i]}"):
                        st.session_state[f"skip_{model_names[i]}"] += page_size  # Adjust page size as needed
                with col5:
                    total_count = api.get_count(model_names[i], get_filter(st, model_names[i]))
                    if st.button("Last Page", key=f"last_{model_names[i]}"):
                        st.session_state[f"skip_{model_names[i]}"] = (total_count // page_size) * page_size  # Adjust page size as needed

                # Display the current range being shown
                start_range = st.session_state[f"skip_{model_names[i]}"] + 1
                end_range = min(st.session_state[f"skip_{model_names[i]}"] + page_size, total_count)
                st.write(f"Showing items {start_range} to {end_range} of {total_count}")

                # Fetch and display the data
                list = api.get_list(model_names[i], get_filter(st, model_names[i]), top=page_size, skip=st.session_state[f"skip_{model_names[i]}"])

                configured_fields = [field['fields'] for field in config['list_fields'] if field['class'] == model_names[i]]

                if len(configured_fields) > 0:
                    if 'others' in configured_fields[0]:
                        all_keys = set(key for item in list for key in item.keys())
                        primary_fields = [key for key in configured_fields[0] if key != 'others']
                        final_fields = primary_fields + [key for key in all_keys if key not in primary_fields]
                    else:
                        all_keys = set(key for item in list for key in item.keys())
                        final_fields = [key for key in configured_fields[0] if key in all_keys]

                    list = [
                        {key: item.get(key, None) for key in final_fields} for item in list
                    ]

                if list:
                    st.dataframe(list)
                else:
                    st.write("No data available for this model.")