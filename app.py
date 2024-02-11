import os
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
import srsly
import streamlit as st
from attrs import asdict
from icecream import ic

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from financial_reports.src.data_extraction import (Asset, date_to_str,
                                                   get_current_asset_data)

st.title('Asset visualizer')
ptf_name = st.text_input('Name of the portfolio (This name will be used the save and load your portfolio.)', 'MyPortfolio', placeholder='MyPortfolio')

# Create data/json, data/parquet if they do not exist
for save_path in ["data/jsonl", "data/parquet"]:
    Path(save_path).mkdir(parents=True, exist_ok=True)

jsonl_ptf_path = f"data/jsonl/{ptf_name}.jsonl"
set_of_assets = {Asset.from_boursorama(a) for a in srsly.read_jsonl(jsonl_ptf_path)} if Path(jsonl_ptf_path).is_file()  else set()

def plot_piechart(data:Iterable, cat_name:str='name', value:str='value'):
    """Extract varible names and their values.
    Returns a pie chart."""
    categories = []
    values = []
    for d in data:
        categories.append(d[cat_name])
        values.append(d[value])
    fig = go.Figure(data=[go.Pie(labels=categories, values=values)])
    return fig

with st.form("sidebar"):
    with st.sidebar:
        asset = st.text_input(
            "Enter an ISIN. You may also enter a name or a ticker, but you might get some errors.",
            placeholder = "ISIN, Ticker.",
        )
        adding_to_portfolio = st.checkbox('Add to your portfolio', True)
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write(f"Asset: {asset}")
            asset_obj = Asset.from_boursorama(get_current_asset_data(asset))

#tabs
asset_tab, portfolio_tab= st.tabs(
    ["Asset", "Portfolio"]
)

with asset_tab:
    if submitted:
        st.header(f'Name: {asset_obj.name}')
        asset_as_dict = asdict(asset_obj)
        asset_as_dict['tradeDate'] = date_to_str(asset_as_dict['tradeDate'])
        if len(asset_as_dict['lastDividende']) >0:
            asset_as_dict['lastDividende']['date'] = date_to_str(asset_as_dict['lastDividende']['date'])
        st.dataframe(asset_as_dict, column_config={0:'property',1:'value'} , use_container_width=True)

        st.subheader('Asset composition')
        asset_comp_chart = plot_piechart(asset_as_dict['assetsComposition'], 'name', 'value')
        st.plotly_chart(asset_comp_chart, use_container_width=True)

        st.subheader('Sector composition')
        sectors_chart = plot_piechart(asset_as_dict['sectors'], 'name', 'value')
        st.plotly_chart(sectors_chart, use_container_width=True)
        
with portfolio_tab:
    if submitted and adding_to_portfolio:
        set_of_assets.add(asset_obj)
        srsly.write_jsonl(jsonl_ptf_path, [asdict(a) for a in set_of_assets])
    ptf_df = pd.DataFrame([(a.name, a.isin, a.asset, True) for a in set_of_assets], columns = ['asset name', 'isin', 'asset_type', 'In Portfolio'])
    with st.form('update_assets'):
        ptf_df = st.data_editor(
                ptf_df,
            num_rows="dynamic",
                column_config={
                    "In Portfolio": st.column_config.CheckboxColumn(
                        "In portfolio?",
                help="Is the asset in your portfolio?",
                default=True,
                    )
                
        },
        disabled=['asset name', 'isin', 'asset_type'],
        hide_index=True,
    )
        st.write("You can remove assets from the portfolio by selecting rows then click on the basket.")
        update_assets = st.form_submit_button("Update assets")

        if update_assets:
            #new_ptf_df = ptf_df.loc[ptf_df['In Portfolio']]
            srsly.write_jsonl(jsonl_ptf_path, [asdict(a) for a in set_of_assets if a.isin in ptf_df['isin'].tolist()])
            #ptf_df = new_ptf_df
