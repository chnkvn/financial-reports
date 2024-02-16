import os
import sys
from itertools import chain
from pathlib import Path
from typing import Iterable
from datetime import datetime,timedelta, date
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import srsly
import streamlit as st
from attrs import asdict
from icecream import ic
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from financial_reports.src.data_extraction import (Asset, date_to_str,
                                                   get_current_asset_data, get_historical_data, DATE_FORMAT)

st.title('Asset visualizer')
ptf_name = st.text_input('Name of the portfolio (This name will be used the save and load your portfolio.)', 'MyPortfolio', placeholder='MyPortfolio')

# Create data/json, data/parquet if they do not exist
for save_path in ["data/jsonl", "data/parquet"]:
    Path(save_path).mkdir(parents=True, exist_ok=True)

jsonl_ptf_path = f"data/jsonl/{ptf_name}.jsonl"
set_of_assets = {Asset.from_boursorama(a) for a in srsly.read_jsonl(jsonl_ptf_path)} if Path(jsonl_ptf_path).is_file()  else set()
today = date.today()

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

def ptf_piechart(iter_of_dicts:Iterable):
    d = {}
    for i,dict_ in enumerate(iter_of_dicts):
        d[dict_['name']] = d.get(dict_['name'], 0) + float(dict_['value'])
    [*categories], [*values] = list(zip(*d.items()))
    values = np.array(values)
    fig = go.Figure(data=[go.Pie(labels=categories, values=values/(i+1))])
    return fig

def convert_to_date(nb:int):
    init_date =datetime.strptime('1970-01-01', DATE_FORMAT)
    return init_date + timedelta(days=nb)

def plot_historical_chart(df:pd.DataFrame, name:str, isin:str):
    fig = px.line(df, x="date", y="c", title=f'{name} - {isin}')
    return fig

with st.form("sidebar"):
    with st.sidebar:
        asset = st.text_input(
            "Enter an ISIN. You may also enter a name or a ticker, but you might get some errors.\nPrefilled with MC, the ticker of LVMH stock.",
            value = 'MC',
            placeholder = "ISIN, Ticker.",
        )
        adding_to_portfolio = st.checkbox('Add to your portfolio', True)
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write(f"Asset: {asset}")
            asset_obj = Asset.from_boursorama(get_current_asset_data(asset))

#tabs
# tabs
asset_tab, details_tab, operations_tab = st.tabs(
    ["Asset", "Portfolio details", "Portfolio Operations "]
)

## asset tab
with asset_tab:
    if submitted:
        st.header(f"Name: {asset_obj.name}")
        asset_as_dict = asdict(asset_obj)
        asset_as_dict["tradeDate"] = date_to_str(asset_as_dict["tradeDate"])
        if len(asset_as_dict["lastDividende"]) > 0:
            asset_as_dict["lastDividende"]["date"] = date_to_str(
                asset_as_dict["lastDividende"]["date"]
            )
        st.dataframe(
            asset_as_dict,
            column_config={0: "property", 1: "value"},
            use_container_width=True,
        )

        asset_comp, historic_chart = st.columns(2)
        with asset_comp:
            st.subheader("Asset composition")
            asset_comp_chart = plot_piechart(
                asset_as_dict["assetsComposition"], "name", "value"
            )
            st.plotly_chart(asset_comp_chart, use_container_width=True)

        with historic_chart:
            st.subheader(f"Historical prices {asset_as_dict['currency']}")
            history = get_historical_data(asset_as_dict["symbol"])
            df = pd.DataFrame(history)
            df["date"] = df.d.apply(convert_to_date)
            st.plotly_chart(
                plot_historical_chart(df, asset_as_dict["name"], asset_as_dict["isin"])
            )


## Portfolio tab
with details_tab:
    if submitted and adding_to_portfolio:
        set_of_assets.add(asset_obj)

        srsly.write_jsonl(jsonl_ptf_path, [asdict(a) for a in set_of_assets])

        # ptf_df = pd.DataFrame([(a.name, a.isin, a.asset) for a in set_of_assets], columns = ['asset name', 'isin', 'asset_type'])
    ptf_df = pd.DataFrame(
        [
            {
                k: v
                for k, v in asdict(a).items()
                if k
                not in [
                    "tradeDate",
                    "assetsComposition",
                    "url",
                    "referenceIndex",
                    "morningstarCategory",
                ]
            }
            for a in set_of_assets
        ]
    )

    ptf_df.insert(0, "in_ptf", True)
    with st.form("update_assets"):

        ptf_df = st.data_editor(
            ptf_df,
            column_config={
                "in_ptf": st.column_config.CheckboxColumn(
                    "In portfolio?",
                    help="Select your current assets.",
                    default=True,
                )
            },
            disabled=[column for column in ptf_df.columns if column != "in_ptf"],
            hide_index=True,
        )

        update_assets = st.form_submit_button("Update assets")
        if update_assets:
            ptf_df = ptf_df[ptf_df["in_ptf"]]
            srsly.write_jsonl(
                jsonl_ptf_path,
                [
                    asdict(a)
                    for a in set_of_assets
                    if a.isin in ptf_df[ptf_df["in_ptf"]]["isin"].tolist()
                ],
            )
            st.rerun()
    if len(set_of_assets) > 0:
        total_assets_comp = chain.from_iterable(
            [a.assetsComposition for a in set_of_assets]
        )
        # total_sectors_comp = chain.from_iterable([a.sectors for a in set_of_assets if a.sectors])
        ptf_asset_comp, ptf_sector_comp = st.columns(2)
        with ptf_asset_comp:
            st.subheader("Portfolio asset repartition")
            ptf_asset_comp_chart = ptf_piechart(total_assets_comp)
            st.plotly_chart(ptf_asset_comp_chart, use_container_width=True)

with operations_tab:
    with st.form('operation'):
        operation_type = st.selectbox(
            "Operation type",
            ("Buy", "Sell", "Dividend", "Interest", "Split"),
            index=None,
            placeholder="Select your operation type.",
        )
        operation_date = st.date_input("Date operation", "today", format = "YYYY-MM-DD")
        operation_on_asset = st.selectbox("The asset to perform the operation.",
                                          sorted([(a.name, a.isin) for a in set_of_assets], key=lambda x: x[0]),
                                          index=None,
                                          placeholder = "Select the asset.")
        update_operation = st.form_submit_button("Add operation")
        if update_operation:
            argA, argB = None, None
            if operation_type in ['Buy', 'Sell']:
                argA = st.number_input("Quantity", min_value=1)
                argB = st.number_input("Price", min_value=0.01)
            elif operation_type == 'Dividend':
                argA = st.number_input("Dividend value", min_value=0.01)
            elif operation_type =='Split':
                argA =  st.text_input("Split ratio",
                                         placeholder = 'Enter the split ratio, e.g. "11:10" or "2:1"'
                                             )

                                             
            st.write(operation_type, operation_date, argA, argB)
