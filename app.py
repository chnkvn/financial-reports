import asyncio
import os
import sys
import time
from datetime import date, datetime, timedelta
from itertools import chain
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import srsly
import streamlit as st
from attrs import asdict
from icecream import ic

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from financial_reports.src.data_extraction import (DATE_FORMAT, Asset,
                                                   date_to_str,
                                                   get_current_asset_data,
                                                   get_historical_data)

st.set_page_config(
    page_title="Asset visualizer",
    layout="wide",
    initial_sidebar_state="expanded")
st.title('Asset visualizer')

# Create data/json, data/parquet if they do not exist
for save_path in ["data/jsonl", "data/db"]:
    Path(save_path).mkdir(parents=True, exist_ok=True)

ptf_name = st.text_input('Name of the portfolio (This name will be used the save and load your portfolio.)',
                         st.session_state.get('ptf_name', 'MyPortfolio'),
                         placeholder='MyPortfolio',
                         key='ptf_name')
jsonl_ptf_path = f"data/jsonl/{ptf_name}.jsonl"
db_ptf_path = f"data/db/{ptf_name}.db"
today = date.today()
set_of_assets = {Asset.from_boursorama(get_current_asset_data(a['url'])) for a in srsly.read_jsonl(jsonl_ptf_path)} if Path(jsonl_ptf_path).is_file() else set()

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
            value = st.session_state.get('last_asset','MC'),
            placeholder = "ISIN, Ticker.",
            key='last_asset'
        )
        adding_to_portfolio = st.checkbox('Add to your portfolio', True)
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write(f"Asset: {asset}")
            asset_obj = Asset.from_boursorama(get_current_asset_data(asset))
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

            asset_comp, historic_chart = st.tabs(['Asset composition', 'Historical prices'])
            with asset_comp:
                st.subheader(f"Asset composition")
                asset_comp_chart = plot_piechart(
                    asset_as_dict["assetsComposition"], "name", "value"
                    )
                st.plotly_chart(asset_comp_chart, use_container_width=True)

            with historic_chart:
                st.subheader(f"Historical prices {asset_as_dict['currency']}")
                st.write('You can view the chart in full screen and zoom in the period by selecting the wanted period.')
                history = get_historical_data(asset_as_dict["symbol"])
                df = pd.DataFrame(history)
                df["date"] = df.d.apply(convert_to_date)
                st.plotly_chart(
                    plot_historical_chart(df, asset_as_dict["name"], asset_as_dict["isin"])
                    )

details_col, operations_col = st.columns(2)
#    ["Asset", "Portfolio details", "Portfolio Operations "]


## Portfolio tab
with details_col:
    if submitted and adding_to_portfolio:
        set_of_assets.add(asset_obj)

        srsly.write_jsonl(jsonl_ptf_path, [asdict(a) for a in set_of_assets])

    st.subheader('Followed assets')
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
                    asdict(a, filter = lambda x : x in ['isin', 'asset','name',  'url' ])
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

            
with operations_col:
    st.subheader('Portfolio operations')
    # Initialization
    db_exists = Path(db_ptf_path).is_file()
    if 'name_isin' not in st.session_state:
        st.session_state['name_isin'] = sorted([(a.name, a.isin) for a in set_of_assets], key=lambda x: x[0])
    with duckdb.connect(db_ptf_path) as con:
        if not db_exists:
            con.sql("""CREATE TABLE Operations(
            name VARCHAR(255),
            isin VARCHAR(255),
            date DATE,
            operation VARCHAR(255),
            quantity int,
            value float                
            )""")
            df = st.dataframe(con.sql('select * from Operations').df())
        else:
            df = con.sql('select row_number() over(order by date, isin, name) as id, * from Operations ORDER BY id, date, name, isin DESC ').df()
        st.dataframe(df, hide_index=True)


# Operation tabs
        add_row, del_row = st.tabs(['Add operation', 'Remove operation'])
# Add operation
        with add_row:
            with st.empty().container():
                operation_type = st.selectbox(
                        "Operation type",
                        ("Buy", "Sell", "Dividend", "Split"),
                        index=None,
                        placeholder="Select your operation type.",
                        key='operation_type_buy'
                    )
                operation_date = st.date_input("Date operation", "today",
                                               format = "YYYY-MM-DD", key='operation_date_buy')
                operation_on_asset = st.selectbox("The asset to perform the operation.",
                                              st.session_state['name_isin'],
                                              index=None,
                                              placeholder = "Select the asset.",
                                              key='asset_operation_buy')
                argA, argB = None, None
                if st.session_state.get('operation_type_buy', None) not in ['Split', 'Interest']:
                    if operation_type in ['Buy', 'Sell']:
                        if operation_type == 'Buy':
                            argB = st.number_input("Quantity",value= 1.0, min_value=0.001)
                            argA = st.number_input("Price", min_value=0.01)
                        else: #sell
                            future_asset_quantity = con.sql("""
                            with sum_buy_sell as (select operation, sum(quantity) as sum_qty from Operations
                            group by operation)
                            select (a.sum_qty - e.sum_qty) as diff from sum_buy_sell a
                            INNER JOIN sum_buy_sell e
                            on a.operation!=e.operation
                            where a.operation = 'Buy' and e.operation = 'Sell'
                            """).df()
                            
                            future_asset_quantity = future_asset_quantity['diff'][0]
                            try:
                                argB = st.number_input("Quantity", value=1.0, min_value=0.0, max_value=future_asset_quantity)
                                argA = st.number_input("Price", min_value=0.01)
                            except Exception as e:
                                st.write('You cannot sell assets you do not held.')
                    elif operation_type == 'Dividend':

                        argA = st.number_input("Dividend value", min_value=0.01)
                    st.write(operation_type, operation_date, argA, argB)
                elif st.session_state.get('operation_type_buy', None) =='Split':
                    argA =  st.text_input("Split ratio",
                                          placeholder = 'Enter the split ratio, e.g. "11:10" or "2:1"'
                                          )
                    if argA:
                        after, before = argA.strip().split(':')
                        if not after.isdecimal() or not before.isdecimal():
                            raise ValueError("You must enter a valid split ratio,"
                                                 " two integer numbers separated by a colon(:).")
                        argA = int(after)/int(before)
                    st.write(operation_type, operation_date, argA, argB)
                if st.button('Add operation'):
                    ic(operation_type,operation_on_asset, operation_date.isoformat(),argA)
                    if operation_type in ['Buy', 'Sell']:
                        con.sql(f'''INSERT INTO Operations (name, isin, date, operation, quantity, value)
                        VALUES ('{operation_on_asset[0].replace("'", "''")}', '{operation_on_asset[1]}','{operation_date.isoformat()}','{operation_type}',{argB}, {argA} )''')
                    elif operation_type in ['Dividend', 'Split']:
                        con.sql(f'''INSERT INTO Operations (name, isin, date, operation, value)
                        VALUES ('{operation_on_asset[0].replace("'", "''")}', '{operation_on_asset[1]}','{operation_date.isoformat()}','{operation_type}', '{argA}' )''')
                    st.rerun()
                    
        # Delete row                
        with del_row:
            with st.form('delete_row'):
                try:
                    row_number = st.number_input("Row number", min_value=1, max_value = len(df.id),
                                                placeholder = 'Row number to remove' )
                except Exception as e:
                    print(e)
                    st.write('Please add an operation before trying to remove one.')
                delete_row = st.form_submit_button('Delete row')
                if delete_row:
                     con.sql(f"""
                     WITH row_nb_table AS (
                     select row_number() over(order by date, isin, name) as id,
                     * from Operations
                     ORDER BY  date, name, isin DESC
                     
                     )
                     Select
                     rnt.id,
                     rnt.name,
                     rnt.isin,
                     rnt.date,
                     rnt.operation,
                     rnt.quantity,
                     rnt.value
                     from row_nb_table rnt
                     Left JOIN Operations 
                     using (isin, date, operation, quantity,value)
                     where rnt.id != {row_number}
                     """)
                     con.table('row_nb_table').show()
                     st.rerun()
