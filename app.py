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
from attrs.filters import exclude
from icecream import ic

# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_extraction import (
    Asset,
    compute_perf,
    date_to_str,
    get_current_asset_data,
)
from src.portfolio import Portfolio

# page config
st.set_page_config(
    page_title="Asset visualizer", layout="wide", initial_sidebar_state="expanded"
)
st.title("Asset visualizer")

# Create data/json, data/parquet if they do not exist
for save_path in ["data/jsonl", "data/operations"]:
    Path(save_path).mkdir(parents=True, exist_ok=True)

# Portfolio name, accept user input
ptf_name = st.text_input(
    "Name of the portfolio (This name will be used the save and load your portfolio.)",
    st.session_state.get("ptf_name", "MyPortfolio"),
    placeholder="MyPortfolio",
    key="ptf_name",
)

# Load it
portfolio = Portfolio(ptf_name)
st.session_state["name_isin"] = {
    (a.name, a.isin) for a in portfolio.dict_of_assets.values()
}


def plot_piechart(data: Iterable, cat_name: str = "name", value: str = "value"):
    """Extract varible names and their values.
    Returns a pie chart."""
    categories = []
    values = []
    for d in data:
        categories.append(d[cat_name])
        values.append(d[value])
    fig = go.Figure(data=[go.Pie(labels=categories, values=values)])
    return fig


def ptf_piechart(iter_of_dicts: Iterable):
    """Pie chart for portfolio"""
    d = {}
    for i, dict_ in enumerate(iter_of_dicts):
        d[dict_["name"]] = d.get(dict_["name"], 0) + dict_["value"]
    [*categories], [*values] = list(zip(*d.items()))
    values = np.array(values)
    fig = go.Figure(data=[go.Pie(labels=categories, values=values / (i + 1))])
    return fig


def plot_historical_chart(df: pd.DataFrame, name: str, isin: str):
    """Plot historical chart"""
    fig = px.line(df, x="date", y="c", title=f"{name} - {isin}")
    return fig


# Sidebar
with st.form("sidebar"):
    with st.sidebar:
        # User input for isin
        asset = st.text_input(
            "Enter an ISIN. You may also enter a name or a ticker, but you might get some errors."
            "\nPrefilled with MC, the ticker of LVMH stock.",
            value=st.session_state.get("last_asset", "MC"),
            placeholder="ISIN, Ticker.",
            key="last_asset",
        )
        adding_to_portfolio = st.checkbox("Add to your portfolio", True)
        submitted = st.form_submit_button("Submit")
        if submitted:
            # scrap first result associated to the user input
            st.write(f"Asset: {asset}")
            asset_obj = Asset.from_boursorama(get_current_asset_data(asset))
            st.header(f"Name: {asset_obj.name}")
            asset_as_dict = asdict(asset_obj, filter=exclude("_quotations"))
            asset_as_dict["tradeDate"] = date_to_str(asset_as_dict["tradeDate"])
            if len(asset_as_dict["lastDividende"]) > 0:
                asset_as_dict["lastDividende"]["date"] = date_to_str(
                    asset_as_dict["lastDividende"]["date"]
                )
            # display the data
            st.dataframe(
                asset_as_dict,
                column_config={0: "property", 1: "value"},
                use_container_width=True,
            )

            asset_comp, historic_chart = st.tabs(
                ["Asset composition", "Historical prices"]
            )

            with asset_comp:
                # Plot asset composition
                st.subheader("Asset composition")
                asset_comp_chart = plot_piechart(
                    asset_as_dict["assetsComposition"], "name", "value"
                )
                st.plotly_chart(asset_comp_chart, use_container_width=True)

            with historic_chart:
                # plot historical chart
                st.subheader(f"Historical prices {asset_as_dict['currency']}")
                perf_dict = pd.DataFrame(
                    [
                        {
                            key: compute_perf(asset_obj.quotations[key])
                            for key in asset_obj.quotations
                        }
                    ]
                ).T
                perf_dict.columns = ["Performance"]
                st.dataframe(perf_dict)
                st.write(
                    "You can view the chart in full screen and zoom in the period by selecting the wanted period."
                )
                st.plotly_chart(
                    plot_historical_chart(
                        asset_obj.quotations["inception"],
                        asset_as_dict["name"],
                        asset_as_dict["isin"],
                    )
                )

# Body
operations_col, details_col = st.tabs(["Portfolio Operations", "Portfolio details"])

## Portfolio tab
with details_col:
    if submitted and adding_to_portfolio:
        # Add to dict of assets the new asset
        portfolio.dict_of_assets[asset_obj.isin] = asset_obj
        st.session_state["name_isin"].add((asset_obj.isin, asset_obj.name))
        # Update jsonl
        srsly.write_jsonl(
            portfolio.jsonl_ptf_path,
            [
                asdict(a, filter=exclude("_quotations"))
                for a in portfolio.dict_of_assets.values()
            ],
        )

    with st.expander("Followed assets"):
        # summary of followed assets in an expandable window
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
                for a in portfolio.dict_of_assets.values()
            ]
        )

        ptf_df.insert(0, "in_ptf", True)
        with st.form("update_assets"):
            # To modify the followed assets
            ptf_df = st.data_editor(
                ptf_df,
                column_config={
                    "in_ptf": st.column_config.CheckboxColumn(
                        "In portfolio?",
                        help="Select your current assets.",
                        default=True,
                    ),
                    "_quotations": None,
                },
                disabled=[column for column in ptf_df.columns if column != "in_ptf"],
                hide_index=True,
            )
            update_assets = st.form_submit_button("Update assets")
            if update_assets:
                keep_isin = duckdb.sql(
                    """SELECT isin from ptf_df where in_ptf='True'"""
                ).fetchall()
                srsly.write_jsonl(
                    portfolio.jsonl_ptf_path,
                    [
                        asdict(portfolio.dict_of_assets[a])
                        for a in portfolio.dict_of_assets
                        if a in set(chain.from_iterable(keep_isin))
                    ],
                )
                st.rerun()

    # If there are some operations, display summary and stats about the portfolio
    if len(portfolio.operations_df["isin"]) > 0:
        # Summary by asset
        st.subheader("Portfolio lines")
        st.dataframe(
            portfolio.assets_summary.round(2),
            hide_index=True,
            column_config={"operations": None},
        )

        # Portfolio summary
        st.subheader("Overall stats")
        st.dataframe(portfolio.portfolio_summary.round(2), hide_index=True)

        # Historical chart
        filled_area_plot = px.area(
            portfolio.asset_values, x="date", y="value", color="name"
        )
        st.subheader("Historical records")
        st.plotly_chart(filled_area_plot, use_container_width=True)

        # Map French asset terminology to their English counterpart
        asset_types = {
            "actions": "stock",
            "obligations": "bond",
            "immobilier": "real estate",
            "matières premières": "commodities",
            "liquidités": "cash",
            "autres": "other",
        }
        total_assets_comp = [
            {
                "name": asset_types.get(d["name"].lower(), d["name"]),
                "value": d["value"] * k,
            }
            for i, (a, k) in enumerate(
                zip(
                    portfolio.assets_summary["isin"].tolist(),
                    portfolio.assets_summary["proportion (%)"].tolist(),
                )
            )
            for d in portfolio.dict_of_assets[a].assetsComposition
        ]

        ptf_asset_comp, ptf_asset_proportion = st.columns(2)
        with ptf_asset_comp:
            # Asset types repartition
            st.subheader("Portfolio asset repartition")
            ptf_asset_comp_chart = ptf_piechart(total_assets_comp)
            st.plotly_chart(ptf_asset_comp_chart, use_container_width=True)

        with ptf_asset_proportion:
            # Chart of assets
            st.subheader("Proportion of each asset in your portfolio")
            proportion_fig = px.pie(
                portfolio.assets_summary,
                values="valuation",
                names="name",
                title="Proportion of each asset in your portfolio",
            )
            st.plotly_chart(proportion_fig, use_container_width=True)


with operations_col:
    st.subheader("Portfolio operations")
    st.dataframe(portfolio.operations_df, hide_index=True)

    # Operation tabs
    add_row, del_row = st.tabs(["Add operation", "Remove operation"])
    # Add operation
    with add_row:
        with st.empty().container():
            operation_type = st.selectbox(
                "Operation type",
                ("Buy", "Sell", "Dividend", "Split"),
                index=None,
                placeholder="Select your operation type.",
                key="operation_type_add",
            )
            operation_date = st.date_input(
                "Date operation", "today", format="YYYY-MM-DD", key="operation_date_buy"
            )
            operation_on_asset = st.selectbox(
                "The asset to perform the operation.",
                st.session_state["name_isin"],
                index=None,
                placeholder="Select the asset.",
                key="asset_operation_add",
            )
            argA, argB, taxes_fees = None, None, 0
            if st.session_state.get("operation_type_add", None) not in [
                "Split",
                "Interest",
            ]:
                if operation_type in ["Buy", "Sell"]:
                    taxes_fees = st.number_input("Taxes/Fees", min_value=0.00)
                    if operation_type == "Buy":
                        argB = st.number_input("Quantity", value=1.0, min_value=0.001)
                        argA = st.number_input("Price", min_value=0.00)
                    else:  # sell
                        # Do not allow to sell more assets that we own
                        try:
                            copy_operations_df = portfolio.operations_df.copy()
                            asset_operations = duckdb.sql(
                                f"""select operation, sum(quantity) as sum_qty
                            from copy_operations_df
                            where name='{st.session_state["asset_operation_add"][0]}'
                            and isin='{st.session_state["asset_operation_add"][1]}'
                            group by operation"""
                            ).fetchall()
                            asset_operations = {
                                op: value for (op, value) in asset_operations
                            }

                            argB = st.number_input(
                                "Quantity",
                                value=1.0,
                                min_value=0.0,
                                max_value=asset_operations.get("Buy", 0)
                                - asset_operations.get("Sell", 0),
                            )
                            argA = st.number_input("Price", min_value=0.00)

                        except Exception as e:
                            print(e)
                            # Cannot sell assets we do not own.
                            st.write("You cannot sell assets you do not own.")
                            # Disable add operation button
                            st.session_state["invalid_operation"] = 1
                elif operation_type == "Dividend":
                    argA = st.number_input("Dividend value", min_value=0.01)

            elif st.session_state.get("operation_type_add", None) == "Split":
                argA = st.text_input(
                    "Split ratio",
                    placeholder='Enter the split ratio, e.g. "11:10" or "2:1"',
                )
                if argA:
                    # Check the ratio is valid
                    after, before = argA.strip().split(":")
                    if not after.isdecimal() or not before.isdecimal():
                        raise ValueError(
                            "You must enter a valid split ratio,"
                            " two integer numbers separated by a colon(:)."
                        )
                    argA = int(after) / int(before)
            # Check all arguments are filled to enable add operation button
            if all([operation_on_asset is not None, operation_type is not None]):
                st.session_state["invalid_operation"] = 0

            # Append operation to csv
            if st.button(
                "Add operation", disabled=st.session_state.get("invalid_operation", 1)
            ):
                portfolio.operations_df.loc[len(portfolio.operations_df.operation)] = {
                    "name": operation_on_asset[0],
                    "isin": operation_on_asset[1],
                    "date": operation_date.isoformat(),
                    "operation": operation_type,
                    "quantity": argB,
                    "value": argA,
                    "fees": taxes_fees,
                }
                portfolio.operations_df.to_csv(
                    portfolio.csv_ptf_path,
                    index=False,
                    columns=[
                        col
                        for col in portfolio.operations_df.columns
                        if not col.startswith("id")
                    ],
                )
                # elif operation_type in ['Dividend', 'Split']:

                # duckdb.sql(f'COPY operations TO {csv_ptf_path}')
                st.rerun()

    # Delete row
    with del_row:
        with st.form("delete_row"):
            try:
                row_number = st.number_input(
                    "Row number",
                    min_value=1,
                    max_value=len(portfolio.operations_df.operation),
                    placeholder="Row number to remove",
                )
            except Exception as e:
                # Cannot remove an operation if there is no operation registered.
                print(e)
                st.write("Please add an operation before trying to remove one.")
            delete_row = st.form_submit_button("Delete row")
            if delete_row:
                # Keep all the operations except the n_th
                duckdb.sql(
                    f"""
                 WITH row_nb_table AS (
                 select row_number() over(order by date, isin, name) as id,
                 * from '{portfolio.csv_ptf_path}'
                 ORDER BY  date, name, isin DESC
                 )
                 Select
                 rnt.name,
                 rnt.isin,
                 rnt.date,
                 rnt.operation,
                 rnt.quantity,
                 rnt.value
                 rnt.fees
                 from row_nb_table rnt
                 Left JOIN '{portfolio.csv_ptf_path}'
                 using (isin, date, operation, quantity,value,fees)
                 where rnt.id != {row_number}
                 """
                ).write_csv(portfolio.csv_ptf_path)
                st.rerun()
