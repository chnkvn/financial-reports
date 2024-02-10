import os
import sys

import streamlit as st
from attrs import asdict

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from financial_reports.src.data_extraction import (
    Asset,
    get_current_asset_data,
    date_to_str,
)

st.title("Asset visualizer")
asset_dict = {}

with st.form("sidebar"):
    with st.sidebar:
        asset = st.text_input(
            "Enter an ISIN. You may also enter a name or a ticker, but you might get some errors.",
            placeholder="ISIN, Ticker.",
        )
        adding_to_portfolio = st.checkbox("Add to your portfolio", True)
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write(f"Asset: {asset}")
            asset_obj = Asset.from_boursorama(get_current_asset_data(asset))

# tabs
asset_tab, portfolio_tab = st.tabs(["Asset", "Portfolio"])

with asset_tab:
    if submitted:
        st.text(f"Name: {asset_obj.name}")
        asset_as_dict = asdict(asset_obj)
        asset_as_dict["tradeDate"] = date_to_str(asset_as_dict["tradeDate"])
        if len(asset_as_dict["lastDividende"]) > 0:
            asset_as_dict["lastDividende"]["date"] = date_to_str(
                asset_as_dict["lastDividende"]["date"]
            )
        st.table(asset_as_dict)

with portfolio_tab:
    if adding_to_portfolio and submitted:
        asset_dict[asset] = asset_obj
    st.table(asset_dict)
