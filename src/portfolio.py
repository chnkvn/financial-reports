from functools import lru_cache
from datetime import date
from itertools import chain, pairwise
from math import floor
from pathlib import Path
from pyxirr import xirr
import duckdb
import pandas as pd
import srsly
from attrs import define, field
from src.data_extraction import (DATE_FORMAT,
                                                   TODAY,
                                                   map_period_to_filter,
                                                   Asset,
                                                   compute_perf,
                                                   date_to_str,
                                                   get_current_asset_data,
                                                   get_historical_data)
from icecream import ic

@define
class Portfolio:
    name: str
    jsonl_ptf_path:str = field(init=False)
    csv_ptf_path: str = field(init=False)
    dict_of_assets:dict = field(init=False)
    operations_df:pd.DataFrame = field(init=False)
    _assets_summary: pd.DataFrame = None
    _asset_values: pd.DataFrame = None
    
    def __attrs_post_init__(self):
        self.jsonl_ptf_path = f"data/jsonl/{self.name}.jsonl"
        self.csv_ptf_path = f"data/db/{self.name}.csv"
        self.dict_of_assets = {a['isin'] : Asset.from_boursorama(
            get_current_asset_data(a['url'])
        )
                               for a in srsly.read_jsonl(self.jsonl_ptf_path)} if Path(self.jsonl_ptf_path).is_file() else {}
        self.operations_df = self.load_operations()
        # duckdb cannot request directly on class attribute
        
        distinct_isins = self.operations_df['isin'].unique()
        self.dict_of_assets.update(
            {isin:Asset.from_boursorama(get_current_asset_data(isin))
             for isin in distinct_isins
             })
        

    def load_operations(self) -> pd.DataFrame:
        """Initialize or read a csv file to get a dataframe containing the operations"""
        db_exists = Path(self.csv_ptf_path).is_file()

        if not db_exists:
            operations =pd.DataFrame({column_name: [] for column_name in ['name', 'isin', 'date', 'operation', 'quantity', 'value']})
            
        else:
            operations = pd.read_csv(self.csv_ptf_path)
            operations = duckdb.sql(f'''
            select row_number() over(order by date, isin, name) as id, * from operations ORDER BY id, date, name, isin DESC ''').df()
        return operations

    @property
    def assets_summary(self) -> pd.DataFrame:
        """"""
        if self._assets_summary is None:
            df = self.operations_df.copy()
            assets = []
            
            for isin in df['isin'].unique():
                ic(self.dict_of_assets[isin].name,)
                isin_df = duckdb.sql(f"""
                select * from df where isin = '{isin}'
                order by date""").df()
                quantity, total_dividends, track_quantity = self.get_asset_quantity(isin_df)
                isin_df['cumulative_quantity'] = track_quantity
                # ic(isin_df)
                summary = {'name':self.dict_of_assets[isin].name,
                           'isin': isin,
                           'asset':self.dict_of_assets[isin].asset,
                           'quantity': quantity,
                           'variation': self.dict_of_assets[isin].variation,
                           'currency':self.dict_of_assets[isin].currency,
                           'latest': self.dict_of_assets[isin].latest,
                           'total dividends': total_dividends,
                           'Perf ytd': compute_perf(self.dict_of_assets[isin].quotations['ytd']),
                           f'Perf {TODAY.year-1}': compute_perf(self.dict_of_assets[isin].quotations[f'{TODAY.year-1}']),
                           'Perf 1m': compute_perf(self.dict_of_assets[isin].quotations['1month']),
                           'Perf 6m': compute_perf(self.dict_of_assets[isin].quotations['6months']),
                           'Perf 1y': compute_perf(self.dict_of_assets[isin].quotations['1year']),
                           'Perf 3y': compute_perf(self.dict_of_assets[isin].quotations['3years']),
                           'Perf 5y': compute_perf(self.dict_of_assets[isin].quotations['5years']),
                           'TRI ytd':self.compute_xirr(isin_df,isin, period='ytd'),
                           f'TRI {TODAY.year-1}':self.compute_xirr(isin_df,isin,period=f'{TODAY.year-1}'),
                           'TRI since 1st buy': self.compute_xirr(isin_df, isin),
                           'operations':isin_df
                           }
                summary['valuation'] = summary['quantity']*summary['latest']
                
                assets.append(summary)
                
            self._assets_summary = pd.DataFrame(assets)
            self._assets_summary['proportion'] = round(
            self._assets_summary['valuation']/self._assets_summary['valuation'].sum(),
                2)
            
        return self._assets_summary
    
    def get_asset_quantity(self, df:pd.DataFrame, limit_day:date=TODAY):
        """Get the total number of parts of an asset and the total dividends earned"""
        quantity = 0
        total_dividends = 0
        tracking = []
        for op in df.itertuples(index=False, name='Row'):
            if op.date > str(limit_day):
                break
            elif op.operation == 'Buy':
                quantity += op.quantity
            elif op.operation == 'Sell':
                quantity-= op.quantity
            elif op.operation == 'Split':
                quantity = floor(op.value*quantity)
            elif op.operation == 'Dividend':
                total_dividends += quantity*op.value
            tracking.append(quantity)
        return quantity, total_dividends, tracking

    def compute_xirr(self, df:pd.DataFrame, isin:str, period='inception'):
        # add Initial value, last value
        quotations_df = self.dict_of_assets[isin].quotations[period]
        cashflows_df = duckdb.sql(f"""
        with first_last_quotations as (
        select * from (select
        date,
        quotations_df.c as value,
        row_number() over(order by date) as rn,
        count(*) over() as total_count
        from quotations_df
        order by date)
        full join df
        using (date, value)
        where rn = 1 or rn = total_count or rn is null
        order by date),
        
        lag_df as (select *,
        COALESCE(quantity, lag(quantity) over(order by date)) as quantity_,
        COALESCE(cumulative_quantity,
        lag(cumulative_quantity) over(order by date)) as cumulative_quantity_
        from first_last_quotations)

        select date, operation, quantity_ as quantity, value,
        (CASE
        WHEN operation = 'Buy' THEN -quantity_*value
        WHEN operation = 'Split' THEN 0
        WHEN operation IS NULL and rn=1 THEN -COALESCE(cumulative_quantity_, 0)*value
        WHEN operation IS NULL and rn!=1 THEN COALESCE(cumulative_quantity_,
        lag(cumulative_quantity_) over())*value
        ELSE quantity_*value
        END) as cashflow
        from lag_df
        {map_period_to_filter.get(period, '')}
  

        """).df()
        if period == 'ytd':
            cashflows_df.at[len(cashflows_df.index) - 1, 'date'] = date(year=TODAY.year, month=12, day=31)
        cashflows_df.to_csv(f'{isin}_{period}.csv')
        
        irr = xirr(cashflows_df['date'], cashflows_df['cashflow'])*100

        return f'{irr:.2f}%'

    @property
    def asset_values(self):
        """"""
        if self._asset_values is None:
            isins = self.operations_df['isin'].unique()
            all_quotations_df = []
            for isin in isins:
                isin_df = self.dict_of_assets[isin].quotations['inception'].copy()
                all_quotations_df.append(duckdb.sql(
                f""" select *, '{self.dict_of_assets[isin].name}' as name from isin_df
                """).df())
                
            all_quotations_df = pd.concat(all_quotations_df)
            # can't join on operation_df, because we need cumulative quantities.
            cum_quantities_df = pd.concat([df for df in self.assets_summary['operations']])
            all_quotations_df['date']=pd.to_datetime(all_quotations_df['date']).dt.date
            
            # Fill null value with last non null value for each asset
            self._asset_values = duckdb.sql(
                '''
                with grouped as (
                select date, name,
                cumulative_quantity,
                count(cumulative_quantity) over(partition by name order by date) as grouper
                from all_quotations_df aqdf
                left join cum_quantities_df cqdf
                using(date, name)
                where date >= (select min(date) from cum_quantities_df)
                order by date
                )
                select * from (select date, name,
                max(cumulative_quantity)
                over(partition by name, grouper
                order by date) as value
                from grouped
                order by date)
                '''
            ).df()
            ic(self._asset_values.tail(20))
        return self._asset_values
