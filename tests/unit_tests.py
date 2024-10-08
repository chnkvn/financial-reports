import os
import sys
import time
import unittest

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.data_extraction import Asset, get_current_asset_data
from src.portfolio import Portfolio


# Scrapping
class TestAsset(unittest.TestCase):
    """Ensure the scraped data is correct"""

    def test_stock(self):
        """Stock: air liquide"""
        for search in ["air liquide", "AI", "FR0000120073"]:
            with self.subTest(i=search):
                AirLiquide = Asset.from_boursorama(get_current_asset_data(search))
                self.assertEqual(AirLiquide.asset, "stock")
                self.assertEqual(AirLiquide.isin, "FR0000120073")
                self.assertEqual(AirLiquide.currency, "EUR")
                self.assertEqual(AirLiquide.name, "AIR LIQUIDE")
                self.assertEqual(AirLiquide.symbol, "1rPAI")
                self.assertEqual(
                    AirLiquide.url, "https://www.boursorama.com/cours/1rPAI/"
                )
                self.assertGreaterEqual(AirLiquide.latest, 0)
                self.assertIsNone(AirLiquide.referenceIndex)
                self.assertIsNone(AirLiquide.morningstarCategory)
            time.sleep(1)

    def test_tracker(self):
        """Tracker: cw8"""
        for search in ["LU1681043599", "CW8"]:
            with self.subTest(i=search):
                cw8 = Asset.from_boursorama(get_current_asset_data(search))
                self.assertEqual(cw8.asset, "trackers")
                self.assertEqual(cw8.isin, "LU1681043599")
                self.assertEqual(cw8.currency, "EUR")
                self.assertEqual(cw8.name, "AMUNDI MSCI WORLD UCITS ETF - EUR")
                self.assertEqual(cw8.symbol, "1rTCW8")
                self.assertEqual(
                    cw8.url, "https://www.boursorama.com/bourse/trackers/cours/1rTCW8/"
                )
                self.assertGreaterEqual(cw8.latest, 0)
                self.assertEqual(cw8.referenceIndex, "MSCI World")
                self.assertEqual(
                    cw8.morningstarCategory, "Actions International Gdes Cap. Mixte"
                )
            time.sleep(1)

    def test_opcvm(self):
        """OPCVM: Réserve Ecureuil C"""
        for search in ["FR0010177378"]:
            with self.subTest(i=search):
                ecureuil = Asset.from_boursorama(get_current_asset_data(search))
                self.assertEqual(ecureuil.asset, "opcvm")
                self.assertEqual(ecureuil.isin, "FR0010177378")
                self.assertEqual(ecureuil.currency, "EUR")
                self.assertEqual(ecureuil.name, "Réserve Ecureuil C")
                self.assertEqual(ecureuil.symbol, "MP-184677")
                self.assertEqual(
                    ecureuil.url,
                    "https://www.boursorama.com/bourse/opcvm/cours/MP-184677/",
                )
                self.assertGreaterEqual(ecureuil.latest, 0)
                self.assertEqual(ecureuil.morningstarCategory, "Swap EONIA PEA")
                self.assertIsNone(ecureuil.referenceIndex)
            time.sleep(1)


class TestPortfolio(unittest.TestCase):
    """Test code with empty and non-empty portfolio"""

    def test_non_empty_portfolio(self):
        """Test code with non-empty portfolio"""
        non_empty_ptf = Portfolio("unit_tests_ptf")

        self.assertEqual(len(non_empty_ptf.operations_df), 16)
        self.assertEqual(len(non_empty_ptf.dict_of_assets), 3)

        total_dividends = pd.DataFrame(
            {
                "isin": ["FR0011869353", "FR0000120073", "FR0010177378"],
                "asset": ["trackers", "stock", "opcvm"],
                "quantity": [200.0, 65.0, 67.0],
            },
            index=range(3),
        )
        self.assertEqual(
            non_empty_ptf.assets_summary.loc[:, ["isin", "asset", "quantity"]].equals(
                total_dividends
            ),
            True,
        )
        self.assertEqual(non_empty_ptf.portfolio_summary.at[0, "Lines number"], 3)
        self.assertEqual(
            round(non_empty_ptf.portfolio_summary.at[0, "Total invested amount"], 2),
            24578.51,
        )
        # Unit test: IRR, asset: Air liquide
        cashflows_df = pd.read_csv("tests/tests_cashflows.csv")
        cashflows_df = cashflows_df.iloc[:-1, :].round(2)
        cum_quantities_df = pd.concat(
            df for df in non_empty_ptf.assets_summary["operations"]
        )
        test_cashflows_df = non_empty_ptf.get_cashflow_df(
            cum_quantities_df, non_empty_ptf.asset_values, "inception"
        ).iloc[:-1, :]
        self.assertEqual(
            cashflows_df.equals(test_cashflows_df.round(2)),
            True,
        )

    def test_empty_ptf(self):
        """Test code with empty, non-existant portfolio"""
        empty_ptf = Portfolio("empty_unit_tests_ptf")
        self.assertFalse(len(empty_ptf.operations_df) > 0)
        self.assertFalse(len(empty_ptf.dict_of_assets) > 0)
        self.assertIsNone(empty_ptf.assets_summary)
        self.assertIsNone(empty_ptf.portfolio_summary)
        self.assertIsNone(empty_ptf.asset_values)


if __name__ == "__main__":
    unittest.main()
