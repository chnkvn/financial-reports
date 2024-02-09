import os
import sys
import time
import unittest
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from financial_reports.src.data_extraction import Asset, get_current_asset_data

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
class TestAsset(unittest.TestCase):
    def test_stock(self):
        """Stock: air liquide"""
        for search in ['air liquide', 'AI', 'FR0000120073']:
            with self.subTest(i=search):
                AirLiquide = Asset.from_boursorama(get_current_asset_data(search))
                self.assertEqual(AirLiquide.asset, 'stock')
                self.assertEqual(AirLiquide.isin,'FR0000120073')
                self.assertEqual(AirLiquide.currency,'EUR')
                self.assertEqual(AirLiquide.name,'AIR LIQUIDE')
                self.assertEqual(AirLiquide.symbol, '1rPAI')
                self.assertEqual(AirLiquide.url, 'https://www.boursorama.com/cours/1rPAI/')
                self.assertGreaterEqual(AirLiquide.latest, 0)
                self.assertIsNone(AirLiquide.referenceIndex)
                self.assertIsNone(AirLiquide.morningstarCategory)
            time.sleep(1)

    def test_tracker(self):
        """Tracker: cw8"""
        for search in ['LU1681043599', 'CW8']:
            with self.subTest(i=search):
                cw8 = Asset.from_boursorama(get_current_asset_data(search))
                self.assertEqual(cw8.asset, 'trackers')
                self.assertEqual(cw8.isin,'LU1681043599')
                self.assertEqual(cw8.currency,'EUR')
                self.assertEqual(cw8.name,'AMUNDI MSCI WORLD UCITS ETF - EUR')
                self.assertEqual(cw8.symbol, '1rTCW8')
                self.assertEqual(cw8.url, 'https://www.boursorama.com/bourse/trackers/cours/1rTCW8/')
                self.assertGreaterEqual(cw8.latest, 0)
                self.assertEqual(cw8.referenceIndex, 'MSCI World')
                self.assertEqual(cw8.morningstarCategory, 'Actions International Gdes Cap. Mixte')
            time.sleep(1)

    def test_opcvm(self):
        """OPCVM: Réserve Ecureuil C"""
        for search in ['FR0010177378']:
            with self.subTest(i=search):
                ecureuil = Asset.from_boursorama(get_current_asset_data(search))
                self.assertEqual(ecureuil.asset, 'opcvm')
                self.assertEqual(ecureuil.isin, 'FR0010177378')
                self.assertEqual(ecureuil.currency, 'EUR')
                self.assertEqual(ecureuil.name, 'Réserve Ecureuil C')
                self.assertEqual(ecureuil.symbol, 'MP-184677')
                self.assertEqual(ecureuil.url,'https://www.boursorama.com/bourse/opcvm/cours/MP-184677/' )
                self.assertGreaterEqual(ecureuil.latest, 0)
                self.assertEqual(ecureuil.morningstarCategory, 'Swap EONIA PEA')
                self.assertIsNone(ecureuil.referenceIndex)
            time.sleep(1)

        

if __name__ == '__main__':
    unittest.main()
