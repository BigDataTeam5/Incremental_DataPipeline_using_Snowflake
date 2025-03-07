import os
import sys
import unittest

# Add the parent directory to path so we can import the function module
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                           "udfs_and_spoc", "weekly_co2_changes", "weekly_changes"))

# Import the function directly
from function import co2_weekly_percent_change

class TestWeeklyCO2PercentChange(unittest.TestCase):
    
    def test_normal_positive_change(self):
        """Test with a normal case where CO2 increases"""
        self.assertAlmostEqual(co2_weekly_percent_change(410.5, 412.3), 0.43848964677223173, places=4)
        
    def test_normal_negative_change(self):
        """Test with a normal case where CO2 decreases"""
        self.assertAlmostEqual(co2_weekly_percent_change(412.3, 410.5), -0.4365753092408468, places=4)
    
    def test_no_change(self):
        """Test with no change between weeks"""
        self.assertEqual(co2_weekly_percent_change(410.5, 410.5), 0.0)
    
    def test_none_values(self):
        """Test with None values"""
        self.assertEqual(co2_weekly_percent_change(None, 410.5), 0.0)
        self.assertEqual(co2_weekly_percent_change(410.5, None), 0.0)
        self.assertEqual(co2_weekly_percent_change(None, None), 0.0)
    
    def test_zero_previous(self):
        """Test with zero as previous value"""
        self.assertEqual(co2_weekly_percent_change(0, 410.5), 0.0)
    
    def test_invalid_inputs(self):
        """Test with invalid inputs"""
        self.assertEqual(co2_weekly_percent_change("invalid", 410.5), 0.0)
        self.assertEqual(co2_weekly_percent_change(410.5, "invalid"), 0.0)
        
    def test_very_large_change(self):
        """Test with a very large change"""
        self.assertAlmostEqual(co2_weekly_percent_change(1.0, 101.0), 10000.0)
        
if __name__ == '__main__':
    unittest.main()
