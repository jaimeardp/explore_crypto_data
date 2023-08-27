from utils.wrap_datetimes import gen_intervales_date_day, convert_dt_to_timestamp
import unittest

class TestWrapDatetimes(unittest.TestCase):
    
    def test_gen_intervales_date_day(self):
        start_dt = "2023-01-01"
        end_dt = "2023-01-31"
        interval = "5d"
        date_list = gen_intervales_date_day(start_dt, end_dt, interval)
      
        self.assertEqual(date_list[0], "2023-01-01")
        self.assertEqual(date_list[-1], "2023-01-31")
        self.assertEqual(len(date_list), 31)
      
    def test_convert_dt_to_timestamp(self):
        dt = "2023-01-01"
        timestamp = convert_dt_to_timestamp(dt)
        self.assertEqual(timestamp, 1673481600)


if __name__ == '__main__':
  unittest.main()