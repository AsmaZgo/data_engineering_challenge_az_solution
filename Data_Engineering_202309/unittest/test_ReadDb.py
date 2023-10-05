from Data_Engineering_202309.ReadDb import ReadDb
import unittest

class ReadDbTest(unittest.TestCase):
    def test_query_data_from_database(self):
        db= ReadDb()
        data = db.query_data_from_database('/Users/zgolli/PycharmProjects/data_engineering_challenge_az_solution/Data_Engineering_202309/challenge.db')
        print(data.head())
        assert True

    def test_query_add_data_from_database(self):
        db = ReadDb()
        session_sources, conversions, session_costs = db.query_all_data_from_database('/Users/zgolli/PycharmProjects/data_engineering_challenge_az_solution/Data_Engineering_202309/challenge.db')
        print(session_sources.head())
        assert True
if __name__ == '__main__':
    unittest.main()