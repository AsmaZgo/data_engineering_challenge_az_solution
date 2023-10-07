from Data_Engineering_202309.data.access.ReadDb import ReadDb
import unittest

class ReadDbTest(unittest.TestCase):
    def test_getSessions(self):
        db= ReadDb('/Users/zgolli/PycharmProjects/challenge.db')
        data = db.getSessions()
        print(data.head().T)
        db.close_connection()
        assert True

    def test_getSessions_between_two_dates(self):
        db= ReadDb('/Users/zgolli/PycharmProjects/challenge.db')
        data = db.getFilteredSessions("2023-08-29","2023-09-06")
        print(data.head().T)
        db.close_connection()
        assert True

    def test_query_add_data_from_database(self):
        db = ReadDb('/Users/zgolli/PycharmProjects/challenge.db')
        session_sources, conversions, session_costs = db.query_all_data_from_database()
        print(session_sources.head())
        db.close_connection()
        assert True

if __name__ == '__main__':
    unittest.main()