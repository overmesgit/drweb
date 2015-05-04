import unittest
from database import DataBase


class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.db = DataBase()

    def test_full(self):
        result = self.db.execute('GET A')
        self.assertEqual('NULL', result)

        self.db.execute('SET A 10')
        result = self.db.execute('GET A')
        self.assertEqual('10', result)

        result = self.db.execute('COUNTS 10')
        self.assertEqual(1, result)

        self.db.execute('SET B 20')
        self.db.execute('SET C 10')

        result = self.db.execute('COUNTS 10')
        self.assertEqual(2, result)

        self.db.execute('UNSET B')

        result = self.db.execute('GET B')
        self.assertEqual('NULL', result)

        self.db.execute('END')

    def test_wrong_command(self):
        with self.assertRaises(ValueError) as ex:
            self.db.execute('asdf')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('GET')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('GET 10 10')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('SET 10')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('SET 10 10 10')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('COUNTS')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('COUNTS 10 10')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('UNSET')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('UNSET 10 10')

        with self.assertRaises(ValueError) as ex:
            self.db.execute('END 10 10')

    def test_transactions(self):
        self.db.execute('SET A 10')

        self.db.execute('BEGIN')

        self.db.execute('SET B 20')
        result = self.db.execute('GET A')
        self.assertEqual('10', result)

        self.db.execute('COMMIT')

        result = self.db.execute('GET B')
        self.assertEqual('20', result)

    def test_transactions_full(self):
        self.db.execute('BEGIN')

        self.db.execute('SET A 10')

        self.db.execute('BEGIN')

        self.db.execute('SET A 20')

        self.db.execute('BEGIN')

        self.db.execute('SET A 30')

        result = self.db.execute('GET A')
        self.assertEqual('30', result)

        self.db.execute('ROLLBACK')

        result = self.db.execute('GET A')
        self.assertEqual('20', result)

        self.db.execute('COMMIT')

        result = self.db.execute('GET A')
        self.assertEqual('20', result)

    def test_transactions_unset_op(self):
        self.db.execute('SET A 10')
        self.db.execute('BEGIN')
        self.db.execute('UNSET A')

        result = self.db.execute('GET A')
        self.assertEqual('NULL', result)
        self.db.execute('ROLLBACK')

        result = self.db.execute('GET A')
        self.assertEqual('10', result)

        self.db.execute('BEGIN')
        self.db.execute('UNSET A')
        self.db.execute('COMMIT')

        result = self.db.execute('GET A')
        self.assertEqual('NULL', result)

        self.db.execute('BEGIN')
        self.db.execute('UNSET A')
        self.db.execute('COMMIT')

    def test_transactions_count_op(self):
        self.db.execute('BEGIN')

        self.db.execute('SET A 10')
        result = self.db.execute('COUNTS 10')
        self.assertEqual(1, result)

        self.db.execute('BEGIN')

        self.db.execute('SET B 10')
        result = self.db.execute('COUNTS 10')
        self.assertEqual(2, result)

        self.db.execute('ROLLBACK')

        result = self.db.execute('COUNTS 10')
        self.assertEqual(1, result)

        self.db.execute('ROLLBACK')

        result = self.db.execute('COUNTS 10')
        self.assertEqual(0, result)

if __name__ == '__main__':
    unittest.main()