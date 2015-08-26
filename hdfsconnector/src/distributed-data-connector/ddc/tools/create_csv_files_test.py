from create_csv_files import *

import unittest
class TestCreateCsvFiles(unittest.TestCase):

    def test_0(self):
        '''when total size is multiple of internal buffer'''
        TOTAL_SIZE = 16
        NUM_COLS = 2
        CELL_SIZE = 3
        INTERNAL_BUFFER_SIZE = 8
        FILENAME = 'out.csv'
        creator = CsvFileCreator(TOTAL_SIZE,NUM_COLS,CELL_SIZE, FILENAME)
        csv = creator.create(INTERNAL_BUFFER_SIZE)
        self.assertEqual(open(FILENAME).read(), '000,001\n002,003\n')
        self.assertEqual(len(open(FILENAME).read()), TOTAL_SIZE)

    def test_1(self):
        '''when total size is equal to the internal buffer size'''
        TOTAL_SIZE = 16
        NUM_COLS = 2
        CELL_SIZE = 3
        INTERNAL_BUFFER_SIZE = 16
        FILENAME = 'out3.csv'
        creator = CsvFileCreator(TOTAL_SIZE,NUM_COLS,CELL_SIZE, FILENAME)
        csv = creator.create(INTERNAL_BUFFER_SIZE)
        self.assertEqual(open(FILENAME).read(), '000,001\n002,003\n')
        self.assertEqual(len(open(FILENAME).read()), TOTAL_SIZE)

    def test_2(self):
        '''when total_size is *not* multiple of internal buffer'''
        TOTAL_SIZE = 16
        NUM_COLS = 2
        CELL_SIZE = 3
        INTERNAL_BUFFER_SIZE = 12
        FILENAME = 'out3.csv'
        creator = CsvFileCreator(TOTAL_SIZE,NUM_COLS,CELL_SIZE, FILENAME)
        self.assertRaises(AssertionError, creator.create,INTERNAL_BUFFER_SIZE)

    def test_3(self):
        '''when buffer is bigger than total size'''
        TOTAL_SIZE = 16
        NUM_COLS = 2
        CELL_SIZE = 3
        INTERNAL_BUFFER_SIZE = 32
        FILENAME = 'out4.csv'
        creator = CsvFileCreator(TOTAL_SIZE,NUM_COLS,CELL_SIZE, FILENAME)
        csv = creator.create(INTERNAL_BUFFER_SIZE)
        self.assertEqual(open(FILENAME).read(), '000,001\n002,003\n')
        self.assertEqual(len(open(FILENAME).read()), TOTAL_SIZE)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
