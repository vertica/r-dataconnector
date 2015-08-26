import logging
import os
import pdb
import sys

#def num_to_digits(num):
#  digits = 1
#  while True:
#    num /= 10
#    if num == 0: break
#    digits += 1
#  return digits



class CsvFileCreator:
    def __init__(self, size, num_cols, cell_size, filename):
        '''size: total size of data to be created,
           num_cols:
           cell_size: size of each number e.g. 0 -> 0000, 1 -> 0001
           filename: output filename'''
        self.size = size
        self.num_cols = num_cols
        self.cell_size = cell_size
        self.filename = filename


    def create(self, buf_size = 1 * 1024 * 1024):
        assert (self.size % buf_size == 0) or (buf_size > self.size), 'Buffer needs to be multiple of total size'

        with open(self.filename,'w') as f:
            buf = ''
            counter = 0
            total_bytes_written = 0
            column = 0
            while total_bytes_written < self.size:
                number = str(counter).zfill(self.cell_size) + ','
                counter += 1
                column += 1
                if column == self.num_cols:
                    number = number[:-1] + '\n' # end last column with a newline instead of a comma
                    column = 0

                if(len(buf) + (self.cell_size + 1)) > buf_size:
                    # flush the buffer when full
                    logging.debug('flushing %s' %(buf))
                    logging.info('Written %d bytes ...', total_bytes_written)
                    f.write(buf)
                    buf = ''

                buf += number
                total_bytes_written += len(number)

            if len(buf) > 0:
                f.write(buf)
                logging.debug('2 flushing %s' %(buf))
                logging.info('2 Written %d bytes ...', total_bytes_written)

            #assert total_bytes_written == self.size, "%d vs %d" %(total_bytes_written,self.size)

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print 'Usage: %s <size in MB> <cols> <cell_size> <out filename> <internal buffer size>' %(sys.argv[0])
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    size = int(sys.argv[1]) * 1024 * 1024
    num_cols = int(sys.argv[2])
    cell_size = int(sys.argv[3])
    filename = sys.argv[4]
    if not os.path.exists(os.path.dirname(filename)):
        print 'need to run script from Debug/Release dirs'
        sys.exit(1)
    buffer_size = int(sys.argv[5]) * 1024 * 1024 # 1MB
    CsvFileCreator(size, num_cols, cell_size, filename).create(buffer_size)

