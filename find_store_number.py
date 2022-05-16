import logging
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

CSV_FILE_NAME = 'CPFM Store-Cost Center-Area'
SAVE_LOG_FILE = "logs/" + CSV_FILE_NAME + CSV_FILE_TYPE
COL1 = 'cost center'
COL2 = 'cpfm store number'
COL3 = 'cpfm cost center'
COL4 = 'cpfm area'

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s',
                    handlers=[logging.FileHandler(SAVE_LOG_FILE+'-' +
                                                  datetime.now(timezone.utc).strftime("%d-%m-%Y-%H-%M-%S") +
                                                  LOG_EXTENSION, mode='w'),
                              stream_handler])


def main():
    df = pd.read_csv(BASE_FOLDER + CSV_FILE_NAME +
                     CSV_FILE_TYPE, encoding=ENCODING_TYPE_UTF8)
    data_col1: list = df[COL1].values.tolist()
    data_col2: list = df[COL2].values.tolist()
    data_col3: list = df[COL3].values.tolist()
    data_col4: list = df[COL4].values.tolist()
    result_dict = dict()
    for item in data_col1:
        if item in data_col3:
            index_in_col2 = data_col3.index(item)
            current_key = data_col2[index_in_col2]
            current_value = data_col4[index_in_col2]
            result_dict[current_key] = current_value
            logging.info('Matched item{0} with {1}'.format(
                current_key, current_value))
        else:
            logging.info('Item: {0} not matched.'.format(item))
    pd.DataFrame(result_dict.items()).to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+CSV_FILE_TYPE, 
        encoding=ENCODING_TYPE_UTF8,
        header=[COL2,COL4],
        index=False
        )


if __name__ == '__main__':
    main()
