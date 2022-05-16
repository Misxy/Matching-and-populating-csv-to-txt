import os
from numpy import NaN
import pandas as pd
from constant import *
import logging
from datetime import datetime

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE_NAME+'-' +
                                                  datetime.now().strftime("%d-%m-%Y-%H-%M-%S") +
                                                  LOG_EXTENSION, mode='w'),
                              stream_handler])


class Util:
    def __init__(self, file_name: str, save_file_path: str) -> None:
        if isinstance(file_name, str) and isinstance(save_file_path, str):
            self.file_name = file_name
            self.save_file_path = save_file_path
        else:
            logging.error("The two arguments must be str type only.")
            raise TypeError("The two arguments must be str type only.")

    def read_data(self):
        self.df = pd.read_csv(self.file_name, encoding=ENCODING_TYPE_ISO)
        logging.info("Reading the file: {0} data...".format(self.file_name))
        self.large_stores: list = self.delete_full_stop(
            self.df[COL_2_NAME].values.tolist())
        self.small_stores: list = self.delete_full_stop(
            self.df[COL_4_NAME].values.tolist())
        self.base_columns = self.df[COL_1_NAME].values.tolist()[
            :TOTAL_STORES+1]

    def process_raw_data(self):
        self.merged_values: list = self.df[COL_3_NAME].values.tolist(
        ) + self.df[COL_5_NAME].values.tolist()
        logging.info("Created a mapping information with the following pattern:\n(Key\tValue):\n{0}\t{1}\n{2}\t{3}".format(
            COL_2_NAME, COL_3_NAME, COL_4_NAME, COL_5_NAME))

    def delete_full_stop(self, ls: list):
        ls = list(map(str, ls))
        return [ls[idx][:ls[idx].index(FULL_STOP)]
                for idx in range(len(ls))
                if FULL_STOP in str(ls[idx])]

    def adjust_digit(self, current_digit: int):
        logging.info(
            "Found the new store number format: {0}".format(current_digit))
        logging.info(
            "Converting to the following digits: {0}".format(OLD_STORE_NUMBER_DIGITS))
        self.base_columns = list(
            map(lambda x: x[current_digit - OLD_STORE_NUMBER_DIGITS:], self.base_columns))
        logging.info(
            "Successfully converted the new store number format from {0} digits to {1} digits."
            .format(current_digit, OLD_STORE_NUMBER_DIGITS))

    def retrieve_results(self):
        self.results = list()
        self.store_names = list()
        current_digit = len(self.base_columns[0])
        if current_digit > OLD_STORE_NUMBER_DIGITS:
            self.adjust_digit(current_digit)
        for idx in range(len(self.base_columns)):
            current_idx = idx+1
            if self.base_columns[idx] in self.large_stores:
                logging.info("File index: {0} Store number: {1} is paired with the value: {2}".format(
                    current_idx, self.base_columns[idx], self.merged_values[self.large_stores.index(self.base_columns[idx])]))
                self.results.append(
                    self.merged_values[self.large_stores.index(self.base_columns[idx])])
            elif self.base_columns[idx] in self.small_stores:
                logging.info("File index: {0} Store number: {1} is paired with the value: {2}".format(
                    current_idx, self.base_columns[idx], self.merged_values[self.small_stores.index(self.base_columns[idx])]))
                self.results.append(
                    self.merged_values[self.small_stores.index(self.base_columns[idx])])
            else:
                logging.info(
                    "File index: {0} Store number {1} is skipped".format(
                        current_idx, self.base_columns[idx]))
                self.results.append(NOT_FILLED)
            self.store_names.append(str(self.base_columns[idx]))
        logging.info(
            "Retrieved the results by comparing the mapping information with the column: {0}".format(COL_1_NAME))

    def create_result_csv(self):
        with open(SAVE_FILE_PATH, 'w') as f:
            for idx in range(len(self.results)):
                f.write(str(self.results[idx]))
                f.write('\n')
        logging.info(
            "Creating the result CSV file: {0}....".format(SAVE_FILE_PATH))

    def validate_the_result_file(self):
        if os.path.exists(SAVE_FILE_PATH):
            logging.info(
                "The result CSV file: {0} is successfully created.".format(SAVE_FILE_PATH))
        else:
            logging.info(
                "The result CSV file: {0} is unsucessfully created.".format(SAVE_FILE_PATH))

    def execute(self):
        self.read_data()
        self.process_raw_data()
        self.retrieve_results()
        self.create_result_csv()
        self.validate_the_result_file()
