import csv


def extract_stocks_from_file(filename):
    with open(filename, 'r') as file:
        stocks = [line.strip() for line in file.readlines()]
    return stocks


def read_csv_to_dict(file_path):
    symbol_iv_dict = {}

    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            symbol_iv_dict[row['Symbol']] = row['IV']

    return symbol_iv_dict


def read_sizes_to_dict(file_path):
    symbol_iv_dict = {}

    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            symbol_iv_dict[row['Symbol']] = row['Size']

    return symbol_iv_dict


def create_symbol_dict(symbols):
    symbol_dict = {}
    for symbol in symbols:
        modified_symbol = symbol[:9].replace('&', '')
        symbol_dict[modified_symbol] = symbol
    return symbol_dict
