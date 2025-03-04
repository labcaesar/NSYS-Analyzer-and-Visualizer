import json
import sqlite3
from bisect import bisect_left, bisect_right
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from absl import logging, app

MAX_WORKERS = 12

QUERY_TOTAL_DURATION = """
SELECT duration AS total_duration
FROM ANALYSIS_DETAILS;
"""

DURATION_REQUIRED_TABLE = ['ANALYSIS_DETAILS']

def file_args_checking(args):
    extract_data = False
    output_data = True
    file_labels = None

    if args.data_file != None and args.nav_file == None:
        extract_data = True

    if extract_data:
        num_files = args.data_file.count(".sqlite")
    else:
        num_files = args.nav_file.count(".nav")

    if num_files > 1:

        if extract_data:
            files = [f.strip () + ".sqlite" for f in args.data_file.split ( ".sqlite" )]
        else:
            files = [f.strip () + ".nav" for f in args.nav_file.split ( ".nav" )]

        files = files[0:num_files]

        if not args.multi_data_label:
            raise app.UsageError("Must provide labels for multiple files extraction")

        file_labels = args.multi_data_label.split(',')

        if len(file_labels) != num_files:
            raise app.UsageError("Must provide labels for each provided files")
    else:
        files = args.data_file if extract_data else args.nav_file

    if args.no_metrics_output:
        output_data = not args.no_metrics_output

    return files, num_files, file_labels, output_data, extract_data


def import_from_NAV(file):
    with open(file, 'r') as nav_file:
        dict = json.load(nav_file, parse_float=float)

    return dict


def table_exists(database_file, table_name):
    try:
        with sqlite3.connect(database_file) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            result = cursor.fetchone()
            if result:
                return True
            else:
                logging.error(f"Statistics were requested but required table {table_name} does not exist")
                return False
    except sqlite3.Error as e:
        logging.error(f"Statistics were requested but required table {table_name} does not exist")
        return False


def mutiple_table_exists(database_file, table_name_list):
    for table_name in table_name_list:
        if not table_exists(database_file, table_name):
            return False
    return True


def execute_query(conn, query, params=None):
    cursor = conn.cursor()
    if params is not None or params == 0:
        key = params
        if not isinstance(params, tuple):
            params = (params,)
        cursor.execute(query, params)
    else:
        key = None
        cursor.execute(query)
    result = cursor.fetchall()
    return key, result


def execute_query_in_thread(query_params, database_file):
    conn = sqlite3.connect(database_file)  # Create a new connection object in each thread
    try:
        conn.execute("PRAGMA cache_size=-64000;")  # Increase cache size (~64MB)
        conn.execute("PRAGMA temp_store=MEMORY;")
        result = execute_query ( conn, *query_params )
    except sqlite3.Error as error:
        print("Error reading data from SQLite table:", error)
    finally:
        conn.close()
    return result


def execute_queries_parallel(queries_with_params, database_file):
    results = []
    total_queries = len(queries_with_params)
    completed_queries = 0
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for query_params in queries_with_params:
            future = executor.submit(execute_query_in_thread, query_params, database_file)
            futures.append(future)
        for future in as_completed(futures):
            results.append(future.result())
            completed_queries += 1
            # Check if 10% of total items are completed
            if int((completed_queries / total_queries) * 100) % 10 == 0:
                logging.info(f"Progress: {(completed_queries / total_queries) * 100:.1f}%")
    return results


def remove_outliers(data):
    # Calculate the first and third quartiles
    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)

    # Calculate the interquartile range (IQR)
    IQR = Q3 - Q1

    # Define the lower and upper bounds for outliers
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Remove outliers
    clean_data = [x for x in data if np.all((x >= lower_bound) & (x <= upper_bound))]

    return clean_data


def generate_statistics(data, label, disable_raw=False):
    kernel_data = {}
    data = [float(x) for x in data]

    # Compute statistics
    mean_duration = np.mean(data)
    median_duration = np.median(data)
    min_duration = np.min(data)
    max_duration = np.max(data)
    std_deviation = np.std(data)

    # Round statistical results to 6 decimal places
    rounded_log_data = np.round(data, 6).tolist()
    rounded_mean_duration = round(mean_duration, 6)
    rounded_median_duration = round(median_duration, 6)
    rounded_min_duration = round(min_duration, 6)
    rounded_max_duration = round(max_duration, 6)
    rounded_std_deviation = round(std_deviation, 6)

    # Prepare kernel data dictionary
    if disable_raw:
        kernel_data[label] = {
            'Mean': rounded_mean_duration,
            'Median': rounded_median_duration,
            'Minimum': rounded_min_duration,
            'Maximum': rounded_max_duration,
            'Standard Deviation': rounded_std_deviation
        }
    else:
        kernel_data[label] = {
            'Raw Data': rounded_log_data,
            'Mean': rounded_mean_duration,
            'Median': rounded_median_duration,
            'Minimum': rounded_min_duration,
            'Maximum': rounded_max_duration,
            'Standard Deviation': rounded_std_deviation
        }

    return kernel_data


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(np.floor(np.log10(size_bytes) / 3))
    p = np.power(10, i * 3)
    s = round(size_bytes / p, 2)
    return f"{s}{size_name[i]}"

def convert_duration(duration):
    if duration == 0:
        return "0"
    size_name = ("", "K", "M", "G", "T", "P", "E", "Z", "Y")
    i = int(np.floor(np.log10(duration) / 3))
    p = np.power(10, i * 3)
    s = round(duration / p, 2)
    return f"{s}{size_name[i]}"

def expand_bins(data, bin_edges):
    expanded_bin_edges = []

    for i in range(len(bin_edges) - 1):
        left_index = bisect_left(data, bin_edges[i])
        right_index = bisect_right(data, bin_edges[i + 1])
        data_subset = data[left_index:right_index]

        if len(data_subset) != 0:
            idx = int((right_index + left_index) / 2 - 1)
            base = data[idx]
            idx += 1
            while (base == data[idx]):
                idx += 1

            if idx is not right_index:
                if i == (len(bin_edges) - 2):
                    expanded_bin_edges.extend([bin_edges[i], float(data[idx]), float(data[-1])])
                else:
                    expanded_bin_edges.extend([bin_edges[i], float(data[idx]), bin_edges[i + 1]])
            else:
                expanded_bin_edges.extend([bin_edges[i], bin_edges[i + 1]])
        else:
            mid_point = (bin_edges[i] + bin_edges[i + 1]) / 2
            expanded_bin_edges.extend([bin_edges[i], mid_point, bin_edges[i + 1]])

    bin_edges = np.array(np.unique(expanded_bin_edges))

    return bin_edges


def create_histogram(data, bins=10, powers_2=False, base=False, convert_bytes=False, return_bins=False):
    if len(data) > 1:
        data.sort()
        if base:
            bin_edges = np.histogram_bin_edges(data, bins=bins)
            if powers_2:
                bin_edges = 2 ** np.round(np.log2(bin_edges))
                bin_edges = np.unique(bin_edges)
        else:
            quantiles = np.linspace(0, 1, bins + 1)
            bin_edges = np.quantile(data, quantiles)
            if powers_2:
                bin_edges = 2 ** np.round(np.log2(bin_edges))
                bin_edges = np.unique(bin_edges)
                if 1 < len(bin_edges) < bins / 2 < len(set(data)):
                    bin_edges = expand_bins(data, bin_edges)
            else:
                bin_edges = np.unique(bin_edges)

        if len(bin_edges) > 1:
            hist, _ = np.histogram(data, bins=bin_edges)
            bin_centers = (bin_edges[1:] + bin_edges[:-1]) / 2

            if convert_bytes:
                bin_labels = [f'{convert_size(left)}-{convert_size(right)}' for left, right in
                              zip(bin_edges[:-1], bin_edges[1:])]
            else:
                bin_labels = [f'{convert_duration(left)}-{convert_duration(right)}' for left, right in zip(bin_edges[:-1], bin_edges[1:])]

            if not isinstance(bin_centers, list):
                bin_centers = bin_centers.tolist()
            hist = hist.tolist()
            bin_width = np.diff(bin_edges).tolist()
        else:
            bin_centers = [data[0]]
            hist = [len(data)]
            bin_width = [0]
            if convert_bytes:
                bin_labels = [f'{convert_size(data[0])}']
            else:
                bin_labels = [f'{convert_duration(data[0])}']

        for sublist in hist:
            if not sublist:
                sublist = []
                sublist.append(0)

        histogram_data = {
            "Bin Centers": bin_centers,
            "Histogram": hist,
            "Bin Width": bin_width,
            "Bin Labels": bin_labels
        }

        if return_bins:

            return_histogram_data = {
                "Bin Centers": bin_centers,
                "Histogram": None,
                "Bin Width": bin_width,
                "Bin Labels": bin_labels
            }

            bins = [(start, end) for start, end in zip(bin_edges[:-1], bin_edges[1:])]
            return histogram_data, (bins, return_histogram_data)
        else:
            return histogram_data
    else:
        if return_bins:
            return None, None
        else:
            return None
