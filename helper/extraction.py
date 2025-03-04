import json
from collections import OrderedDict
from absl import logging

from helper.communication import parallel_parse_communication_data, COMM_REQUIRED_TABLES, QUERY_COMMUNICATION, \
    QUERY_COMMUNICATION_STATS, create_specific_communication_stats
from helper.general import execute_query_in_thread, execute_queries_parallel, mutiple_table_exists, \
    DURATION_REQUIRED_TABLE, QUERY_TOTAL_DURATION
from helper.kernel import parallel_parse_kernel_data, KERNEL_REQUIRED_TABLES, QUERY_KERNEL, QUERY_KERNEL_STATS, \
    parallel_create_general_kernel_stats
from helper.transfer import parallel_parse_transfer_data, TRANSFER_REQUIRED_TABLES, QUERY_TRANSFERS, \
    QUERY_TRANSFERS_STATS, create_specific_transfer_stats

KERNEL_STATS = 0
TRANSFER_STATS = 1
COMMUNICATION_STATS = 2


def generate_queries(qurey, id_list):
    queries = []

    for name in id_list:
        queries.append((qurey, name))

    return queries


def create_statistics(database_file, first_query, raw_data_query, metric_type, sort_metric='Time Total'):
    ids = []
    statistics = {}
    name_stats = ''

    if metric_type is KERNEL_STATS:
        name_stats = 'Kernel'
    elif metric_type is TRANSFER_STATS:
        name_stats = 'Transfer'
    elif metric_type is COMMUNICATION_STATS:
        name_stats = 'Communication'

    logging.info(f"Getting General {name_stats} Information")
    res = execute_query_in_thread((first_query, None), database_file)

    if metric_type is KERNEL_STATS:
        for id, time_percent, time_total, instance, name in res[1]:
            ids.append(id)
            statistics[id] = {'Name': name, 'Time Percent': time_percent, 'Time Total': time_total,
                              'Instance': instance}
    elif metric_type is TRANSFER_STATS:
        for type, time_percent, time_total, mem_total, instance in res[1]:
            ids.append(type)
            statistics[type] = {'Type': type, 'Time Percent': time_percent, 'Time Total': time_total,
                                'Memory Total': mem_total,
                                'Instance': instance}
    elif metric_type is COMMUNICATION_STATS:
        for name, time_percent, time_total, instance in res[1]:
            ids.append(name)
            statistics[name] = {'Name': name, 'Time Percent': time_percent, 'Time Total': time_total,
                                'Instance': instance}
    else:
        logging.error('Unknown metric type')

    if metric_type is KERNEL_STATS:
        logging.info(
            f"Getting RAW Data for each specific {name_stats} (RAW kernel extraction will take a while for large sqlite files, ~1h)")
    else:
        logging.info(f"Getting RAW Data for each specific {name_stats}")

    queries = generate_queries(raw_data_query, ids)
    queries_res = execute_queries_parallel(queries, database_file)

    logging.info(f"Parsing RAW Data and generating Statistics for {name_stats}")
    if metric_type is KERNEL_STATS:
        results = parallel_parse_kernel_data(queries_res)
    elif metric_type is TRANSFER_STATS:
        results = parallel_parse_transfer_data(queries_res)
    elif metric_type is COMMUNICATION_STATS:
        results = parallel_parse_communication_data(queries_res)

    for id, dict in results:
        statistics[id].update(dict)

    statistics = OrderedDict(
        sorted(statistics.items(), key=lambda item: item[1][sort_metric], reverse=True))

    return statistics


def create_statistics_from_file(database_file, output_dir, FLAGS):
    full_statistics = {}

    logging.info(f"Starting extraction and creation of statistics from {database_file}")

    if not FLAGS.no_kernel_metrics:
        logging.info("Starting Kernel Statistics")
        if mutiple_table_exists(database_file, KERNEL_REQUIRED_TABLES):
            kernel_statistics = create_statistics(database_file, QUERY_KERNEL, QUERY_KERNEL_STATS,
                                                  metric_type=KERNEL_STATS)
            full_statistics['Kernel Statistics'] = {'Individual Kernels': kernel_statistics}
            full_statistics['Kernel Statistics'].update(parallel_create_general_kernel_stats(kernel_statistics))

    if not FLAGS.no_transfer_metrics:
        logging.info("Starting Transfer Statistics")
        if mutiple_table_exists(database_file, TRANSFER_REQUIRED_TABLES):
            transfer_statistics = create_statistics(database_file, QUERY_TRANSFERS, QUERY_TRANSFERS_STATS,
                                                    metric_type=TRANSFER_STATS)
            full_statistics['Transfer Statistics'] = {'Individual Transfers': transfer_statistics}
            full_statistics['Transfer Statistics'].update(create_specific_transfer_stats(transfer_statistics))

    if not FLAGS.no_communication_metrics:
        logging.info("Starting Communication Statistics")
        if mutiple_table_exists(database_file, COMM_REQUIRED_TABLES):
            comm_statistics = create_statistics(database_file, QUERY_COMMUNICATION, QUERY_COMMUNICATION_STATS,
                                                metric_type=COMMUNICATION_STATS)
            full_statistics['Communication Statistics'] = {'Individual Communications': comm_statistics}
            full_statistics['Communication Statistics'].update(create_specific_communication_stats(comm_statistics))

    if mutiple_table_exists(database_file, DURATION_REQUIRED_TABLE):
        full_statistics['Total Duration'] = execute_query_in_thread((QUERY_TOTAL_DURATION, None), database_file)[1][0][0]

    if not FLAGS.no_save_data and full_statistics:
        database_file_NAV = output_dir + database_file.split('.')[0] + '_parsed_stats.nav'
        logging.info(f"Saving Extracted Statistics of {database_file} to {database_file_NAV}")
        with open(database_file_NAV, 'w') as NAV_file:
            json.dump(full_statistics, NAV_file, indent=4)

    return full_statistics
