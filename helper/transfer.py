from concurrent.futures import ProcessPoolExecutor, as_completed

from absl import logging

from helper.general import generate_statistics, MAX_WORKERS, create_histogram, remove_outliers

QUERY_TRANSFERS = """
WITH
    memops AS (
        SELECT
            CASE
                WHEN mcpy.copyKind = 0 THEN 'Unknown'
                WHEN mcpy.copyKind = 1 THEN 'Host-to-Device'
                WHEN mcpy.copyKind = 2 THEN 'Device-to-Host'
                WHEN mcpy.copyKind = 3 THEN 'Host-to-Array'
                WHEN mcpy.copyKind = 4 THEN 'Array-to-Host'
                WHEN mcpy.copyKind = 5 THEN 'Array-to-Array'
                WHEN mcpy.copyKind = 6 THEN 'Array-to-Device'
                WHEN mcpy.copyKind = 7 THEN 'Device-to-Array'
                WHEN mcpy.copyKind = 8 THEN 'Device-to-Device'
                WHEN mcpy.copyKind = 9 THEN 'Host-to-Host'
                WHEN mcpy.copyKind = 10 THEN 'Peer-to-Peer'
                WHEN mcpy.copyKind = 11 THEN 'Unified Host-to-Device'
                WHEN mcpy.copyKind = 12 THEN 'Unified Device-to-Host'
                WHEN mcpy.copyKind = 13 THEN 'Unified Device-to-Device'
                ELSE 'Unknown'
            END AS name,
            mcpy.end - mcpy.start AS duration,
            mcpy.bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY as mcpy
        UNION ALL
        SELECT
            'Memset' AS name,
            end - start AS duration,
            bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMSET
    ),
    summary AS (
        SELECT
            name AS name,
            sum(duration) AS time_total,
            sum(size) AS mem_total,
            count(*) AS num
        FROM
            memops
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(time_total) AS time_total FROM summary
    )
SELECT
    summary.name AS "Operation",
    round(summary.time_total * 100.0 / (SELECT time_total FROM totals), 1) AS "Time:ratio_%",
    summary.time_total AS "Total Time:dur_ns",
    summary.mem_total AS "Total:mem_B",
    summary.num AS "Count"
FROM
    summary
ORDER BY 2 DESC
"""

QUERY_TRANSFERS_STATS = """
WITH
    transfers AS (
        SELECT
            CASE
                WHEN mcpy.copyKind = 0 THEN 'Unknown'
                WHEN mcpy.copyKind = 1 THEN 'Host-to-Device'
                WHEN mcpy.copyKind = 2 THEN 'Device-to-Host'
                WHEN mcpy.copyKind = 3 THEN 'Host-to-Array'
                WHEN mcpy.copyKind = 4 THEN 'Array-to-Host'
                WHEN mcpy.copyKind = 5 THEN 'Array-to-Array'
                WHEN mcpy.copyKind = 6 THEN 'Array-to-Device'
                WHEN mcpy.copyKind = 7 THEN 'Device-to-Array'
                WHEN mcpy.copyKind = 8 THEN 'Device-to-Device'
                WHEN mcpy.copyKind = 9 THEN 'Host-to-Host'
                WHEN mcpy.copyKind = 10 THEN 'Peer-to-Peer'
                WHEN mcpy.copyKind = 11 THEN 'Unified Host-to-Device'
                WHEN mcpy.copyKind = 12 THEN 'Unified Device-to-Host'
                WHEN mcpy.copyKind = 13 THEN 'Unified Device-to-Device'
                ELSE 'Unknown'
            END AS name,
            mcpy.end - mcpy.start AS duration,
            mcpy.bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY as mcpy
        UNION ALL
        SELECT
            'Memset' AS name,
            end - start AS duration,
            bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMSET
    )
SELECT
    name AS "Name",
    duration AS "Duration",
    size AS "Size"
FROM
    transfers
WHERE
    name = ?
"""

TRANSFER_REQUIRED_TABLES = ['CUPTI_ACTIVITY_KIND_MEMCPY', 'CUPTI_ACTIVITY_KIND_MEMSET']

CONVERSION_TO_SECONDS = 1e-6 #Nsight claims ns for duration but found to be us

def generate_transfer_stats(transfers):
    transfer_sizes = []
    transfer_durations = []
    temp_bandwidth = []
    histgram_bins = []

    for _, duration, size in transfers[1]:
        transfer_sizes.append ( size )
        transfer_durations.append ( duration )
        temp_bandwidth.append ( (size, size / (duration * CONVERSION_TO_SECONDS))) # convert to B/s

    transfer_data = {}

    if transfer_sizes:
        transfer_data.update ( generate_statistics ( transfer_sizes, "Transfer Size" ) )
        histogram_data, returned_hist_data = create_histogram ( transfer_sizes, bins=10, powers_2=True, base=False,
                                                                convert_bytes=True, return_bins=True )
        if returned_hist_data:
            histgram_bins, histogram_dict = returned_hist_data
        transfer_data['Transfer Size']['Distribution'] = histogram_data
    else:
        transfer_data['Transfer Size'] = None

    if transfer_durations:
        transfer_data.update ( generate_statistics ( transfer_durations, "Transfer Durations" ) )
        histogram_data = create_histogram ( transfer_durations, bins=10, powers_2=False, base=False, convert_bytes=False )
        transfer_data['Transfer Durations']['Distribution'] = histogram_data
    else:
        transfer_data['Transfer Durations'] = None

    if histgram_bins:
        bandwidth_distro = [[] for _ in range ( len ( histgram_bins ) )]
        for index, (start, end) in enumerate ( histgram_bins ):
            for size, bandwidth in temp_bandwidth:
                if start <= size < end:
                    bandwidth_distro[index].append ( bandwidth )

        histogram_dict['Histogram'] = bandwidth_distro
        transfer_data['Bandwidth Distribution'] = histogram_dict
        transfer_data['Bandwidth Distribution']['Raw Data'] = temp_bandwidth
    else:
        transfer_data['Bandwidth Distribution'] = None

    return transfers[0], transfer_data


def parallel_parse_transfer_data(queries_res):
    total_tasks = len ( queries_res )
    completed_tasks = 0

    with ProcessPoolExecutor ( max_workers=MAX_WORKERS ) as executor:
        futures = []
        for data in queries_res:
            future = executor.submit ( generate_transfer_stats, data )
            futures.append ( future )

        results = []
        for future in as_completed ( futures ):
            results.append ( future.result () )
            completed_tasks += 1
            if int ( (completed_tasks / total_tasks) * 100 ) % 10 == 0:
                logging.info ( f"Progress: {(completed_tasks / total_tasks) * 100:.1f}%" )

    return results


def create_specific_transfer_stats(transfer_stats, handle_outliers=False):
    dict = {}
    duration_cluster_data = []
    size_cluster_data = []
    combined_raw_duration_data = []
    combined_raw_size_data = []

    for transfer_id, transfer_info in transfer_stats.items ():
        if transfer_info:
            if transfer_info['Transfer Size']:
                if transfer_info['Transfer Size']["Raw Data"]:
                    combined_raw_size_data.extend ( transfer_info['Transfer Size']["Raw Data"] )
                if transfer_info['Transfer Size']['Mean'] and transfer_info['Transfer Size'][
                    'Median'] and transfer_info[
                    "Instance"]:
                    size_cluster_data.append ( [transfer_info['Transfer Size']['Mean'],
                                                transfer_info['Transfer Size']['Median'],
                                                transfer_info["Instance"]] )
            if transfer_info['Transfer Durations']:
                if transfer_info['Transfer Durations']["Raw Data"]:
                    combined_raw_duration_data.extend ( transfer_info['Transfer Durations']["Raw Data"] )
                if transfer_info['Transfer Durations']['Mean'] and transfer_info['Transfer Durations'][
                    'Median'] and transfer_info[
                    "Instance"]:
                    duration_cluster_data.append ( [transfer_info['Transfer Durations']['Mean'],
                                                    transfer_info['Transfer Durations']['Median'],
                                                    transfer_info["Instance"]] )

    if handle_outliers and duration_cluster_data: duration_cluster_data = remove_outliers ( duration_cluster_data )
    if handle_outliers and size_cluster_data: size_cluster_data = remove_outliers ( size_cluster_data )

    if combined_raw_duration_data:
        dict.update ( generate_statistics ( combined_raw_duration_data, 'Transfer Durations', disable_raw=True ) )
        dict['Transfer Durations']['Distribution'] = create_histogram ( combined_raw_duration_data )
    if combined_raw_size_data:
        dict.update ( generate_statistics ( combined_raw_size_data, 'Transfer Size', disable_raw=True ) )
        dict['Transfer Size']['Distribution'] = create_histogram ( combined_raw_size_data )
    if duration_cluster_data:
        dict['Transfer Durations']['k-mean'] = {'Raw Data': duration_cluster_data}
    if size_cluster_data:
        dict['Transfer Size']['k-mean'] = {'Raw Data': size_cluster_data}

    return dict
