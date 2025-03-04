from concurrent.futures import ProcessPoolExecutor, as_completed

from absl import logging

from helper.general import generate_statistics, MAX_WORKERS, create_histogram, remove_outliers

QUERY_COMMUNICATION = """
WITH
    domains AS (
        SELECT
            min(start),
            domainId AS id,
            globalTid AS globalTid,
            text AS name
        FROM
            NVTX_EVENTS
        WHERE
            eventType == 75
        GROUP BY 2, 3
    ),
    maxts AS(
        SELECT max(max(start), max(end)) AS m
        FROM   NVTX_EVENTS
    ),
    nvtx AS (
        SELECT
            coalesce(ne.end, (SELECT m FROM maxts)) - ne.start AS duration,
            CASE
                WHEN d.name NOT NULL AND sid.value IS NOT NULL
                    THEN d.name || ':' || sid.value
                WHEN d.name NOT NULL AND sid.value IS NULL
                    THEN d.name || ':' || ne.text
                WHEN d.name IS NULL AND sid.value NOT NULL
                    THEN sid.value
                ELSE ne.text
            END AS tag,
            CASE ne.eventType
                WHEN 59
                    THEN 'PushPop'
                WHEN 60
                    THEN 'StartEnd'
                WHEN 70
                    THEN 'PushPop'
                WHEN 71
                    THEN 'StartEnd'
                ELSE 'Unknown'
            END AS style
        FROM
            NVTX_EVENTS AS ne
        LEFT OUTER JOIN
            domains AS d
            ON ne.domainId == d.id
                AND (ne.globalTid & 0x0000FFFFFF000000) == (d.globalTid & 0x0000FFFFFF000000)
        LEFT OUTER JOIN
            StringIds AS sid
            ON ne.textId == sid.id
        WHERE
            ne.eventType == 59
            OR
            ne.eventType == 60
            OR
            ne.eventType == 70
            OR
            ne.eventType == 71
    ),
    summary AS (
        SELECT
            tag AS name,
            style AS style,
            sum(duration) AS total,
            count(*) AS num
        FROM
            nvtx
        GROUP BY 1, 2
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
    SELECT
        name AS "Name",
        round(total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
        total AS "Total Time:dur_ns",
        num AS "Instances"
    FROM
        summary
    ORDER BY 2 DESC
"""

QUERY_COMMUNICATION_STATS = """ 
WITH
    max_times AS (
        SELECT MAX(start) AS max_start, MAX(end) AS max_end
        FROM NVTX_EVENTS
    ),
    nvtx AS (
        SELECT
            COALESCE(ne.end, (SELECT max_end FROM max_times)) - ne.start AS duration,
            CASE
                WHEN d.name IS NOT NULL AND sid.value IS NOT NULL THEN d.name || ':' || sid.value
                WHEN d.name IS NOT NULL AND sid.value IS NULL THEN d.name || ':' || ne.text
                WHEN d.name IS NULL AND sid.value IS NOT NULL THEN sid.value
                ELSE ne.text
            END AS tag
        FROM
            NVTX_EVENTS AS ne
        LEFT OUTER JOIN
            (
                SELECT
                    MIN(start) AS min_start,
                    domainId AS id,
                    globalTid AS globalTid,
                    text AS name
                FROM
                    NVTX_EVENTS
                WHERE
                    eventType = 75
                GROUP BY
                    domainId, globalTid, text
            ) AS d
        ON
            ne.domainId = d.id
            AND (ne.globalTid & 0x0000FFFFFF000000) = (d.globalTid & 0x0000FFFFFF000000)
        LEFT OUTER JOIN
            StringIds AS sid
        ON
            ne.textId = sid.id
        WHERE
            ne.eventType IN (59, 60, 70, 71)
    )
SELECT
    tag AS "Name",
    duration AS "Duration:dur_ns"
FROM
    nvtx
WHERE
    name = ?
"""

COMM_REQUIRED_TABLES = ['NVTX_EVENTS', 'StringIds']


def generate_communicaiton_stats(comm):
    durations = [dur[1] for dur in comm[1]]
    label = comm[0]
    dict = {}

    if durations and label:
        dict[label] = generate_statistics(durations, 'Execution Duration')
        histogram_data = create_histogram( durations, bins=10, powers_2=False, base=False,
                                           convert_bytes=False, return_bins=False )
        dict[label]['Execution Duration']['Distribution'] = histogram_data
    else:
        dict[label] = None

    return label, dict[label]


def parallel_parse_communication_data(queries_res):
    total_tasks = len(queries_res)
    completed_tasks = 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for data in queries_res:
            future = executor.submit(generate_communicaiton_stats, data)
            futures.append(future)

        results = []
        for future in as_completed(futures):
            results.append(future.result())
            completed_tasks += 1

            # Log progress every 10%
            if int((completed_tasks / total_tasks) * 100 % 10) == 0:
                logging.info(f"Progress: {(completed_tasks / total_tasks) * 100:.1f}%")

    return results


def create_specific_communication_stats(comm_stats, handle_outliers=False):
    dict = {}
    cluster_data = []
    combined_raw_data = []

    for kernel_id, kernel_info in comm_stats.items():
        if kernel_info["Execution Duration"]:
            if kernel_info["Execution Duration"]["Raw Data"]:
                combined_raw_data.extend(kernel_info["Execution Duration"]["Raw Data"])
            if kernel_info["Execution Duration"]['Mean'] and kernel_info["Execution Duration"]['Median'] and \
                    kernel_info[
                        "Instance"]:
                cluster_data.append(
                    [kernel_info["Execution Duration"]['Mean'], kernel_info["Execution Duration"]['Median'],
                     kernel_info["Instance"]])

    if handle_outliers and cluster_data: cluster_data = remove_outliers(cluster_data)
    if combined_raw_data:
        dict.update(generate_statistics(combined_raw_data, "Execution Duration", disable_raw=True))
        dict["Execution Duration"]['Distribution'] = create_histogram(combined_raw_data)
    if cluster_data:
        dict["Execution Duration"]['k-mean'] = {'Raw Data': cluster_data}

    return dict
