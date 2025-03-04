import os
import multiprocessing
import time
from absl import flags

from helper.extraction import create_statistics_from_file
from helper.general import *
from helper.export_statistics import generation_tables_and_figures

# General Flags
flags.DEFINE_string('output_dir', "output", "Name of directory to save generated NAV files and export Tables and Figures (default: ./output)", short_name='o')
flags.DEFINE_string('multi_data_label', None, "(REQUIRED for multi-files) Labels for each database/json file provided to distinguish in statistics ex:(1 GPU, 2 GPU, 3 GPU), commas used to split names and order must be same as provided files", short_name='mdl')
flags.DEFINE_integer('max_workers', None, "Number of threads to split work (Default to CPU count)", short_name='mw')

# Extraction Flags
flags.DEFINE_string('data_file', None, "Data Base file for extraction (sqlite)", short_name='df')
flags.DEFINE_string('nav_file', None, "NAV file with extracted statistics", short_name='nf')
flags.DEFINE_boolean('no_kernel_metrics', False, "export kernel metrics", short_name='nkm')
flags.DEFINE_boolean('no_transfer_metrics', False, "export transfer metrics", short_name='ntm')
flags.DEFINE_boolean('no_communication_metrics', False, "export communication metrics", short_name='ncm')
flags.DEFINE_boolean('no_save_data', False, "Save metrics to NAV file", short_name='nsd')

# Graphics and Table Flags
flags.DEFINE_boolean('no_metrics_output', None, "disable metrics export after extraction", short_name='nmo')
flags.DEFINE_boolean('no_compare_metrics_output', False, "disable comparison metrics export (multi-file only)", short_name='ncmo')
flags.DEFINE_boolean('no_general_metrics_output', False, "disable general metrics export (Kernel, Transfer, Communication)", short_name='ngmo')
flags.DEFINE_boolean('no_specific_metrics_output', False, "disable specific metrics export (Duration, Size, Slack, Overhead, etc)", short_name='nsmo')
flags.DEFINE_boolean('no_individual_metrics_output', False, "disable individual metrics export (individual kernel, transfer, communication statistics)", short_name='nimo')

FLAGS = flags.FLAGS

def run(args):
    files, num_files, file_labels, output_data, extract_data = file_args_checking(args)
    output_dir = None
    output_dir_name = FLAGS.output_dir

    if num_files > 1:
        temp = []
        for label in file_labels:
            dir = f"./{output_dir_name}/{label}/"
            os.makedirs(dir, exist_ok=True)
            temp.append(dir)
        output_dir = temp
        output_dir.append(output_dir_name)
    else:
        output_dir = f"./{output_dir_name}/" + files.split(".")[0] + "/"
        os.makedirs(output_dir, exist_ok=True)

    extracted_data = {}

    if extract_data:
        if num_files > 1:
            for i, file in enumerate(files):
                extracted_data[file_labels[i]] = create_statistics_from_file(file, output_dir[i], FLAGS)
        else:
            extracted_data.update(create_statistics_from_file(files, output_dir, FLAGS))
    else:
        if num_files > 1:
            for i, file in enumerate(files):
                extracted_data[file_labels[i]] = import_from_NAV(file)
        else:
            extracted_data.update(import_from_NAV(files))

    if output_data and extracted_data:
        no_compare = True if num_files < 2 and not args.no_compare_metrics_output else False
        generation_tables_and_figures(extracted_data, no_compare, args.no_general_metrics_output, args.no_specific_metrics_output, args.no_individual_metrics_output, num_files, output_dir)


def main(argv):
    args = FLAGS
    logging.set_verbosity(logging.INFO)
    if not args.data_file and not args.nav_file:
        raise app.UsageError("Must provide path to data base file or already parsed json file")

    if not args.max_workers:
        max_workers = multiprocessing.cpu_count()
        MAX_WORKERS = max_workers
    else:
        MAX_WORKERS = args.max_workers
    logging.info(f"Using {MAX_WORKERS} threads")
    start_time = time.time()
    try:
        run(args)
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        exit(1)
    end_time = time.time()
    execution_time = end_time - start_time

    # Convert seconds to hours, minutes, and seconds
    hours, remainder = divmod(execution_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = (execution_time % 1) * 1000  # Get milliseconds

    # Format the time as HH:MM:SS.sss (with milliseconds)
    formatted_time = "{:02}:{:02}:{:02}.{:03}".format(
        int(hours), int(minutes), int(seconds), int(milliseconds)
    )
    logging.info("Script Execution Time: %s", formatted_time)


if __name__ == "__main__":
    app.run(main)
