import csv


def latex_safe_string(title):
    translation_table = str.maketrans ( {char: f'\{char}' for char in '\`*_{}[]()<>#+-.!$:;,/'} )
    latex_safe_title = title.translate ( translation_table )
    return latex_safe_title


def export_single_general_stat_to_latex(data_dict, parent_dir, title):
    underscore_title = title.replace ( ' ', '_' )
    latex_filename = parent_dir + f'/{underscore_title}_general_statistics.tex'
    safe_title = latex_safe_string ( title )
    with open ( latex_filename, 'w' ) as latexfile:
        latexfile.write ( "\\begin{table}[ht]\n" )
        latexfile.write ( "\\centering\n" )
        latexfile.write ( "\\caption{" + safe_title + " General Statistics}\n" )
        latexfile.write ( "\\begin{tabular}{|c|c|c|c|c|c|}\n" )
        latexfile.write ( "\\hline\n" )
        latexfile.write (
            "\\textbf{Metric} & \\textbf{Mean} & \\textbf{Median} & \\textbf{Minimum} & \\textbf{Maximum} & \\textbf{Standard Deviation} \\\\\n" )
        latexfile.write ( "\\hline\n" )
        for metric, stats in data_dict.items ():
            if isinstance ( stats, dict ) and 'Individual' not in metric and 'Bandwidth Distribution' not in metric:
                if 'Duration' in metric or 'Slack' in metric or 'Overhead' in metric:
                    units = ' (us)'
                else:
                    units = ' (B)'

                metric = metric + units
                mean = stats.get ( 'Mean', '' )
                median = stats.get ( 'Median', '' )
                minimum = stats.get ( 'Minimum', '' )
                maximum = stats.get ( 'Maximum', '' )
                std_dev = stats.get ( 'Standard Deviation', '' )
                latexfile.write ( f"{metric} & {mean} & {median} & {minimum} & {maximum} & {std_dev} \\\\\n" )
                latexfile.write ( "\\hline\n" )
        latexfile.write ( "\\end{tabular}\n" )
        latexfile.write ( "\\label{tab:" + underscore_title + "_general_stats}\n" )
        latexfile.write ( "\\end{table}\n" )


def export_single_general_stat_to_CSV(data_dict, parent_dir, title):
    underscore_title = title.replace ( ' ', '_' )
    csv_filename = parent_dir + f'/{underscore_title}_general_statistics.csv'
    with open ( csv_filename, 'w', newline='' ) as csvfile:
        writer = csv.writer ( csvfile )
        writer.writerow ( [f"{title} General Statistics"] )
        writer.writerow ( ['Metric', 'Mean', 'Median', 'Minimum', 'Maximum', 'Standard Deviation'] )
        for metric_name, stats in data_dict.items ():
            if isinstance ( stats,
                            dict ) and 'Individual' not in metric_name and 'Bandwidth Distribution' not in metric_name:
                if 'Duration' in metric_name or 'Slack' in metric_name or 'Overhead' in metric_name:
                    units = ' (us)'
                else:
                    units = ' (B)'
                writer.writerow ( [metric_name + units] + [stats.get ( stat, '' ) for stat in
                                                           ['Mean', 'Median', 'Minimum', 'Maximum',
                                                            'Standard Deviation']] )


def export_summary_stat_to_latex(data_dict, parent_dir, title, stat_name):
    stat_name_replaced = stat_name.replace ( ' ', '_' )
    latex_filename = parent_dir + f'/{stat_name_replaced}_summary_statistics.tex'
    safe_title = latex_safe_string ( title )

    if 'Duration' in stat_name or 'Slack' in stat_name or 'Overhead' in stat_name:
        header = "\\textbf{Name} & \\textbf{Total Time (\\%)} & \\textbf{Total Time (us)} & \\textbf{Instances} & \\textbf{Mean (us)} & \\textbf{Median (us)} & \\textbf{Minimum (us)} & \\textbf{Maximum (us)} & \\textbf{Standard Deviation} \\\\\n"
    else:
        header = "\\textbf{Name} & \\textbf{Total Time (\\%)} & \\textbf{Total Time (us)} & \\textbf{Instances} & \\textbf{Mean (B)} & \\textbf{Median (B)} & \\textbf{Minimum (B)} & \\textbf{Maximum (B)} & \\textbf{Standard Deviation} \\\\\n"

    with open ( latex_filename, 'w' ) as latexfile:
        latexfile.write ( "\\begin{table}[ht]\n" )
        latexfile.write ( "\\centering\n" )
        latexfile.write ( "\\caption{" + safe_title + " Summary " + stat_name + " Statistics}\n" )
        latexfile.write ( "\\begin{tabular}{|c|c|c|c|c|c|c|c|c|}\n" )
        latexfile.write ( "\\hline\n" )
        latexfile.write (header)
        latexfile.write ( "\\hline\n" )
        for metric, stats in data_dict.items ():
            name = stats['Name'] if "Kernel" in title else metric
            name = latex_safe_string ( name )
            time_percent = stats['Time Percent']
            time_duration = stats['Time Total']
            instances = stats['Instance']

            if isinstance ( stats[stat_name], dict ):
                mean = stats[stat_name].get ( 'Mean', '' )
                median = stats[stat_name].get ( 'Median', '' )
                minimum = stats[stat_name].get ( 'Minimum', '' )
                maximum = stats[stat_name].get ( 'Maximum', '' )
                std_dev = stats[stat_name].get ( 'Standard Deviation', '' )
                latexfile.write ( f"{name} & {time_percent} & {time_duration} & {instances} & {mean} & {median} & {minimum} & {maximum} & {std_dev} \\\\\n" )
                latexfile.write ( "\\hline\n" )
        latexfile.write ( "\\end{tabular}\n" )
        latexfile.write ( "\\label{tab:" + stat_name_replaced + "_summary_stats}\n" )
        latexfile.write ( "\\end{table}\n" )


def export_summary_stat_to_CSV(data_dict, parent_dir, title, stat_name):
    stat_name_replaced = stat_name.replace ( ' ', '_' )
    csv_filename = parent_dir + f'/{stat_name_replaced}_summary_statistics.csv'
    with open ( csv_filename, 'w', newline='' ) as csvfile:
        writer = csv.writer ( csvfile )
        writer.writerow ( [f"{title} Summary {stat_name} Statistics"] )
        if 'Duration' in stat_name or 'Slack' in stat_name or 'Overhead' in stat_name:
            writer.writerow ( ['Name', 'Total Time (%)', 'Total Time (us)', 'Instances', 'Mean (us)', 'Median (us)', 'Minimum (us)', 'Maximum (us)', 'Standard Deviation'] )
        else:
            writer.writerow ( ['Name', 'Total Time (%)', 'Total Time (us)', 'Instances', 'Mean (B)', 'Median (B)', 'Minimum (B)', 'Maximum (B)', 'Standard Deviation'] )

        for metric_name, stats in data_dict.items ():
            name = stats['Name'] if "Kernel" in title else metric_name
            time_percent = stats['Time Percent']
            time_duration = stats['Time Total']
            instances = stats['Instance']

            if isinstance ( stats[stat_name], dict ):
                writer.writerow ( [name, time_percent, time_duration, instances] +
                                  [stats[stat_name].get ( stat, '' ) for stat in
                                   ['Mean', 'Median', 'Minimum', 'Maximum', 'Standard Deviation']] )


def export_overall_summary_stat_to_latex(data_dict, parent_dir):
    latex_filename = parent_dir + '/overall_application_summary_statistics.tex'
    total_time = data_dict['Time Total']

    with open ( latex_filename, 'w' ) as latexfile:
        latexfile.write ( "\\begin{table}[ht]\n" )
        latexfile.write ( "\\centering\n" )
        latexfile.write ( "\\caption{Overall Application Duration Summary}\n" )
        latexfile.write ( "\\begin{tabular}{|c|c|c|c|}\n" )
        latexfile.write ( "\\hline\n" )
        latexfile.write ("\\textbf{Name} & \\textbf{Total Relative Time (\\%)} & \\textbf{Total Time (us)} & \\textbf{Instances} \\\\\n")
        latexfile.write ( "\\hline\n" )
        for name, stats in data_dict.items():
            if isinstance(stats, dict):
                time_duration = stats['Time Total']
                instances = stats['Instance']
                time_percent = round( time_duration / total_time * 100, 2)
                latexfile.write ( f"{name} & {time_percent} & {time_duration} & {instances} \\\\\n" )
                latexfile.write ( "\\hline\n" )
        latexfile.write ( "\\end{tabular}\n" )
        latexfile.write ( "\\label{tab:overall_summary_stats}\n" )
        latexfile.write ( "\\end{table}\n" )


def export_summary_summary_stat_to_CSV(data_dict, parent_dir):
    csv_filename = parent_dir + f'/overall_application_summary_statistics.csv'
    total_time = data_dict['Time Total']

    with open ( csv_filename, 'w', newline='' ) as csvfile:
        writer = csv.writer ( csvfile )
        writer.writerow ( [f"Overall Application Duration Summary"] )
        writer.writerow ( ['Name', 'Total Relative Time (%)', 'Total Time (us)', 'Instances'] )
        for name, stats in data_dict.items():
            if isinstance(stats, dict):
                time_duration = stats['Time Total']
                instances = stats['Instance']
                time_percent = round( time_duration / total_time * 100, 2)
                writer.writerow ( [name, time_percent, time_duration, instances] )


def export_combined_overall_component_summary_stat_to_latex(data_dict, stat, parent_dir):
    latex_filename = parent_dir + '/overall_combined_' + stat.replace(' ', '_') + '_summary_statistics.tex'

    with open ( latex_filename, 'w' ) as latexfile:
        latexfile.write ( "\\begin{table}[ht]\n" )
        latexfile.write ( "\\centering\n" )
        latexfile.write ( "\\caption{Overall" + stat + " Duration Summary}\n" )
        latexfile.write ( "\\begin{tabular}{|c|c|c|c|}\n" )
        latexfile.write ( "\\hline\n" )
        latexfile.write ("\\textbf{Trace Name} & \\textbf{Individual Trace Duration (\\%)} & \\textbf{Total Time (us)} & \\textbf{Instances} \\\\\n")
        latexfile.write ( "\\hline\n" )
        for name, stats in data_dict.items():
            time_duration = stats['Time Total']
            instances = stats['Instance']
            relative_time = stats['Relative Total Time']
            time_percent = round( time_duration / relative_time * 100, 2)
            latexfile.write ( f"{name} & {time_percent} & {time_duration} & {instances} \\\\\n" )
            latexfile.write ( "\\hline\n" )
        latexfile.write ( "\\end{tabular}\n" )
        latexfile.write ( "\\label{tab:overall_combined_" + stat.replace(' ', '_') + "_summary_statistics}\n" )
        latexfile.write ( "\\end{table}\n" )


def export_combined_overall_component_summary_stat_to_CSV(data_dict, stat, parent_dir):
    csv_filename = parent_dir + '/overall_combined_' + stat.replace(' ', '_') + '_summary_statistics.csv'

    with open ( csv_filename, 'w', newline='' ) as csvfile:
        writer = csv.writer ( csvfile )
        writer.writerow([f"Overall {stat} Duration Summary"])
        writer.writerow ( ['Trace Name', 'Individual Trace Duration (%)', 'Total Time (us)', 'Instances'] )
        for name, stats in data_dict.items():
            time_duration = stats['Time Total']
            instances = stats['Instance']
            relative_time = stats['Relative Total Time']
            time_percent = round(time_duration / relative_time * 100, 2)
            writer.writerow ( [name, time_percent, time_duration, instances] )


def export_combined_overall_duration_summary_stat_to_latex(data_dict, parent_dir):
    latex_filename = parent_dir + '/overall_combined_duration_summary_statistics.tex'

    with open ( latex_filename, 'w' ) as latexfile:
        latexfile.write ( "\\begin{table}[ht]\n" )
        latexfile.write ( "\\centering\n" )
        latexfile.write ( "\\caption{Overall Trace Duration Summary}\n" )
        latexfile.write ( "\\begin{tabular}{|c|c|}\n" )
        latexfile.write ( "\\hline\n" )
        latexfile.write ("\\textbf{Trace Name} & \\textbf{Total Trace Time (us)} \\\\\n")
        latexfile.write ( "\\hline\n" )
        for name, stats in data_dict.items():
            time_duration = stats['Total Duration']
            latexfile.write ( f"{name} &  {time_duration} \\\\\n" )
            latexfile.write ( "\\hline\n" )
        latexfile.write ( "\\end{tabular}\n" )
        latexfile.write ( "\\label{tab:overall_combined_duration_summary_statistics}\n" )
        latexfile.write ( "\\end{table}\n" )


def export_combined_overall_duration_summary_stat_to_CSV(data_dict, parent_dir):
    csv_filename = parent_dir + '/overall_combined_duration_summary_statistics.csv'

    with open ( csv_filename, 'w', newline='' ) as csvfile:
        writer = csv.writer ( csvfile )
        writer.writerow([f"Overall Trace Duration Summary"])
        writer.writerow ( ['Trace Name', 'Total Trace Time (us)'] )
        for name, stats in data_dict.items():
            time_duration = stats['Total Duration']
            writer.writerow ( [name, time_duration] )


def export_combined_summary_stat_to_latex(data_dict, parent_dir, title, stat_name):
    stat_name_replaced = stat_name.replace ( ' ', '_' )
    latex_filename = parent_dir + f'/{title}_{stat_name_replaced}_combined_summary_statistics.tex'
    safe_title = latex_safe_string ( title )

    if 'Duration' in stat_name or 'Slack' in stat_name or 'Overhead' in stat_name:
        header = "\\textbf{Name} & \\textbf{Total Time (\\%)} & \\textbf{Total Time (us)} & \\textbf{Instances} & \\textbf{Mean (us)} & \\textbf{Median (us)} & \\textbf{Minimum (us)} & \\textbf{Maximum (us)} & \\textbf{Standard Deviation} \\\\\n"
    else:
        header = "\\textbf{Name} & \\textbf{Total Time (\\%)} & \\textbf{Total Time (us)} & \\textbf{Instances} & \\textbf{Mean (B)} & \\textbf{Median (B)} & \\textbf{Minimum (B)} & \\textbf{Maximum (B)} & \\textbf{Standard Deviation} \\\\\n"

    with open ( latex_filename, 'w' ) as latexfile:
        latexfile.write ( "\\begin{table}[ht]\n" )
        latexfile.write ( "\\centering\n" )
        latexfile.write ( "\\caption{" + safe_title + " Combined  " + stat_name + " Summary Statistics}\n" )
        latexfile.write ( "\\begin{tabular}{|c|c|c|c|c|c|c|c|c|}\n" )
        latexfile.write ( "\\hline\n" )
        latexfile.write (header)
        latexfile.write ( "\\hline\n" )
        for metric, stats in data_dict.items ():
            name = metric
            name = latex_safe_string ( name )
            time_percent = stats['Time Percent']
            time_duration = stats['Time Total']
            instances = stats['Instance']

            if isinstance ( stats[stat_name], dict ):
                mean = stats[stat_name].get ( 'Mean', '' )
                median = stats[stat_name].get ( 'Median', '' )
                minimum = stats[stat_name].get ( 'Minimum', '' )
                maximum = stats[stat_name].get ( 'Maximum', '' )
                std_dev = stats[stat_name].get ( 'Standard Deviation', '' )
                latexfile.write ( f"{name} & {time_percent} & {time_duration} & {instances} & {mean} & {median} & {minimum} & {maximum} & {std_dev} \\\\\n" )
                latexfile.write ( "\\hline\n" )
        latexfile.write ( "\\end{tabular}\n" )
        latexfile.write ( "\\label{tab:" + stat_name_replaced + "_summary_stats}\n" )
        latexfile.write ( "\\end{table}\n" )


def export_combined_summary_stat_to_CSV(data_dict, parent_dir, title, stat_name):
    stat_name_replaced = stat_name.replace ( ' ', '_' )
    csv_filename = parent_dir + f'/{title}_{stat_name_replaced}_combined_summary_statistics.csv'

    with open ( csv_filename, 'w', newline='' ) as csvfile:
        writer = csv.writer ( csvfile )
        writer.writerow ( [f"{title} Combined {stat_name} Summary Statistics"] )
        if 'Duration' in stat_name or 'Slack' in stat_name or 'Overhead' in stat_name:
            writer.writerow (
                ['Configuration', 'Total Time (%)', 'Total Time (us)', 'Instances', 'Mean (us)', 'Median (us)', 'Minimum (us)',
                 'Maximum (us)', 'Standard Deviation'] )
        else:
            writer.writerow (
                ['Configuration', 'Total Time (%)', 'Total Time (us)', 'Instances', 'Mean (B)', 'Median (B)', 'Minimum (B)',
                 'Maximum (B)', 'Standard Deviation'] )

        for metric_name, stats in data_dict.items ():
            name = metric_name
            time_percent = stats['Time Percent']
            time_duration = stats['Time Total']
            instances = stats['Instance']

            if isinstance ( stats[stat_name], dict ):
                writer.writerow ( [name, time_percent, time_duration, instances] +
                                  [stats[stat_name].get ( stat, '' ) for stat in
                                   ['Mean', 'Median', 'Minimum', 'Maximum', 'Standard Deviation']] )


def export_combined_overall_summary_stat_to_latex(data_dict, parent_dir, title, stat_name):
    stat_name_replaced = stat_name.replace ( ' ', '_' )
    title_replaced = title.replace ( ' ', '_' )
    latex_filename = parent_dir + f'/{title_replaced}_{stat_name_replaced}_combined_summary_statistics.tex'
    safe_title = latex_safe_string ( title )

    if 'Duration' in stat_name or 'Slack' in stat_name or 'Overhead' in stat_name:
        header = "\\textbf{Name} & \\textbf{Mean (us)} & \\textbf{Median (us)} & \\textbf{Minimum (us)} & \\textbf{Maximum (us)} & \\textbf{Standard Deviation} \\\\\n"
    else:
        header = "\\textbf{Name} &  \\textbf{Mean (B)} & \\textbf{Median (B)} & \\textbf{Minimum (B)} & \\textbf{Maximum (B)} & \\textbf{Standard Deviation} \\\\\n"

    with open ( latex_filename, 'w' ) as latexfile:
        latexfile.write ( "\\begin{table}[ht]\n" )
        latexfile.write ( "\\centering\n" )
        latexfile.write ( "\\caption{" + safe_title + " Combined  " + stat_name + " Summary Statistics}\n" )
        latexfile.write ( "\\begin{tabular}{|c|c|c|c|c|c|}\n" )
        latexfile.write ( "\\hline\n" )
        latexfile.write (header)
        latexfile.write ( "\\hline\n" )
        for metric, stats in data_dict.items ():
            name = metric
            name = latex_safe_string ( name )
            if isinstance ( stats, dict ):
                mean = stats.get ( 'Mean', '' )
                median = stats.get ( 'Median', '' )
                minimum = stats.get ( 'Minimum', '' )
                maximum = stats.get ( 'Maximum', '' )
                std_dev = stats.get ( 'Standard Deviation', '' )
                latexfile.write ( f"{name} & {mean} & {median} & {minimum} & {maximum} & {std_dev} \\\\\n" )
                latexfile.write ( "\\hline\n" )
        latexfile.write ( "\\end{tabular}\n" )
        latexfile.write ( "\\label{tab:" + title_replaced + '_' + stat_name_replaced + "_summary_stats}\n" )
        latexfile.write ( "\\end{table}\n" )


def export_combined_overall_summary_stat_to_CSV(data_dict, parent_dir, title, stat_name):
    stat_name_replaced = stat_name.replace ( ' ', '_' )
    title_replaced = title.replace ( ' ', '_' )
    csv_filename = parent_dir + f'/{title_replaced}_{stat_name_replaced}_combined_summary_statistics.csv'

    with open ( csv_filename, 'w', newline='' ) as csvfile:
        writer = csv.writer ( csvfile )
        writer.writerow ( [f"{title} Combined {stat_name} Summary Statistics"] )
        if 'Duration' in stat_name or 'Slack' in stat_name or 'Overhead' in stat_name:
            writer.writerow (
                ['Trace Name', 'Mean (us)', 'Median (us)', 'Minimum (us)',
                 'Maximum (us)', 'Standard Deviation'] )
        else:
            writer.writerow (
                ['Trace Name', 'Mean (B)', 'Median (B)', 'Minimum (B)',
                 'Maximum (B)', 'Standard Deviation'] )

        for metric_name, stats in data_dict.items ():
            name = metric_name

            if isinstance ( stats, dict ):
                writer.writerow ( [name] +
                                  [stats.get ( stat, '' ) for stat in
                                   ['Mean', 'Median', 'Minimum', 'Maximum', 'Standard Deviation']] )
