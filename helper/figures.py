import os
import numpy as np
from absl import logging
from matplotlib import pyplot as plt, ticker
from matplotlib.ticker import ScalarFormatter
from sklearn.cluster import KMeans

from helper.general import convert_size, convert_duration

plt.rcParams.update({
    'font.size': 16,           # Default font size for all text
    'axes.titlesize': 16,      # Title font size
    'axes.labelsize': 16,      # Axis label font size
    'xtick.labelsize': 16,     # X-axis tick label font size
    'ytick.labelsize': 16,     # Y-axis tick label font size
})


def format_power_2_ticks(value, _):
    if value >= 2 ** 50:
        return f'{value / 2 ** 50:.2f}P'
    elif value >= 2 ** 40:
        return f'{value / 2 ** 40:.2f}T'
    elif value >= 2 ** 30:
        return f'{value / 2 ** 30:.2f}G'
    elif value >= 2 ** 20:
        return f'{value / 2 ** 20:.2f}M'
    elif value >= 2 ** 10:
        return f'{value / 2 ** 10:.2f}K'
    else:
        return f'{value:.2f}'


def format_power_10_ticks(value, _):
    if value >= 1e15:
        return f'{value / 1e15:.2f}P'
    elif value >= 1e12:
        return f'{value / 1e12:.2f}T'
    elif value >= 1e9:
        return f'{value / 1e9:.2f}G'
    elif value >= 1e6:
        return f'{value / 1e6:.2f}M'
    elif value >= 1e3:
        return f'{value / 1e3:.2f}K'
    else:
        return f'{value:.2f}'


def create_and_plot_k_mean_statistics(cluster_data, title, parent_dir):
    X = np.array ( cluster_data['Raw Data'] )

    wcss_values = []
    max_clusters = min ( 8, len ( X ) )
    for i in range ( 1, max_clusters + 1 ):
        kmeans = KMeans ( n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0 )
        kmeans.fit ( X )
        wcss_values.append ( kmeans.inertia_ )

    # Plot the WCSS values
    fig, ax = plt.subplots ( figsize=(10, 8) )
    ax.plot ( range ( 1, max_clusters + 1 ), wcss_values, marker='o' )
    ax.set_title ( 'Elbow Method for Optimal k' )
    ax.set_xlabel ( 'Number of clusters (k)' )
    ax.set_ylabel ( 'Within-Cluster Sum of Squares (WCSS)' )
    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )
    file = parent_dir + "/" + title.split ( " " )[0].replace ( '-', '_' ) + '_elbow_method.png'
    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )

    cluster_dir = parent_dir + '/Cluster Options'
    os.makedirs ( cluster_dir, exist_ok=True )

    for n_clusters in range ( 1, max_clusters + 1 ):
        kmeans = KMeans ( n_clusters=n_clusters, random_state=42 )
        cluster_labels = kmeans.fit_predict ( X )
        min_x = np.min(X[:, 0])
        min_y = np.min(X[:, 1])
        min_x_log10 = np.floor(np.log10(min_x))
        min_y_log10 = np.floor(np.log10(min_y))
        fig, ax = plt.subplots ( 1, figsize=(10, 8) )
        ax.scatter ( X[:, 0], X[:, 1], c=cluster_labels, cmap='tab10', s=50, alpha=0.5 )
        ax.set_title ( 'Execution Duration K-means Clustering' )
        ax.set_xlabel ( 'Mean Execution Duration' )
        ax.set_ylabel ( 'Median Execution Duration' )
        ax.set_xscale ( 'log', base=10 )
        ax.set_yscale ( 'log', base=10 )
        ax.set_xlim(left=10 ** min_x_log10)
        ax.set_ylim(bottom=10 ** min_y_log10)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_power_10_ticks))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_power_10_ticks))
        fig.tight_layout ()
        fig.subplots_adjust ( top=0.95 )
        file = cluster_dir + "/" + title.split ( " " )[0].replace ( ' ', '_' ) + f'_k_{n_clusters}_mean_cluster.png'
        fig.savefig ( file, bbox_inches='tight' )
        plt.close ( fig )


def plot_combined_data(combined_data, title, metric, parent_dir, raw_provided=False):
    data = []
    labels = []

    if not raw_provided:
        for name, sub_dict in combined_data.items ():
            if sub_dict[metric]:
                if sub_dict[metric]["Raw Data"] and metric == 'Bandwidth Distribution':
                    labels.append ( name )
                    temp = []
                    for transfer_size, bandwidth in sub_dict[metric]["Raw Data"]:
                        temp.append ( bandwidth )
                    data.append ( temp )
                elif sub_dict[metric]["Raw Data"]:
                    labels.append ( name )
                    data.append ( sub_dict[metric]["Raw Data"] )
    else:
        for name, sub_list in combined_data.items ():
            labels.append ( name )
            data.append ( sub_list )

    if len ( data ) < 2:
        logging.error ( f'\"{title}: Combined {metric}\" - Only 1 Raw Data found, No figure generated' )
        return None

    fig, ax = plt.subplots ( 1, figsize=(10, 8) )
    parts = ax.violinplot ( data, showmeans=True, showmedians=True )

    for pc in parts['bodies']:
        pc.set_facecolor ( 'skyblue' )
        pc.set_edgecolor ( 'black' )
        pc.set_alpha ( 0.7 )

    parts['cmedians'].set_color ( 'blue' )
    parts['cmedians'].set_linewidth ( 2 )
    parts['cmins'].set_color ( 'red' )
    parts['cmins'].set_linestyle ( '--' )
    parts['cmaxes'].set_color ( 'green' )
    parts['cmaxes'].set_linestyle ( '--' )
    parts['cbars'].set_color ( 'black' )

    ax.xaxis.set_ticks ( range ( 1, len ( labels ) + 1 ) )
    ax.xaxis.set_ticklabels ( labels )
    ax.tick_params ( axis='x', rotation=25 )
    ax.set_xlabel ( "Trace Name" )

    flat_data = [item for sublist in data for item in sublist]
    min_value = np.min ( flat_data )
    max_value = np.max ( flat_data )
    magnitude_diff = np.log10 ( max_value ) - np.log10 ( min_value )
    if magnitude_diff >= 1:
        ax.set_yscale ( 'log', base=10 )
    ax.grid ( axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5 )
    ax.grid(which='minor', axis='y', linestyle=':', linewidth=0.5, color='lightgray')
    ax.yaxis.set_major_formatter ( ticker.FuncFormatter ( format_power_10_ticks ) )
    if 'Size' in metric:
        ax.set_ylabel ( "Size (B)" )
    elif metric == 'Bandwidth Distribution':
        ax.set_ylabel ( "Bandwidth (B/s)" )
    else:
        ax.set_ylabel ( "Time (us)" )

    ax.set_title ( f"{title}: Combined {metric}" )

    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )
    file = parent_dir + "/" + title.replace ( ' ', '_' ) + '_' + metric.replace ( ' ', '_' ) + '_combined_distribution.png'
    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )


def plot_combined_overall_bandwidth_distribution(combined_data, title, parent_dir):
    data = []
    labels = []

    for name, sub_list in combined_data.items ():
        labels.append(name)
        temp = []
        for _, bandwidth in sub_list:
            temp.append (bandwidth)
        data.append ( temp )

    fig, ax = plt.subplots ( 1, figsize=(10, 8) )
    parts = ax.violinplot ( data, showmeans=True, showmedians=True )

    for pc in parts['bodies']:
        pc.set_facecolor ( 'skyblue' )
        pc.set_edgecolor ( 'black' )
        pc.set_alpha ( 0.7 )

    parts['cmedians'].set_color ( 'blue' )
    parts['cmedians'].set_linewidth ( 2 )
    parts['cmins'].set_color ( 'red' )
    parts['cmins'].set_linestyle ( '--' )
    parts['cmaxes'].set_color ( 'green' )
    parts['cmaxes'].set_linestyle ( '--' )
    parts['cbars'].set_color ( 'black' )

    min_value = min ( min ( sublist ) for sublist in data )
    ax.grid ( axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5 )
    ax.grid(which='minor', axis='y', linestyle=':', linewidth=0.5, color='lightgray')
    ax.set_title ( f'{title}: Overall Combined Bandwidth Distribution' )
    ax.xaxis.set_ticks ( range ( 1, len ( labels ) + 1 ) )
    ax.xaxis.set_ticklabels ( labels )
    ax.tick_params ( axis='x', rotation=25 )
    ax.set_xlabel ( "Trace Name" )
    ax.set_yscale ( 'log', base=10 )
    ax.yaxis.set_major_formatter ( ticker.FuncFormatter ( format_power_10_ticks ) )
    ax.set_ylabel ( "Bandwidth (B/s)" )

    if min_value > 0:
        min_value_power_of_ten = 10 ** int ( np.floor ( np.log10 ( min_value ) ) )
        ax.set_ylim ( bottom=min_value_power_of_ten )

    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )
    file = parent_dir + '/Transfer_Statistics_Overall_Combined_Bandwidth_distribution.png'
    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )


def plot_binned_bandwidth_distribution(combined_data, title, parent_dir):
    all_sizes = []
    for name, sub_list in combined_data.items():
        sizes, bandwidths = zip(*sub_list)
        all_sizes.extend(sizes)

    quantiles = np.linspace(0, 1, 8)
    bin_edges = np.quantile(all_sizes, quantiles)
    fig, ax = plt.subplots(figsize=(10, 8))
    num_configs = len(combined_data.items())
    width_per_bin = 0.25  # adjusted width for violins

    x = np.arange(len(bin_edges) - 1)  # the label locations

    for i, (name, bandwidths) in enumerate(combined_data.items()):
        binned_bandwidths = [[] for _ in range(len(bin_edges) - 1)]
        for j in range(len(bin_edges) - 1):
            bin_bandwidths = [bw for size, bw in bandwidths if bin_edges[j] <= size < bin_edges[j + 1]]
            if bin_bandwidths:
                binned_bandwidths[j] = bin_bandwidths
        if binned_bandwidths:
            for item in binned_bandwidths:
                if len(item) == 0:
                    item.append(0)

            offset = (num_configs - 1) / 2
            positions = x + offset + i * width_per_bin
            parts = ax.violinplot(binned_bandwidths, showmeans=True, showmedians=True,
                                  positions=positions, widths=width_per_bin)

            for pc in parts['bodies']:
                pc.set_facecolor('C' + str(i))
                pc.set_edgecolor('black')
                pc.set_alpha(0.7)

            parts['cmedians'].set_color('blue')
            parts['cmedians'].set_linewidth(2)
            parts['cmins'].set_color('red')
            parts['cmins'].set_linestyle('--')
            parts['cmaxes'].set_color('green')
            parts['cmaxes'].set_linestyle('--')
            parts['cbars'].set_color('black')

        ax.plot([], [], color='C' + str(i), label=name)

    ax.grid(axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5)
    ax.grid(which='minor', axis='y', linestyle=':', linewidth=0.5, color='lightgray')
    ax.set_title(f'{title}: Bandwidth Distribution by Transfer Size')
    ax.set_xticks(x + 0.5 * (num_configs - 1))
    bin_labels = [f'{convert_size ( left )}-{convert_size ( right )}' for left, right in
                  zip ( bin_edges[:-1], bin_edges[1:] )]
    ax.set_xticklabels(bin_labels, rotation=25, ha='right')
    ax.set_xlabel("Data Transfer Size")
    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_power_10_ticks))
    ax.set_ylabel("Bandwidth (B/s)")
    ax.legend()
    fig.tight_layout()
    fig.subplots_adjust(top=0.9, bottom=0.15)  # Adjust top and bottom margins
    file = parent_dir + '/' + title.replace(' ', '_') + '_Combined_Bandwidth_distribution_By_Size.png'
    fig.savefig(file, bbox_inches='tight')
    plt.close(fig)


def plot_bandwidth_distribution(histogram_data, title, parent_dir):
    array_lists = histogram_data['Histogram']
    labels = histogram_data['Bin Labels']

    x_values = np.arange ( 1, len ( array_lists ) + 1 )
    fig, ax = plt.subplots ( 1, figsize=(10, 8) )
    parts = ax.violinplot ( array_lists, showmeans=True, showmedians=True )

    for pc in parts['bodies']:
        pc.set_facecolor ( 'skyblue' )
        pc.set_edgecolor ( 'black' )
        pc.set_alpha ( 0.7 )

    parts['cmedians'].set_color ( 'blue' )
    parts['cmedians'].set_linewidth ( 2 )
    parts['cmins'].set_color ( 'red' )
    parts['cmins'].set_linestyle ( '--' )
    parts['cmaxes'].set_color ( 'green' )
    parts['cmaxes'].set_linestyle ( '--' )
    parts['cbars'].set_color ( 'black' )

    ax.xaxis.set_ticks ( x_values )
    ax.xaxis.set_ticklabels ( labels )
    ax.tick_params ( axis='x', rotation=25 )
    min_value = min ( min ( sublist ) for sublist in array_lists )
    ax.grid ( axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5 )
    ax.grid(which='minor', axis='y', linestyle=':', linewidth=0.5, color='lightgray')
    ax.set_title ( title )
    ax.set_xlabel ( "Transfer size range" )
    ax.set_yscale ( 'log', base=10 )
    ax.yaxis.set_major_formatter ( ticker.FuncFormatter ( format_power_10_ticks ) )
    ax.set_ylabel ( "Bandwidth (B/s)" )

    if min_value > 0:
        min_value_power_of_ten = 10 ** int ( np.floor ( np.log10 ( min_value ) ) )
        ax.set_ylim ( bottom=min_value_power_of_ten )

    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )
    file = parent_dir + "/" + title.split ( " " )[0].replace ( '-', '_' ) + '_bandwidth_distribution.png'
    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )


def plot_frequency_distribution(histogram_data, title, xlabel, parent_dir):
    bin_array = histogram_data['Histogram']
    labels = histogram_data['Bin Labels']

    fig, ax = plt.subplots ( 1, figsize=(10, 8) )
    ax.bar ( range ( 1, len ( bin_array ) + 1 ), bin_array, width=1, edgecolor='black' )

    x_values = np.arange ( 1, len ( bin_array ) + 1 )
    ax.xaxis.set_ticks ( x_values )
    ax.xaxis.set_ticklabels ( labels )
    ax.tick_params ( axis='x', rotation=25 )
    ax.grid ( axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5 )
    ax.grid(which='minor', axis='y', linestyle=':', linewidth=0.5, color='lightgray')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_power_10_ticks))
    ax.set_title ( title )
    ax.set_xlabel ( xlabel )
    ax.set_ylabel ( "Frequency" )
    fig.tight_layout ()
    fig.subplots_adjust ( top=0.95 )

    if 'Slack' in xlabel:
        file = parent_dir + "/" + title.split ( " " )[0] + "_" + "_".join (
            xlabel.lower ().split ( " " )[0:1] ) + '_frequency_distribution.png'
    else:
        file = parent_dir + "/" + title.split ( " " )[0] + "_" + "_".join (
            xlabel.lower ().split ( " " )[0:2] ) + '_frequency_distribution.png'

    fig.savefig ( file, bbox_inches='tight' )
    plt.close ( fig )


def plot_combined_frequency_distribution(combined_data, title, metric, parent_dir):
    all_values = []
    for name, sub_list in combined_data.items():
        all_values.extend(sub_list)

    quantiles = np.linspace(0, 1, num=9)
    bin_edges = np.quantile(all_values, quantiles)
    fig, ax = plt.subplots(figsize=(10, 8))
    num_configs = len(combined_data.items())
    width_per_bin = 0.25

    x = np.arange(len(bin_edges) - 1)

    for i, (name, data) in enumerate(combined_data.items()):
        binned_data = [[] for _ in range(len(bin_edges) - 1)]
        for j in range(len(bin_edges) - 1):
            bin_data = [val for val in data if bin_edges[j] <= val < bin_edges[j + 1]]
            if bin_data:
                binned_data[j] = bin_data
        if binned_data:
            for item in binned_data:
                if len(item) == 0:
                    item.append(0)

            offset = (num_configs - 1) / 2
            positions = x + offset + i * width_per_bin
            ax.bar(positions, [np.mean(b) for b in binned_data], width_per_bin, alpha=0.7, label=name)

    ax.grid(axis='y', linestyle='--', linewidth=0.5, color='gray', alpha=0.5)
    ax.grid(which='minor', axis='y', linestyle=':', linewidth=0.5, color='lightgray')
    ax.set_xticks(x + 0.5 * (num_configs - 1))

    if 'Size' in metric:
        ax.set_title(f'{title}: {metric} Distribution by Size')
        ax.set_xlabel("Size (B)")
        bin_labels = [f'{convert_size ( left )}-{convert_size ( right )}' for left, right in
                      zip ( bin_edges[:-1], bin_edges[1:] )]
    else:
        ax.set_title(f'{title}: {metric} Distribution by Duration')
        ax.set_xlabel("Time (us)")
        bin_labels = [f'{convert_duration ( left )}-{convert_duration ( right )}' for left, right in
                      zip ( bin_edges[:-1], bin_edges[1:] )]


    ax.set_xticklabels ( bin_labels, rotation=25, ha='right' )
    ax.set_ylabel("Frequency")
    ax.set_yscale('log', base=10)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(format_power_10_ticks))
    ax.legend()
    fig.tight_layout()
    fig.subplots_adjust(top=0.9, bottom=0.15)
    file = os.path.join(parent_dir, title.replace(' ', '_') + '_Combined_' + metric.replace(' ', '_') + '_distribution_By_Size.png')
    fig.savefig(file, bbox_inches='tight')
    plt.close(fig)