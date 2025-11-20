#!/usr/bin/env python3
"""
Visualization script for experiment results.
Generates graphs comparing throughput vs input data size for various experiments.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from pathlib import Path

# Set style for better-looking plots
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.size'] = 10


def human_readable_throughput(value, _pos):
    """Format throughput values on a log axis using human-friendly units."""
    if value >= 1_000_000:
        return f"{value/1_000_000:.0f}M"
    if value >= 1_000:
        return f"{value/1_000:.0f}K"
    if value == 0:
        return "0"
    return f"{int(value)}"


def load_experiment1_data(csv_path):
    """Load experiment 1 CSV data."""
    df = pd.read_csv(csv_path)
    return df

def load_experiment2_data(csv_path):
    """Load experiment 2 CSV data, handling duplicate rows."""
    df = pd.read_csv(csv_path)
    # Remove duplicate data_size_mb entries, keeping the first occurrence
    df = df.drop_duplicates(subset=['data_size_mb'], keep='first')
    return df

def plot_experiment1(data_dir='results'):
    """Plot experiment 1: Search comparison (binary search vs b-tree search)."""
    csv_path = Path(__file__).parent / data_dir / 'experiment1_results.csv'
    df = load_experiment1_data(csv_path)

    fig, ax = plt.subplots(1, 1, figsize=(10, 6))

    # Plot both search methods
    ax.plot(df['data_size_mb'], df['binary_search_throughput'],
            marker='s', markersize=8, linewidth=2,
            label='Binary Search', color='#1f77b4')
    ax.plot(df['data_size_mb'], df['btree_search_throughput'],
            marker='o', markersize=8, linewidth=2,
            label='B-Tree Search', color='#ff7f0e')

    ax.set_xlabel('Input Data Size (MB)', fontsize=12)
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12)
    ax.set_title('Search Operation Comparison (Throughput vs. Input data size)',
                 fontsize=13, fontweight='bold')
    ax.set_xscale('log')
    ax.set_yscale('log')

    # Set X-axis ticks - show all data points with actual MB values
    x_data_ticks = df['data_size_mb'].values
    ax.set_xticks(x_data_ticks)
    ax.set_xticklabels([f'{int(x)}' for x in x_data_ticks])
    ax.xaxis.set_minor_locator(ticker.NullLocator())

    # Format Y-axis to show all major ticks with human-friendly labels
    ax.yaxis.set_major_locator(ticker.LogLocator(base=10, numticks=12))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(human_readable_throughput))
    ax.yaxis.set_minor_locator(ticker.NullLocator())

    ax.grid(True, alpha=0.3, which='both')
    ax.legend(loc='best', fontsize=11)

    plt.tight_layout()
    output_path = Path(__file__).parent / data_dir / 'experiment1_visualization.png'
    plt.savefig(output_path, bbox_inches='tight')
    print(f"Saved Experiment 1 visualization to: {output_path}")
    plt.close()

def plot_experiment2(data_dir='results'):
    """Plot experiment 2: Throughput over time (insert, get, scan operations)."""
    csv_path = Path(__file__).parent / data_dir / 'experiment2_results.csv'
    df = load_experiment2_data(csv_path)

    # Create figure with 3 subplots
    fig, axes = plt.subplots(3, 1, figsize=(10, 12))
    fig.suptitle('Throughput Over Time (Insert, Get, and Scan Operations)',
                 fontsize=14, fontweight='bold', y=0.995)

    # Get data range for X-axis
    x_data_points = df['data_size_mb'].values

    # Plot 1: Insert operations (linear scale - throughput doesn't vary that much)
    axes[0].plot(df['data_size_mb'], df['insert_throughput'],
                 marker='s', markersize=6, linewidth=2,
                 label='Insert Throughput', color='#2ca02c')
    axes[0].set_xlabel('Input Data Size (MB)', fontsize=11)
    axes[0].set_ylabel('Throughput (ops/sec)', fontsize=11)
    axes[0].set_title('Insert Operation (Throughput vs. Input data size)',
                      fontsize=12, fontweight='bold')
    # Use linear scale for X-axis since data points are evenly spaced (100MB intervals)
    axes[0].set_xticks(x_data_points)
    axes[0].set_xticklabels([f'{int(x)}' for x in x_data_points], rotation=45, ha='right')
    # Use linear Y-axis since insert throughput is relatively stable
    axes[0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1e6:.2f}M' if x >= 1e6 else f'{x/1e3:.0f}K'))
    axes[0].grid(True, alpha=0.3, which='major')
    axes[0].legend(loc='best', fontsize=10)

    # Plot 2: Get operations (log Y-axis for better visibility of decline)
    axes[1].plot(df['data_size_mb'], df['get_throughput'],
                 marker='o', markersize=6, linewidth=2,
                 label='Get Throughput', color='#d62728')
    axes[1].set_xlabel('Input Data Size (MB)', fontsize=11)
    axes[1].set_ylabel('Throughput (ops/sec)', fontsize=11)
    axes[1].set_title('Get Operation (Throughput vs. Input data size)',
                      fontsize=12, fontweight='bold')
    # Use linear X-axis for consistency
    axes[1].set_xticks(x_data_points)
    axes[1].set_xticklabels([f'{int(x)}' for x in x_data_points], rotation=45, ha='right')
    # Use linear Y-axis but format nicely
    axes[1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1e3:.0f}K'))
    axes[1].grid(True, alpha=0.3, which='major')
    axes[1].legend(loc='best', fontsize=10)

    # Plot 3: Scan operations
    axes[2].plot(df['data_size_mb'], df['scan_throughput'],
                 marker='^', markersize=6, linewidth=2,
                 label='Scan Throughput', color='#9467bd')
    axes[2].set_xlabel('Input Data Size (MB)', fontsize=11)
    axes[2].set_ylabel('Throughput (ops/sec)', fontsize=11)
    axes[2].set_title('Scan Operation (Throughput vs. Input data size)',
                      fontsize=12, fontweight='bold')
    # Use linear X-axis for consistency
    axes[2].set_xticks(x_data_points)
    axes[2].set_xticklabels([f'{int(x)}' for x in x_data_points], rotation=45, ha='right')
    axes[2].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1e3:.0f}K'))
    axes[2].grid(True, alpha=0.3, which='major')
    axes[2].legend(loc='best', fontsize=10)

    plt.tight_layout()
    output_path = Path(__file__).parent / data_dir / 'experiment2_visualization.png'
    plt.savefig(output_path, bbox_inches='tight')
    print(f"Saved Experiment 2 visualization to: {output_path}")
    plt.close()

def plot_experiment2_combined(data_dir='results'):
    """Plot experiment 2: All operations on a single graph for comparison."""
    csv_path = Path(__file__).parent / data_dir / 'experiment2_results.csv'
    df = load_experiment2_data(csv_path)

    fig, ax = plt.subplots(1, 1, figsize=(10, 6))

    # Plot all three operations
    ax.plot(df['data_size_mb'], df['insert_throughput'],
            marker='s', markersize=6, linewidth=2,
            label='Insert', color='#2ca02c')
    ax.plot(df['data_size_mb'], df['get_throughput'],
            marker='o', markersize=6, linewidth=2,
            label='Get', color='#d62728')
    ax.plot(df['data_size_mb'], df['scan_throughput'],
            marker='^', markersize=6, linewidth=2,
            label='Scan', color='#9467bd')

    ax.set_xlabel('Input Data Size (MB)', fontsize=12)
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12)
    ax.set_title('All Operations Comparison (Throughput vs. Input data size)',
                 fontsize=13, fontweight='bold')

    # Get data points for X-axis
    x_data_points = df['data_size_mb'].values

    # Use linear X-axis since data points are evenly spaced (100MB intervals)
    ax.set_xticks(x_data_points)
    ax.set_xticklabels([f'{int(x)}' for x in x_data_points], rotation=45, ha='right')

    # Use log Y-axis to accommodate wide range between insert and get/scan
    ax.set_yscale('log')

    # Format Y-axis to show readable values
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, p: f'{x/1e6:.2f}M' if x >= 1e6 else (f'{x/1e3:.0f}K' if x >= 1e3 else f'{x:.0f}')
    ))

    ax.grid(True, alpha=0.3, which='both')
    ax.legend(loc='best', fontsize=11)

    plt.tight_layout()
    output_path = Path(__file__).parent / data_dir / 'experiment2_combined_visualization.png'
    plt.savefig(output_path, bbox_inches='tight')
    print(f"Saved Experiment 2 combined visualization to: {output_path}")
    plt.close()

def main():
    """Generate all visualizations."""
    print("Generating visualizations...")

    try:
        plot_experiment1()
        print("✓ Experiment 1 visualization created")
    except Exception as e:
        print(f"✗ Error creating Experiment 1 visualization: {e}")

    try:
        plot_experiment2()
        print("✓ Experiment 2 visualization created")
    except Exception as e:
        print(f"✗ Error creating Experiment 2 visualization: {e}")

    try:
        plot_experiment2_combined()
        print("✓ Experiment 2 combined visualization created")
    except Exception as e:
        print(f"✗ Error creating Experiment 2 combined visualization: {e}")

    print("\nVisualization generation complete!")

if __name__ == '__main__':
    main()
