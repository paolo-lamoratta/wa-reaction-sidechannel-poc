#!/usr/bin/env python3
"""
Plot WhatsApp reaction delivery times from CSV - Multi-target support
Optimized for clear visualization with minimum alignment
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import sys
import glob
import os

def plot_delivery_times(csv_files):
    # If single string, convert to list
    if isinstance(csv_files, str):
        csv_files = [csv_files]
    
    # Load data from all files
    dfs = []
    labels = []
    # Distinct and vivid colors
    colors_map = ['#1f77b4', '#2ca02c', '#d62728', '#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    
    # Threshold for clipping
    threshold = 2000
    
    for i, csv_file in enumerate(csv_files):
        if not os.path.exists(csv_file):
            print(f"⚠️  File {csv_file} not found, skipping")
            continue
            
        df = pd.read_csv(csv_file)
        df['ack_time'] = pd.to_datetime(df['ack_timestamp']).dt.tz_localize(None) + pd.Timedelta(hours=0)
        
        # Clip values for visualization
        df['delivery_time_clipped'] = df['delivery_time_ms'].clip(upper=threshold)
        
        # Add label to identify target
        target_name = os.path.basename(csv_file).replace('.csv', '').replace('results_', '').replace('results', 'main')
        df['target'] = target_name
        df['color'] = colors_map[i % len(colors_map)]
        
        dfs.append(df)
        labels.append(target_name)
    
    if not dfs:
        print("❌ No valid CSV files found!")
        return
    
    # Concatenate all dataframes
    df_all = pd.concat(dfs, ignore_index=True)
    
    # Find the minimum value across all targets for alignment
    target_mins = {label: df['delivery_time_ms'].min() for df, label in zip(dfs, labels)}
    global_min = min(target_mins.values())
    
    # Calculate offset for each target to align minimums
    offsets = {}
    for df, label in zip(dfs, labels):
        target_min = df['delivery_time_ms'].min()
        offset = target_min - global_min
        offsets[label] = offset
        df['delivery_time_ms_aligned'] = df['delivery_time_ms'] - offset
        df['delivery_time_clipped_aligned'] = df['delivery_time_ms_aligned'].clip(upper=threshold)
    
    # Create figure with subplots
    num_targets = len(dfs)
    fig, axes = plt.subplots(2, 1, figsize=(20, 12), gridspec_kw={'height_ratios': [1, 2]})
    fig.suptitle(f'WhatsApp Reaction Delivery Times - {num_targets} Target(s) - Aligned by Minimum', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # ========== 1. HISTOGRAM - distribution (per target) ==========
    ax1 = axes[0]
    
    # Plot histogram for each target (aligned data)
    bins = np.linspace(0, threshold, 40)
    for i, (df, label) in enumerate(zip(dfs, labels)):
        color = colors_map[i % len(colors_map)]
        offset_str = f" (offset: -{offsets[label]:.0f}ms)" if offsets[label] > 0 else ""
        ax1.hist(df['delivery_time_clipped_aligned'], bins=bins, edgecolor='white', 
                alpha=0.6, color=color, label=f'{label}{offset_str}', linewidth=0.5)
    
    # Common minimum line
    ax1.axvline(x=global_min, color='black', linestyle='--', 
               linewidth=2, label=f"Common Minimum: {global_min:.0f}ms")
    
    ax1.set_xlabel('Delivery Time Aligned (ms)', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Frequency', fontsize=11, fontweight='bold')
    ax1.set_title(f'Distribution of Aligned Delivery Times (capped at {threshold}ms)', fontsize=12)
    ax1.set_xlim(0, threshold)
    ax1.legend(fontsize=9, loc='upper right', framealpha=0.9)
    ax1.grid(True, alpha=0.3, linestyle='-', axis='y')
    
    # Compact stats box
    stats_lines = []
    for label, df in zip(labels, dfs):
        med = df['delivery_time_ms'].median()
        stats_lines.append(f"{label}: min={target_mins[label]:.0f} med={med:.0f}")
    stats_text = " | ".join(stats_lines)
    ax1.text(0.5, 0.95, stats_text, transform=ax1.transAxes, fontsize=9, 
            verticalalignment='top', horizontalalignment='center',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.9), family='monospace')
    
    # ========== 2. TIME SERIES - aligned comparison ==========
    ax2 = axes[1]
    
    # Find global time range
    all_times = pd.concat([df['ack_time'] for df in dfs])
    time_min, time_max = all_times.min(), all_times.max()
    
    # Plot scatter with small transparent points to reveal patterns
    for i, (df, label) in enumerate(zip(dfs, labels)):
        color = colors_map[i % len(colors_map)]
        ax2.scatter(df['ack_time'], df['delivery_time_clipped_aligned'], 
                   c=color, alpha=0.25, s=8, label=f'{label}', rasterized=True)
    
    # Calculate and plot rolling average for each target
    for i, (df, label) in enumerate(zip(dfs, labels)):
        if len(df) < 10:
            continue
            
        color = colors_map[i % len(colors_map)]
        
        # Rolling average with window proportional to data size
        window = max(10, min(50, len(df) // 20))
        df_sorted = df.sort_values('ack_time').copy()
        df_sorted['rolling_avg'] = df_sorted['delivery_time_ms_aligned'].rolling(
            window=window, center=True, min_periods=5).mean()
        df_sorted['rolling_avg_clipped'] = df_sorted['rolling_avg'].clip(upper=threshold)
        
        ax2.plot(df_sorted['ack_time'], df_sorted['rolling_avg_clipped'], 
                color=color, linewidth=2.5, label=f'{label} avg (w={window})', 
                zorder=10, alpha=0.9)
    
    # Common minimum line
    ax2.axhline(y=global_min, color='black', linestyle='--', 
               linewidth=1.5, alpha=0.8, label=f"Min: {global_min:.0f}ms")
    
    # Colored zones for threshold
    ax2.axhspan(0, global_min + 100, alpha=0.06, color='green')
    ax2.axhspan(1500, threshold, alpha=0.06, color='red')
    
    ax2.set_xlabel('Time', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Delivery Time Aligned (ms)', fontsize=11, fontweight='bold')
    ax2.set_title(f'Delivery Time Over Time - Aligned by Minimum (capped at {threshold}ms)', fontsize=12)
    ax2.set_ylim(0, threshold)
    ax2.set_xlim(time_min, time_max)
    
    # Compact legend outside
    ax2.legend(fontsize=8, loc='upper right', ncol=2, framealpha=0.9)
    ax2.grid(True, alpha=0.3, linestyle='-')
    
    # Format x-axis for time display
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    # Dynamic interval based on duration
    duration_minutes = (time_max - time_min).total_seconds() / 60
    if duration_minutes > 30:
        ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
    elif duration_minutes > 10:
        ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=2))
    else:
        ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    
    plt.xticks(rotation=30, ha='right')
    plt.tight_layout()
    
    # Save
    if len(csv_files) == 1:
        output_file = csv_files[0].replace('.csv', '_plot.png')
    else:
        output_file = 'results_multi_target_plot.png'
    
    plt.savefig(output_file, dpi=150, bbox_inches='tight', facecolor='white')
    print(f"📊 Plot saved to: {output_file}")
    
    # Print summary statistics per target
    print("\n" + "="*70)
    print("SUMMARY STATISTICS BY TARGET (with alignment info)")
    print("="*70)
    
    for df, label in zip(dfs, labels):
        offset = offsets[label]
        print(f"\n📱 {label.upper()}:")
        print(f"  Samples: {len(df)}")
        print(f"  Original Min: {target_mins[label]:.0f}ms → Aligned to: {global_min:.0f}ms (offset: -{offset:.0f}ms)")
        print(f"  Mean: {df['delivery_time_ms'].mean():.1f}ms | Median: {df['delivery_time_ms'].median():.1f}ms")
        print(f"  Std Dev: {df['delivery_time_ms'].std():.1f}ms")
        print(f"  Max: {df['delivery_time_ms'].max():.0f}ms")
        
        slow = (df['delivery_time_ms'] > 1000).sum()
        very_slow = (df['delivery_time_ms'] > threshold).sum()
        print(f"  >1000ms: {slow} ({slow/len(df)*100:.1f}%) | >{threshold}ms (outliers): {very_slow} ({very_slow/len(df)*100:.1f}%)")
    
    print("\n" + "="*70)
    print("ALIGNMENT SUMMARY")
    print("="*70)
    print(f"Global minimum (baseline): {global_min:.0f}ms")
    for label in labels:
        print(f"  {label}: original min = {target_mins[label]:.0f}ms, offset = -{offsets[label]:.0f}ms")
    print("="*70 + "\n")

if __name__ == "__main__":
    # Support for single file or multiple
    if len(sys.argv) > 1:
        if '*' in sys.argv[1]:
            csv_files = glob.glob(sys.argv[1])
        else:
            csv_files = sys.argv[1:]
    else:
        csv_files = glob.glob("results*.csv")
        if not csv_files:
            csv_files = ["results.csv"]
    
    plot_delivery_times(csv_files)
