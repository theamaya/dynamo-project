#!/usr/bin/env python3
"""
Visualization script for Dynamo experiment results.
Aggregates results from all experiment suites and generates plots.
"""

import json
import os
import glob
import argparse
from pathlib import Path
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional

def load_experiment_data(results_dir: str = "results") -> List[Dict[str, Any]]:
    """
    Load all experiment results from the results directory.
    Returns a list of dictionaries containing both config and metrics.
    """
    experiments = []
    results_path = Path(results_dir)
    
    # Find all computed_metrics.json files
    for metrics_file in results_path.rglob("computed_metrics.json"):
        exp_dir = metrics_file.parent
        
        # Load metrics
        try:
            with open(metrics_file) as f:
                metrics = json.load(f)
        except Exception as e:
            print(f"Warning: Could not load {metrics_file}: {e}")
            continue
        
        # Load config
        config_file = exp_dir / "experiment_config.json"
        config = {}
        if config_file.exists():
            try:
                with open(config_file) as f:
                    config = json.load(f)
            except Exception:
                pass
        
        # Extract experiment name from directory
        exp_name = exp_dir.name
        suite_name = exp_dir.parent.name
        
        # Combine into single record
        exp_data = {
            "suite": suite_name,
            "experiment": exp_name,
            "path": str(exp_dir),
            **config,
            **metrics
        }
        experiments.append(exp_data)
    
    return experiments

def plot_availability_vs_crash_count(experiments: List[Dict], output_dir: str = "plots"):
    """Plot availability vs crash count for different configurations, separated by RF and concurrency."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter experiments with crash_count
    crash_experiments = [e for e in experiments if e.get("crash_count") is not None and e.get("failure_mode") == "crash"]
    
    if not crash_experiments:
        print("No crash count experiments found")
        return
    
    # Group by replication_factor first, then by nodes, read_quorum_r, write_quorum_w, concurrency
    rf_values = sorted(set(e.get("replication_factor", 0) for e in crash_experiments))
    
    for rf in rf_values:
        rf_experiments = [e for e in crash_experiments if e.get("replication_factor") == rf]
        if not rf_experiments:
            continue
        
        # Get unique concurrency values
        concurrency_values = sorted(set(e.get("concurrency", 10) for e in rf_experiments))
        
        for concurrency in concurrency_values:
            conc_experiments = [e for e in rf_experiments if e.get("concurrency", 10) == concurrency]
            if not conc_experiments:
                continue
            
            # Group by nodes, read_quorum_r, write_quorum_w
            nodes_list = sorted(set(e.get("nodes", 0) for e in conc_experiments))
            r_values = sorted(set(e.get("read_quorum_r", 0) for e in conc_experiments))
            w_values = sorted(set(e.get("write_quorum_w", 0) for e in conc_experiments))
            
            # Create subplots for each (nodes, r, w) combination
            num_plots = len(nodes_list) * len(r_values) * len(w_values)
            if num_plots == 0:
                continue
            
            cols = min(3, num_plots)
            rows = (num_plots + cols - 1) // cols
            fig, axes = plt.subplots(rows, cols, figsize=(5*cols, 4*rows))
            if num_plots == 1:
                axes = [axes]
            else:
                axes = axes.flatten()
            
            plot_idx = 0
            for nodes in nodes_list:
                for r in r_values:
                    for w in w_values:
                        subset = [e for e in conc_experiments 
                                 if e.get("nodes") == nodes and 
                                 e.get("read_quorum_r") == r and 
                                 e.get("write_quorum_w") == w]
                        
                        if not subset or plot_idx >= len(axes):
                            continue
                        
                        ax = axes[plot_idx]
                        crash_counts = sorted(set(e.get("crash_count", 0) for e in subset))
                        overall_avail = []
                        read_avail = []
                        write_avail = []
                        
                        for cc in crash_counts:
                            exp = next((e for e in subset if e.get("crash_count") == cc), None)
                            if exp and "availability" in exp:
                                overall_avail.append(exp["availability"].get("overall", 0))
                                read_avail.append(exp["availability"].get("reads", 0))
                                write_avail.append(exp["availability"].get("writes", 0))
                            else:
                                overall_avail.append(0)
                                read_avail.append(0)
                                write_avail.append(0)
                        
                        ax.plot(crash_counts, overall_avail, 'o-', label='Overall', linewidth=2, markersize=8)
                        ax.plot(crash_counts, read_avail, 's-', label='Reads', linewidth=2, markersize=8)
                        ax.plot(crash_counts, write_avail, '^-', label='Writes', linewidth=2, markersize=8)
                        ax.set_xlabel('Crash Count', fontsize=10)
                        ax.set_ylabel('Availability', fontsize=10)
                        ax.set_title(f'N={nodes}, R={r}, W={w}', fontsize=11, fontweight='bold')
                        ax.set_ylim([0, 1.05])
                        ax.grid(True, alpha=0.3)
                        if plot_idx == 0:
                            ax.legend(fontsize=8)
                        plot_idx += 1
            
            # Hide unused subplots
            for i in range(plot_idx, len(axes)):
                axes[i].set_visible(False)
            
            plt.suptitle(f'Availability vs Crash Count (RF={rf}, Concurrency={concurrency})', 
                        fontsize=14, fontweight='bold', y=1.0)
            plt.tight_layout()
            filename = f"{output_dir}/availability_vs_crash_count_rf{rf}_c{concurrency}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"Saved: {filename}")
            plt.close()

def plot_availability_vs_quorum(experiments: List[Dict], output_dir: str = "plots"):
    """Plot availability vs quorum values for different failure modes, separated by RF and concurrency."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter experiments with quorum values
    quorum_experiments = [e for e in experiments 
                         if e.get("read_quorum_r") is not None and 
                         e.get("write_quorum_w") is not None]
    
    if not quorum_experiments:
        print("No quorum experiments found")
        return
    
    # Group by replication_factor first
    rf_values = sorted(set(e.get("replication_factor", 0) for e in quorum_experiments))
    failure_modes = sorted(set(e.get("failure_mode", "none") for e in quorum_experiments))
    nodes_list = sorted(set(e.get("nodes", 0) for e in quorum_experiments))
    concurrency_values = sorted(set(e.get("concurrency", 10) for e in quorum_experiments))
    
    for rf in rf_values:
        rf_experiments = [e for e in quorum_experiments if e.get("replication_factor") == rf]
        if not rf_experiments:
            continue
        
        for concurrency in concurrency_values:
            conc_experiments = [e for e in rf_experiments if e.get("concurrency", 10) == concurrency]
            if not conc_experiments:
                continue
            
            for nodes in nodes_list:
                subset = [e for e in conc_experiments if e.get("nodes") == nodes]
                if not subset:
                    continue
                
                fig, axes = plt.subplots(1, len(failure_modes), figsize=(6*len(failure_modes), 5))
                if len(failure_modes) == 1:
                    axes = [axes]
                
                for idx, failure_mode in enumerate(failure_modes):
                    ax = axes[idx]
                    mode_subset = [e for e in subset if e.get("failure_mode") == failure_mode]
                    
                    if not mode_subset:
                        ax.set_visible(False)
                        continue
                    
                    # Create heatmap data
                    r_values = sorted(set(e.get("read_quorum_r", 0) for e in mode_subset))
                    w_values = sorted(set(e.get("write_quorum_w", 0) for e in mode_subset))
                    
                    heatmap_data = np.zeros((len(r_values), len(w_values)))
                    for i, r in enumerate(r_values):
                        for j, w in enumerate(w_values):
                            exp = next((e for e in mode_subset 
                                       if e.get("read_quorum_r") == r and e.get("write_quorum_w") == w), None)
                            if exp and "availability" in exp:
                                heatmap_data[i, j] = exp["availability"].get("overall", 0)
                    
                    im = ax.imshow(heatmap_data, cmap='RdYlGn', aspect='auto', vmin=0, vmax=1)
                    ax.set_xticks(range(len(w_values)))
                    ax.set_xticklabels([f'W={w}' for w in w_values])
                    ax.set_yticks(range(len(r_values)))
                    ax.set_yticklabels([f'R={r}' for r in r_values])
                    ax.set_xlabel('Write Quorum (W)', fontsize=11)
                    ax.set_ylabel('Read Quorum (R)', fontsize=11)
                    ax.set_title(f'{failure_mode.capitalize()} (N={nodes})', fontsize=12, fontweight='bold')
                    
                    # Add colorbar
                    plt.colorbar(im, ax=ax, label='Availability')
                    
                    # Add text annotations
                    for i in range(len(r_values)):
                        for j in range(len(w_values)):
                            text = ax.text(j, i, f'{heatmap_data[i, j]:.2f}',
                                         ha="center", va="center", color="black", fontweight='bold')
                
                plt.suptitle(f'Availability vs Quorum (RF={rf}, Concurrency={concurrency})', 
                            fontsize=14, fontweight='bold')
                plt.tight_layout()
                filename = f"{output_dir}/availability_quorum_heatmap_rf{rf}_n{nodes}_c{concurrency}.png"
                plt.savefig(filename, dpi=300, bbox_inches='tight')
                print(f"Saved: {filename}")
                plt.close()

def plot_consistency_metrics(experiments: List[Dict], output_dir: str = "plots"):
    """Plot consistency metrics across experiments, separated by RF and concurrency."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter experiments with consistency data
    consistency_experiments = [e for e in experiments if "consistency" in e]
    
    if not consistency_experiments:
        print("No consistency data found")
        return
    
    # Group by replication_factor
    rf_values = sorted(set(e.get("replication_factor", 0) for e in consistency_experiments))
    failure_modes = sorted(set(e.get("failure_mode", "none") for e in consistency_experiments))
    concurrency_values = sorted(set(e.get("concurrency", 10) for e in consistency_experiments))
    
    metrics_to_plot = [
        ("ryw_violations", "Read-Your-Write Violations"),
        ("monotonic_read_violations", "Monotonic Read Violations"),
        ("stale_read_rate", "Stale Read Rate"),
        ("multi_version_reads", "Multi-Version Reads")
    ]
    
    for rf in rf_values:
        rf_experiments = [e for e in consistency_experiments if e.get("replication_factor") == rf]
        if not rf_experiments:
            continue
        
        for concurrency in concurrency_values:
            conc_experiments = [e for e in rf_experiments if e.get("concurrency", 10) == concurrency]
            if not conc_experiments:
                continue
            
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            for idx, (metric_key, metric_name) in enumerate(metrics_to_plot):
                if idx >= len(axes):
                    break
                
                ax = axes[idx]
                data_by_mode = {mode: [] for mode in failure_modes}
                
                for exp in conc_experiments:
                    mode = exp.get("failure_mode", "none")
                    if "consistency" in exp and metric_key in exp["consistency"]:
                        value = exp["consistency"][metric_key]
                        if isinstance(value, (int, float)):
                            data_by_mode[mode].append(value)
                
                # Create box plot
                positions = range(len(failure_modes))
                box_data = [data_by_mode[mode] for mode in failure_modes if data_by_mode[mode]]
                box_labels = [mode for mode in failure_modes if data_by_mode[mode]]
                
                if box_data:
                    bp = ax.boxplot(box_data, labels=box_labels, patch_artist=True)
                    for patch in bp['boxes']:
                        patch.set_facecolor('lightblue')
                    ax.set_ylabel(metric_name, fontsize=11)
                    ax.set_xlabel('Failure Mode', fontsize=11)
                    ax.set_title(metric_name, fontsize=12, fontweight='bold')
                    ax.grid(True, alpha=0.3, axis='y')
            
            plt.suptitle(f'Consistency Metrics (RF={rf}, Concurrency={concurrency})', 
                        fontsize=14, fontweight='bold')
            plt.tight_layout()
            filename = f"{output_dir}/consistency_metrics_rf{rf}_c{concurrency}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"Saved: {filename}")
            plt.close()

def plot_availability_comparison(experiments: List[Dict], output_dir: str = "plots"):
    """Compare availability across different failure modes and configurations, separated by RF and concurrency."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter experiments with availability data
    avail_experiments = [e for e in experiments if "availability" in e]
    
    if not avail_experiments:
        print("No availability data found")
        return
    
    # Group by replication_factor first
    rf_values = sorted(set(e.get("replication_factor", 0) for e in avail_experiments))
    nodes_list = sorted(set(e.get("nodes", 0) for e in avail_experiments))
    failure_modes = sorted(set(e.get("failure_mode", "none") for e in avail_experiments))
    concurrency_values = sorted(set(e.get("concurrency", 10) for e in avail_experiments))
    
    for rf in rf_values:
        rf_experiments = [e for e in avail_experiments if e.get("replication_factor") == rf]
        if not rf_experiments:
            continue
        
        for concurrency in concurrency_values:
            conc_experiments = [e for e in rf_experiments if e.get("concurrency", 10) == concurrency]
            if not conc_experiments:
                continue
            
            for nodes in nodes_list:
                subset = [e for e in conc_experiments if e.get("nodes") == nodes]
                
                if not subset:
                    continue
                
                fig, ax = plt.subplots(figsize=(12, 6))
                
                # Create grouped bar chart
                r_values = sorted(set(e.get("read_quorum_r", 0) for e in subset))
                w_values = sorted(set(e.get("write_quorum_w", 0) for e in subset))
                
                x = np.arange(len(r_values) * len(w_values))
                width = 0.15
                multiplier = 0
                
                for mode in failure_modes:
                    mode_subset = [e for e in subset if e.get("failure_mode") == mode]
                    offsets = []
                    values = []
                    
                    for r in r_values:
                        for w in w_values:
                            exp = next((e for e in mode_subset 
                                       if e.get("read_quorum_r") == r and e.get("write_quorum_w") == w), None)
                            if exp and "availability" in exp:
                                values.append(exp["availability"].get("overall", 0))
                            else:
                                values.append(0)
                            offsets.append(f"R{r}W{w}")
                    
                    if values:
                        ax.bar(x + multiplier * width, values, width, label=mode.capitalize())
                        multiplier += 1
                
                ax.set_xlabel('Quorum Configuration (R=Read, W=Write)', fontsize=11)
                ax.set_ylabel('Availability', fontsize=11)
                ax.set_title(f'Availability Comparison (RF={rf}, N={nodes}, Concurrency={concurrency})', 
                            fontsize=12, fontweight='bold')
                ax.set_xticks(x + width * (len(failure_modes) - 1) / 2)
                ax.set_xticklabels(offsets, rotation=45, ha='right')
                ax.set_ylim([0, 1.05])
                ax.legend()
                ax.grid(True, alpha=0.3, axis='y')
                
                plt.tight_layout()
                filename = f"{output_dir}/availability_comparison_rf{rf}_n{nodes}_c{concurrency}.png"
                plt.savefig(filename, dpi=300, bbox_inches='tight')
                print(f"Saved: {filename}")
                plt.close()

def plot_failure_window_analysis(experiments: List[Dict], output_dir: str = "plots"):
    """Plot availability during different time windows (warmup, failure, recovery)."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter experiments with window data
    window_experiments = [e for e in experiments 
                         if "availability_by_window" in e and e.get("failure_mode") != "none"]
    
    if not window_experiments:
        print("No window data found")
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    windows = ["warmup", "failure"]
    window_labels = ["Warmup", "During Failure"]
    
    for idx, (window, label) in enumerate(zip(windows, window_labels)):
        ax = axes[idx]
        
        # Group by failure mode
        failure_modes = sorted(set(e.get("failure_mode", "none") for e in window_experiments))
        data_by_mode = {mode: [] for mode in failure_modes}
        
        for exp in window_experiments:
            mode = exp.get("failure_mode", "none")
            if "availability_by_window" in exp and window in exp["availability_by_window"]:
                avail = exp["availability_by_window"][window].get("overall", 0)
                data_by_mode[mode].append(avail)
        
        # Create box plot
        positions = range(len(failure_modes))
        box_data = [data_by_mode[mode] for mode in failure_modes if data_by_mode[mode]]
        box_labels = [mode for mode in failure_modes if data_by_mode[mode]]
        
        if box_data:
            bp = ax.boxplot(box_data, labels=box_labels, patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightcoral' if window == "failure" else 'lightgreen')
            ax.set_ylabel('Availability', fontsize=11)
            ax.set_xlabel('Failure Mode', fontsize=11)
            ax.set_title(f'Availability During {label}', fontsize=12, fontweight='bold')
            ax.set_ylim([0, 1.05])
            ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/failure_window_analysis.png", dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir}/failure_window_analysis.png")
    plt.close()

def plot_concurrency_effects(experiments: List[Dict], output_dir: str = "plots"):
    """Plot availability vs concurrency for different configurations."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter experiments with availability and concurrency data
    avail_experiments = [e for e in experiments 
                        if "availability" in e and e.get("concurrency") is not None]
    
    if not avail_experiments:
        print("No concurrency experiments found")
        return
    
    # Group by replication_factor, nodes, failure_mode
    rf_values = sorted(set(e.get("replication_factor", 0) for e in avail_experiments))
    nodes_list = sorted(set(e.get("nodes", 0) for e in avail_experiments))
    failure_modes = sorted(set(e.get("failure_mode", "none") for e in avail_experiments))
    
    for rf in rf_values:
        rf_experiments = [e for e in avail_experiments if e.get("replication_factor") == rf]
        if not rf_experiments:
            continue
        
        for nodes in nodes_list:
            node_experiments = [e for e in rf_experiments if e.get("nodes") == nodes]
            if not node_experiments:
                continue
            
            # Create subplot for each failure mode
            fig, axes = plt.subplots(1, len(failure_modes), figsize=(6*len(failure_modes), 5))
            if len(failure_modes) == 1:
                axes = [axes]
            
            for idx, failure_mode in enumerate(failure_modes):
                ax = axes[idx]
                mode_experiments = [e for e in node_experiments if e.get("failure_mode") == failure_mode]
                
                if not mode_experiments:
                    ax.set_visible(False)
                    continue
                
                # Group by quorum configuration
                r_values = sorted(set(e.get("read_quorum_r", 0) for e in mode_experiments))
                w_values = sorted(set(e.get("write_quorum_w", 0) for e in mode_experiments))
                concurrency_values = sorted(set(e.get("concurrency", 10) for e in mode_experiments))
                
                for r in r_values:
                    for w in w_values:
                        subset = [e for e in mode_experiments 
                                 if e.get("read_quorum_r") == r and e.get("write_quorum_w") == w]
                        
                        if not subset:
                            continue
                        
                        conc_sorted = sorted(set(e.get("concurrency", 10) for e in subset))
                        avail_values = []
                        
                        for conc in conc_sorted:
                            exp = next((e for e in subset if e.get("concurrency", 10) == conc), None)
                            if exp and "availability" in exp:
                                avail_values.append(exp["availability"].get("overall", 0))
                            else:
                                avail_values.append(0)
                        
                        if avail_values:
                            ax.plot(conc_sorted, avail_values, 'o-', 
                                   label=f'R={r}, W={w}', linewidth=2, markersize=8)
                
                ax.set_xlabel('Concurrency', fontsize=11)
                ax.set_ylabel('Availability', fontsize=11)
                ax.set_title(f'{failure_mode.capitalize()} (RF={rf}, N={nodes})', fontsize=12, fontweight='bold')
                ax.set_ylim([0, 1.05])
                ax.grid(True, alpha=0.3)
                ax.legend(fontsize=8)
            
            plt.tight_layout()
            filename = f"{output_dir}/availability_vs_concurrency_rf{rf}_n{nodes}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"Saved: {filename}")
            plt.close()

def generate_summary_table(experiments: List[Dict], output_dir: str = "plots"):
    """Generate a summary table of all experiments, including RF and concurrency."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Create DataFrame
    rows = []
    for exp in experiments:
        row = {
            "Suite": exp.get("suite", "unknown"),
            "Nodes": exp.get("nodes", "?"),
            "RF": exp.get("replication_factor", "?"),
            "R": exp.get("read_quorum_r", "?"),
            "W": exp.get("write_quorum_w", "?"),
            "Failure": exp.get("failure_mode", "?"),
            "Crash Count": exp.get("crash_count", "?"),
            "Concurrency": exp.get("concurrency", "?"),
            "Overall Avail": exp.get("availability", {}).get("overall", 0) if "availability" in exp else 0,
            "Read Avail": exp.get("availability", {}).get("reads", 0) if "availability" in exp else 0,
            "Write Avail": exp.get("availability", {}).get("writes", 0) if "availability" in exp else 0,
            "RYW Violations": exp.get("consistency", {}).get("ryw_violations", 0) if "consistency" in exp else 0,
            "Stale Read Rate": exp.get("consistency", {}).get("stale_read_rate", 0) if "consistency" in exp else 0,
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df = df.sort_values(by=["RF", "Nodes", "R", "W", "Failure", "Crash Count", "Concurrency"])
    
    # Save to CSV
    csv_path = f"{output_dir}/experiment_summary.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved: {csv_path}")
    
    # Save formatted table
    html_path = f"{output_dir}/experiment_summary.html"
    df.to_html(html_path, index=False, float_format=lambda x: f'{x:.4f}' if isinstance(x, float) else str(x))
    print(f"Saved: {html_path}")
    
    return df

def main():
    parser = argparse.ArgumentParser(description="Visualize Dynamo experiment results")
    parser.add_argument("--results-dir", type=str, default="results", 
                       help="Directory containing experiment results")
    parser.add_argument("--output-dir", type=str, default="plots",
                       help="Directory to save plots")
    parser.add_argument("--plots", nargs="+", 
                       choices=["all", "crash_count", "quorum", "consistency", "comparison", "windows", "concurrency", "summary"],
                       default=["all"],
                       help="Which plots to generate")
    
    args = parser.parse_args()
    
    print(f"Loading experiments from {args.results_dir}...")
    experiments = load_experiment_data(args.results_dir)
    print(f"Loaded {len(experiments)} experiments")
    
    if not experiments:
        print("No experiments found!")
        return
    
    plots_to_generate = args.plots
    if "all" in plots_to_generate:
        plots_to_generate = ["crash_count", "quorum", "consistency", "comparison", "windows", "concurrency", "summary"]
    
    if "crash_count" in plots_to_generate:
        print("\nGenerating crash count plots (separated by RF and concurrency)...")
        plot_availability_vs_crash_count(experiments, args.output_dir)
    
    if "quorum" in plots_to_generate:
        print("\nGenerating quorum heatmaps (separated by RF and concurrency)...")
        plot_availability_vs_quorum(experiments, args.output_dir)
    
    if "consistency" in plots_to_generate:
        print("\nGenerating consistency plots...")
        plot_consistency_metrics(experiments, args.output_dir)
    
    if "comparison" in plots_to_generate:
        print("\nGenerating availability comparison plots (separated by RF and concurrency)...")
        plot_availability_comparison(experiments, args.output_dir)
    
    if "windows" in plots_to_generate:
        print("\nGenerating failure window analysis...")
        plot_failure_window_analysis(experiments, args.output_dir)
    
    if "concurrency" in plots_to_generate:
        print("\nGenerating concurrency effect plots...")
        plot_concurrency_effects(experiments, args.output_dir)
    
    if "summary" in plots_to_generate:
        print("\nGenerating summary table (including RF and concurrency)...")
        generate_summary_table(experiments, args.output_dir)
    
    print(f"\nAll plots saved to {args.output_dir}/")

if __name__ == "__main__":
    main()

