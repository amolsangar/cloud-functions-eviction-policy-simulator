#!/usr/bin/python3
from AnalyzeResults import analyze_timings
import pickle
import os
import matplotlib.ticker as mticker

import matplotlib as mpl
mpl.rcParams.update({'font.size': 14})
mpl.use('Agg')
import matplotlib.pyplot as plt
import argparse


def plot_run(results, save_path):
    fig, ax = plt.subplots()
    ax2 = ax.twinx()
    
    plt.tight_layout()
    fig.set_size_inches(5,3)

    pts = sorted(results, key=lambda x: x[0])
    xs = [mem/1024 for mem, cold_pct, dropped_pct in pts]
    colds = [cold_pct for mem, cold_pct, dropped_pct in pts]
    drops = [dropped_pct for mem, cold_pct, dropped_pct in pts]

    ax.plot(xs, colds, color="blue")
    print("Cold %s", colds)

    ax2.plot(xs, drops, color="red")
    print("Dropped %s", drops)
    
    ax.set_ylabel("% Cold Starts", color="blue")
    ax.tick_params(axis='y', colors='blue')
    ax.set_ylim((0,None))

    ax2.set_ylabel("% Dropped", color="red")
    ax2.tick_params(axis='y', colors='red')
    ax2.set_ylim((0,None))
    ax.set_xlabel("System Memory (GB)")

    plt.savefig(save_path, bbox_inches="tight")
    plt.close(fig)

def get_info_from_file(filename):
    policy, num_funcs, mem, run = filename[:-5].split("-")
    return policy, int(num_funcs), int(mem), run

def load_data(path):
    with open(path, "r+b") as f:
        return pickle.load(f)

def plot_all(args):
    results = []
    num_funcs = args.numfuncs
    filt = "{}-{}-".format(args.policy, num_funcs)
    char_filt = "-{}.pckl".format(args.char)
    for file in os.listdir(args.pckldir):
        if filt in file and char_filt in file:
            policy, num_funcs, mem, run = get_info_from_file(file)
            pth = os.path.join(args.pckldir, file)
            policy, evdict, miss_stats, lambdas, capacity_misses, len_trace = load_data(pth)

            data = analyze_timings(policy, lambdas, miss_stats)
            total_misses = sum(capacity_misses.values())
            total_cold = 0
            total_warm = 0
            for func in data.keys():
              if func != "global":
                total_warm += data[func]["hits"]
                total_cold += data[func]["misses"]
            print(data["global"], total_misses, total_warm, total_cold, len_trace)

            cold_pct = data["global"]["server_cold"] *100
            dropped_pct = (total_misses / len_trace) * 100
            # print(file, "Cold starts %:", cold_pct, "; Dropped %:", dropped_pct)
            results.append((mem, cold_pct, dropped_pct))
    save_path = os.path.join(args.plotdir, "results-{}-{}-{}.png".format(args.policy, num_funcs, args.char))
    plot_run(results, save_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='plot FaasCache Simulation')
    parser.add_argument("--pckldir", type=str, default="/data/alfuerst/verify-test/", required=False)
    parser.add_argument("--plotdir", type=str, default="../figs/", required=False)
    parser.add_argument("--numfuncs", type=int, default=20, required=False)
    parser.add_argument("--char", type=str, default="a", required=False)
    parser.add_argument("--policy", type=str, default="RAND", required=False)
    
    args = parser.parse_args()
    plot_all(args)
