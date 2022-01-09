#!/bin/bash

trace_dir="../traces"
trace_output_dir="../output"
log_dir="$trace_output_dir/logs"
memory_dir="$trace_output_dir/memory"
plot_dir="./figs/"
num_funcs=100
char='b'
policy="CLOUD21"

# run simulation
python ./sim/ParallelRunner.py --tracedir $trace_dir --numfuncs $num_funcs --savedir $trace_output_dir --logdir $log_dir --char $char --policy $policy  --mem 7500 --mem 10000 --mem 11000 --mem 13000 --mem 15000 --mem 5000 --mem 3000

# plot graphs
python ./analyze/PlotResults.py --pckldir $trace_output_dir --plotdir $plot_dir --numfuncs $num_funcs --char $char --policy $policy

