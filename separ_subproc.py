import pandas as pd
import os, shutil

def separ_subproc(subproc_evts, non_subproc_evts, parent_col, subactiv_col, non_subactiv_col, dir_subproc, path_log):
    # create groups based on parent column
    # (we can just combine all cases here per activity)
    labeled_logs = [ (g, df) for g, df in subproc_evts.groupby(parent_col) ]

    shutil.rmtree(dir_subproc)
    os.mkdir(dir_subproc)
    os.mkdir(os.path.join(dir_subproc, "logs"))

    # per activity,
    for label, sublog in labeled_logs:        
        print(f"{label} (# events: {sublog.shape[0]})")

        # the subactivity column will be the activity name here
        if subactiv_col != 'concept:name':
            sublog = sublog.rename({subactiv_col: 'concept:name'}, axis=1)

        sublog = sublog.sort_values(by=['case:concept:name', 'time:timestamp'])
        # store log
        sublog.to_csv(os.path.join(dir_subproc, "logs", f"{label.replace('/', '_')}.csv"), index=False)
        
    # per case, per subprocess, replace all sub-events by single start & end event
    sorted_grouped = subproc_evts.sort_values(['case:concept:name', parent_col, 'time:timestamp']).groupby(['case:concept:name', parent_col])
    start_evts = sorted_grouped.first().reset_index(); start_evts['concept:name'] = start_evts[parent_col] + ' [start]'
    end_evts = sorted_grouped.last().reset_index(); end_evts['concept:name'] = end_evts[parent_col] + ' [end]'
    abstract_log = pd.concat([start_evts, end_evts])
    
    # re-add the non-subprocess activities
    non_subproc_evts['concept:name'] = non_subproc_evts[non_subactiv_col]
    abstract_log = pd.concat([ abstract_log, non_subproc_evts ], ignore_index=True).sort_values(by=['case:concept:name','time:timestamp'])
    abstract_log.to_csv(path_log, index=False)
    
    return abstract_log