from enum import Enum
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.algo.discovery.ilp import algorithm as ilp_miner
from pm4py.visualization.dfg import visualizer as dfg_visualizer
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
from pm4py.visualization.heuristics_net import visualizer as hn_visualizer
from pm4py.visualization.process_tree import visualizer as pt_visualizer
from pm4py.objects.conversion.process_tree import converter as process_tree_converter
from pm4py.visualization.heuristics_net.variants.pydotplus_vis import get_graph as hn_get_graph
import pandas as pd

class ProcAnn(Enum):
    FREQ = "frequency"
    PERF = "performance"

def mine_vis(visualizer, gviz, output_path, save_gviz=False):
    if output_path is not None:
        visualizer.save(gviz, f"{output_path}.jpg")
        if save_gviz:
            gviz.save(f"{output_path}.gv")
    else:
        visualizer.view(gviz)

def mine_dfg(log, ann=ProcAnn.FREQ, output_path=None, save_gviz=False):
    match ann:
        case ProcAnn.FREQ:
            mine_var = dfg_discovery.Variants.FREQUENCY
            vis_var = dfg_visualizer.Variants.FREQUENCY
        case ProcAnn.PERF:
            mine_var = dfg_discovery.Variants.PERFORMANCE
            vis_var = dfg_visualizer.Variants.PERFORMANCE
    
    # discover
    dfg = dfg_discovery.apply(log, variant=mine_var)

    # visualize
    gviz = dfg_visualizer.apply(dfg, log=log, variant=vis_var)
    mine_vis(dfg_visualizer, gviz, output_path, save_gviz)
        

def mine_alpha(log, output_path=None, save_gviz=False):
    # alpha miner
    net, initial_marking, final_marking = alpha_miner.apply(log)

    # visualise
    gviz = pn_visualizer.apply(net, initial_marking, final_marking)
    mine_vis(pn_visualizer, gviz, output_path, save_gviz)
    

def mine_heur(log, ann=ProcAnn.FREQ, output_path=None, save_gviz=False):
    # heuristics miner
    heu_net = heuristics_miner.apply_heu(log, { "heu_net_decoration": ann.value })

    # visualize
    # (works differently ...)
    if save_gviz and output_path is not None:
        gviz = hn_get_graph(heu_net)
        gviz.write(f"{output_path}.gv")
    
    gviz = hn_visualizer.apply(heu_net)
    mine_vis(hn_visualizer, gviz, output_path, False)
    
    
def mine_induct(log, convert_to=None, output_path=None, save_gviz=False):
    # create the process tree
    # (wvw: drop "_tree" from call)
    tree = inductive_miner.apply(log)

    if convert_to is not None:
        match (convert_to):
            case 'petri_net': 
                net, initial_marking, final_marking = process_tree_converter.apply(tree)
                gviz = pn_visualizer.apply(net, initial_marking, final_marking)
            case 'bpmn':
                bpmn = process_tree_converter.apply(tree, variant=process_tree_converter.Variants.TO_BPMN)
                gviz = bpmn_visualizer.apply(bpmn)
            case _:
                raise f"Unsupported format: {convert_to}"
        
        mine_vis(pn_visualizer, gviz, output_path, save_gviz)
    else:
        gviz = pt_visualizer.apply(tree)
        mine_vis(pt_visualizer, gviz, output_path, save_gviz)
        
        
def mine_ilp(log, output_path=None, save_gviz=False):
    # heuristics miner
    net, init_mark, final_mark = ilp_miner.apply(log)

    # visualize
    gviz = pn_visualizer.apply(net, init_mark, final_mark)
    mine_vis(pn_visualizer, gviz, output_path, save_gviz)
    
    
def log_subset_horizontal(log, perc):
    new_len = int(log.shape[0] * perc)
    new_log = log.iloc[0:new_len]
    
    # (ensures that full cases are extracted)
    cutoff_case = log.iloc[new_len-1]['case:concept:name']
    copied_len = new_log[new_log['case:concept:name']==cutoff_case].shape[0]
    total_len = log[log['case:concept:name']==cutoff_case].shape[0]

    if copied_len != total_len:
        new_len = new_len + (total_len - copied_len)
        new_log = log.iloc[0:new_len]

    return new_log


def log_subset_vertical(log, perc):
    print("original:")
    counts = log.groupby('case:concept:name')['concept:name'].count()
    print(counts.describe())
    
    case_logs = [ df for _, df in log.groupby('case:concept:name') ]
    case_logs = map(lambda df: df.iloc[0:int(df.shape[0]*perc)], case_logs)
    
    log_subset = pd.concat(case_logs)
    
    print("\nsubset:")
    counts = log_subset.groupby('case:concept:name')['concept:name'].count()
    print(counts.describe())
    
    return log_subset

def aggregate_events(log, events, max_timedelta, repl=None, verbose=False):
    # log = log.copy()
    
    # - uses loops but more robust
    
    total_groups = 0; total_size = 0
    total_simult = 0; total_diff = 0; non_simul = []
    # activ_orders = {
    #     (events[0], events[-1]): 0, 
    #     (events[-1], events[0]): 0, 
    # }
    
    class Group:        
        def __init__(self):
            self.events = []
            self.idxes = []
        def add(self, event):
            self.events.append(event['concept:name'])
            self.idxes.append(event.name)
        def includes(self, evt_name):
            return evt_name in self.events
        def indexes(self):
            return self.idxes
        def size(self):
            return len(self.events)
        def __str__(self):
            return " (" + ", ".join([ f"'{evt}'@{idx}" for evt, idx in zip(self.events, self.idxes) ]) + ") "
    
    class Case:        
        def __init__(self, id=None, verbose=False):
            self.id = id
            self.verbose = verbose;
            if verbose: # only keep groups if verbose
                self.groups = []
        def add(self, group):
            # pass
            if self.verbose:
                self.groups.append(group)
        def size(self):
            if self.verbose:
                return len(self.groups)
            else:
                return 0
        def __str__(self):
            return f"case {self.id}: " + "; ".join(map(lambda g: g.__str__(), self.groups))
    
    to_drop = []
    cur_case = Case(verbose=verbose); cur_group = Group()
    
    def record_case(cur_case, cur_group):
        # deal with leftover (incomplete) group
        cur_group = record_group(cur_case, cur_group) if cur_group.size() > 0 else Group()
        # (testing) print non-empty cases
        if verbose: # and cur_case.size() > 0:
            print(cur_case)
        cur_case = Case(verbose=verbose)
        return cur_case, cur_group
            
    def record_group(cur_case, cur_group, init_evt=None):
        nonlocal total_groups, total_size, total_simult, total_diff #, activ_orders
        if verbose and cur_group.size() != len(events):
            print(f"case {cur_case.id}: non-complete group {cur_group}")
        
        # - replace last event in group with 'repl' event
        # log.loc[cur_group.idxes[-1], 'concept:name'] = repl
        # to_drop.extend(cur_group.idxes[:-1])
        
        # - get timestamp differences using diff()
        # (time differences can be cumulative this way)
        # diff = log.loc[cur_group.idxes]['time:timestamp'].diff().rename('diff').astype(int)
        # simult = diff[(diff >= 0) & (diff <= max_timedelta)]
        # if (len(simult) == cur_group.size()-1):
            # total_simult += 1
        
        # - drop groups with "simultaneous" events (as per max_timedelta)
        # get timestamp differences between first, last index
        ts1 = log.loc[cur_group.idxes[0], 'time:timestamp']
        ts2 = log.loc[cur_group.idxes[-1], 'time:timestamp']
        # name1 = log.loc[cur_group.idxes[0], 'concept:name']
        # name2 = log.loc[cur_group.idxes[-1], 'concept:name']
        diff = (ts2 - ts1).total_seconds()
        # print(total_diff)
        if diff <= max_timedelta:
            total_simult += 1
            to_drop.extend(cur_group.idxes)
        else:
            total_diff += diff
            
        # else:
        #     if ts1 < ts2:
        #         activ_orders[(name1, name2)] += 1
        #     elif ts2 < ts1:
        #         activ_orders[(name2, name1)] += 1
        
        # enforce ordering
        # first = cur_group.idxes[0 if name1 == events[0] else -1]
        # second = cur_group.idxes[-1 if name1 == events[0] else 0]
        # log.loc[second, 'time:timestamp'] = log.loc[first, 'time:timestamp'] + pd.Timedelta(seconds=1)
        
        # housekeeping
        total_groups += 1
        total_size += cur_group.size()
        cur_case.add(cur_group)
        cur_group = Group()
        if init_evt is not None:
            cur_group.add(init_evt)
        return cur_group
    
    cnt = 0; size = log.shape[0]; cur_perc = 0
    def progress():
        nonlocal cnt, size, cur_perc
        perc = int(cnt / size * 100)
        if perc != cur_perc and perc % 5 == 0:
            print(f"{perc}% done")
        cnt += 1
        cur_perc = perc
    
    for _, row in log.iterrows():
        # if verbose:
        progress()
        
        case_id = row['case:concept:name']
        # if new case is found, record prior case
        if cur_case.id is None or cur_case.id != case_id:
            if cur_case.id is not None:
                cur_case, cur_group = record_case(cur_case, cur_group)
                # break # (testing)

            cur_case.id = case_id
        
        cur_evt = row['concept:name']
        if cur_evt in events:
            # current event is already in group;
            # record prior group & start new one (incl. current event)
            if cur_group.includes(cur_evt):
                cur_group = record_group(cur_case, cur_group, row)
            else:
                # add event to current group
                cur_group.add(row)
                # if group is full, record it & start new one
                if cur_group.size() == len(events):
                    cur_group = record_group(cur_case, cur_group)
        
    # record last case
    record_case(cur_case, cur_group)
    
    print("# groups:", total_groups, "total size:", total_size, "avg size:", round(total_size/total_groups, 2))
    print("# groups:", total_groups, "# simult:", total_simult, "avg:", round(total_simult/total_groups*100, 2), "%")
    print("# groups:", total_groups, "avg_diff:", int(total_diff/total_groups), "s", "(", int(total_diff/total_groups/60), "m", ")")
    # print(activ_orders)
    
    return log.drop(to_drop)
    
    # - uses dataframes but makes assumptions
    
    # # put 1 in case activity corresponds to event
    # log['evt_cnt'] = 0
    # log.loc[log['concept:name'].isin(events), 'evt_cnt'] = 1
    
    # # take cumulative sum per case; put in cumul_cnt column
    # # (takes sum of current and all prior evt_cnt values; resets per group)
    # cumul_cnts = log.groupby('case:concept:name')['evt_cnt'].cumsum().rename('cumul_cnt')
    
    # # all rows where activity is in events, and cumul_cnt % len(events) is 0, is the last of the aggregation group
    
    # # replace activity with repl
    # log.loc[(log['concept:name'].isin(events)) & (cumul_cnts % len(events) == 0), 'concept:name'] = repl
    # # drop other events not meeting those criteria (repl may also be in events)
    # # return log
    # return log.loc[(~ log['concept:name'].isin(events)) | (log['concept:name'] == repl), log.columns != 'evt_cnt']