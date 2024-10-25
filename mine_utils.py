from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.visualization.dfg import visualizer as dfg_visualizer
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.visualization.heuristics_net import visualizer as hn_visualizer
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.visualization.process_tree import visualizer as pt_visualizer

def mine_vis(visualizer, gviz, output_path):
    if output_path is not None:
        visualizer.save(gviz, f"{output_path}.jpg")
    else:
        visualizer.view(gviz)

def mine_dfg(log, output_path=None):
    # discover
    dfg = dfg_discovery.apply(log, variant=dfg_discovery.Variants.FREQUENCY)

    # visualize
    gviz = dfg_visualizer.apply(dfg, log=log, variant=dfg_visualizer.Variants.FREQUENCY)
    mine_vis(dfg_visualizer, gviz, output_path)
        
    
# (todo - get annotations such as frequency/performance here as well)

def mine_alpha(log, output_path=None):
    # alpha miner
    net, initial_marking, final_marking = alpha_miner.apply(log)

    # visualise
    gviz = pn_visualizer.apply(net, initial_marking, final_marking)
    mine_vis(pn_visualizer, gviz, output_path)
    
    
def mine_heur(log, output_path=None):
    # heuristics miner
    heu_net = heuristics_miner.apply_heu(log)

    # visualize
    gviz = hn_visualizer.apply(heu_net)
    mine_vis(hn_visualizer, gviz, output_path)
    
    
def mine_induct(log, output_path=None):
    # create the process tree
    # (wvw: drop "_tree" from call)
    tree = inductive_miner.apply(log)

    # visualize
    gviz = pt_visualizer.apply(tree)
    mine_vis(pt_visualizer, gviz, output_path)