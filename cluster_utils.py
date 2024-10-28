import pandas as pd
from collections_extended import frozenbag
#from itertools import chain
import numpy
import scipy
import sklearn.preprocessing
from pm4py.algo.evaluation import algorithm as general_evaluation

def sequences_to_sets(sequences):
    sets = {}
    for seq in sequences:
        seq_set = frozenset(seq)
        
        if seq_set in sets:
            sets.get(seq_set).append(seq)
        else:
            sets[seq_set] = [ seq ]

    df = pd.DataFrame({ 'set': sets.keys(), 'sequences': sets.values() })
    df['num_seq'] = df.apply(lambda row: len(row['sequences']), axis=1)
    df = df.sort_values(by='num_seq', ascending=False)
    df.reset_index(inplace=True, drop=True)
    df = df[['set', 'num_seq', 'sequences']]

    return df


def sequences_to_bags(sequences):
    bags = {}
    for seq in sequences:
        seq_bag = frozenbag(seq)
        
        if seq_bag in bags:
            bags.get(seq_bag).append(seq)
        else:
            bags[seq_bag] = [ seq ]

    df = pd.DataFrame({ 'bag': bags.keys(), 'sequences': bags.values() })
    df['num_seq'] = df.apply(lambda row: len(row['sequences']), axis=1)
    df = df.sort_values(by='num_seq', ascending=False)
    df.reset_index(inplace=True, drop=True)
    df = df[['bag', 'num_seq', 'sequences']]

    return df


def log_per_case(log, callback):
    # inspired by pm4py's get_variants
    log = log.sort_values(by=[ 'case:concept:name', 'time:timestamp', 'concept:name' ])
    log = log.reset_index(drop=True)
    unique_cases, case_indexes, num_evts_per_case = numpy.unique(log['case:concept:name'], return_index=True, return_counts=True)

    for case_id, case_idx, num_evts in zip(unique_cases, case_indexes, num_evts_per_case):
        # case_evts = log.loc[case_idx:(case_idx+num_evts-1),:] (debugging)
        case_evts = list(log['concept:name'][case_idx:(case_idx+num_evts)])
        callback(case_id, case_evts)

def log_to_sequences_list(log):
    sequences = []
    log_per_case(log, lambda _, case_evts: list.append(case_evts))

    return sequences
        

def log_to_sequences_df(log, columns=[ 'case:concept_name', 'sequence' ]):
    cases = []
    log_per_case(log, lambda case_id, case_evts: cases.append([ case_id, case_evts ]))

    return pd.DataFrame(cases, columns=columns)
    

def sequences_to_transit_matrix(sequences, as_sparse=False, normalize_axis=None):
    # collect unique activities
    unique_activ = {evt for seq in sequences for evt in seq}
    
    # create transit matrix where row is src activ, col is tgt activ, 
    # value is number of directed edges in all sequences
    # (initially all 0's)
    transit_matrix = pd.DataFrame(0, index=unique_activ, columns=unique_activ)
    for seq in sequences:
        # per pair of events in sequence, create tuple between src & tgt
        transit_tuples = [ (seq[i], seq[i+1]) for i in range(0, len(seq) - 1) ]
        # for each transit tuple, update count in transit matrix
        for src, tgt in transit_tuples:
            transit_matrix[tgt][src] += 1

    if as_sparse:
        transit_matrix = scipy.sparse.csr_matrix(transit_matrix.values)
    if normalize_axis is not None:
        transit_matrix = sklearn.preprocessing.normalize(transit_matrix, norm="l1", axis=normalize_axis)

    return transit_matrix


def num_cases(log):
    return len(log['case:concept:name'].unique())


def eval_metrics(log, net, init_marking, final_marking, show_progress_bar=True):
    print("getting metrics")
    metrics = general_evaluation.apply(log, net, init_marking, final_marking, parameters={ 'show_progress_bar': show_progress_bar } )
    return { 
        'fscore': metrics['fscore'],
        'log_fitness': metrics['fitness']['log_fitness'],
        'precision': metrics['precision'],
        'generalization': metrics['generalization'],
        'simplicity': metrics['simplicity'] 
    }


def eval_cluster_metrics(log, sublogs, miner_fn, show_progress_bar=True):
    print("\n> evaluating sublog metrics")

    print("mining original process")
    net, init_marking, final_marking = miner_fn(log)
    print("getting metrics")
    metrics = eval_metrics(log, net, init_marking, final_marking, show_progress_bar=show_progress_bar)
    print("\noriginal log")
    print(f"(num cases: {num_cases(log)})")
    print(metrics)
    print()

    cnt = 0
    total_fscore = 0

    for sublog in sublogs:
        print("sublog", cnt)
        print(f"(num cases: {num_cases(sublog)})")

        print("mining subprocess")
        sub_net, sub_init_marking, sub_final_marking = miner_fn(sublog)

        metrics = eval_metrics(log, sub_net, sub_init_marking, sub_final_marking, show_progress_bar=show_progress_bar)
        print(metrics)
        print()

        cnt += 1
        total_fscore += metrics['fscore']

    print("avg fscore:", total_fscore / cnt)