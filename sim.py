#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from functools import partial, update_wrapper
from itertools import combinations

from lib import config
from lib.log import log
from lib.strategies.standard import standard_balance, then_rack_aware, then_rack_aware2


def sim_partial(*args, **kwargs):
    fn_name = kwargs.pop("fn_name", "")
    fn = partial(*args, **kwargs)
    update_wrapper(fn, args[0])

    if fn_name:
        fn.__name__ = f"{fn.__name__}_{fn_name}"

    return fn


def describe_map(nodes, racks, pmap):
    n_racks = len(set(racks.values()))

    if n_racks > 1:
        return describe_rack_aware_map(nodes, racks, pmap)

    replica_map = (r[: config.REPLICATION_FACTOR] for r in pmap)
    replicas_counts = [Counter() for _ in range(config.REPLICATION_FACTOR)]
    expected = (config.N_PARTITIONS + len(nodes) - 1) // len(nodes)

    for replicas in replica_map:
        for r, node in enumerate(replicas):
            replicas_counts[r][node] += 1

    if config.DISABLE_MAX_OUTAGE:
        combos = []
        max_outage = -1
    else:
        combos = combinations(nodes, config.REPLICATION_FACTOR)
        max_outage = 0

    for combo in combos:
        combo = set(combo)
        outage = 0

        for row in pmap:
            if combo == set(row[: config.REPLICATION_FACTOR]):
                outage += 1

        if outage > max_outage:
            max_outage = outage

    stats = [sorted(counts.values()) for counts in replicas_counts]
    stats = [
        (
            values[0],
            values[-1],
            values[-1] - expected,
            sum(1 for v in values if v > expected),
        )
        for values in stats
    ]

    nodes_counts = Counter()

    for replica_counts in replicas_counts:
        nodes_counts.update(replica_counts)

    # Compute node excess.
    remainder = config.N_PARTITIONS % len(nodes)
    extra_per_node = (remainder * config.REPLICATION_FACTOR + len(nodes) - 1) // len(
        nodes
    )
    max_per_node = (
        config.REPLICATION_FACTOR * (config.N_PARTITIONS // len(nodes)) + extra_per_node
    )
    node_excess = [sv for sv in sorted(v - max_per_node for v in nodes_counts.values())]

    # Compute column excess.
    column_max_excess = 0

    with log(
        " ".join(
            [
                f"pmap with rf {config.REPLICATION_FACTOR} n_nodes {len(nodes)}",
                f"expected {expected} max_rf_loss_outage {max_outage}",
            ]
        )
    ):
        for r, (minimum, maximum, excess, n_exceeding) in enumerate(stats):
            if excess > column_max_excess:
                column_max_excess = excess

            log(
                " ".join(
                    [
                        f"Column {r}: minimum {minimum} maximum {maximum}",
                        f"excess {excess} n_exceeding {n_exceeding}",
                    ]
                )
            )

        log(
            " ".join(
                [
                    f"Node excess: minimum {node_excess[0]}",
                    f"median {node_excess[len(node_excess) // 2]}",
                    f"maximum {node_excess[-1]}",
                ]
            )
        )

    return column_max_excess, node_excess[-1]


def describe_rack_aware_map(nodes, racks, pmap):
    replica_map = (r[: config.REPLICATION_FACTOR] for r in pmap)
    replicas_counts = [Counter() for _ in range(config.REPLICATION_FACTOR)]
    node_counts = Counter()

    for replicas in replica_map:
        for r, node in enumerate(replicas):
            replicas_counts[r][node] += 1
            node_counts[node] += 1

    n_racks = len(set(racks.values()))
    n_rows_violating = 0
    n_replica_racks = min([n_racks, config.REPLICATION_FACTOR, len(nodes)])
    is_rack_aware = True

    for row in pmap:
        replica_racks = set()

        for r in range(n_replica_racks):
            replica_racks.add(racks[row[r]])

        if len(replica_racks) != n_replica_racks:
            is_rack_aware = False
            n_rows_violating += 1

    with log(f"Rack aware: n_replica_racks {n_replica_racks}"):
        if not is_rack_aware:
            log(f"WARNING - DID NOT SATISFY RACK AWARE {n_rows_violating}")
            return 0, 0

        rack_ids = sorted(set(racks.values()))
        rack_replica_counts = [Counter() for r in range(config.REPLICATION_FACTOR)]
        rack_counts = Counter()
        rack_n_nodes = Counter()

        for n in nodes:
            rack_n_nodes[racks[n]] += 1

        for row in pmap:
            for r in range(config.REPLICATION_FACTOR):
                node = row[r]
                rack = racks[node]

                rack_replica_counts[r][rack] += 1
                rack_counts[rack] += 1

        rack_nodes = {rack_id: [] for rack_id in rack_ids}

        for node, rack_id in racks.items():
            if node in nodes:
                rack_nodes[rack_id].append(node)

        for value in rack_nodes.values():
            value.sort()

        min_rack_size = min(len(v) for v in rack_nodes.values())
        column_spreads = []
        node_spreads = []

        for rack_id in rack_ids:
            is_min_rack = len(rack_nodes[rack_id]) == min_rack_size
            node_spread_values = Counter()

            with log(
                " ".join(
                    [
                        f"Rack {rack_id}: n_nodes {rack_n_nodes[rack_id]}",
                        f"count {rack_counts[rack_id]}",
                    ]
                )
            ):
                for r in range(config.REPLICATION_FACTOR):
                    n_counts = []

                    for n in rack_nodes[rack_id]:
                        n_counts.append(replicas_counts[r][n])

                    if is_min_rack:
                        column_spreads.append(n_counts[-1] - n_counts[0])

                        for n in rack_nodes[rack_id]:
                            node_spread_values[n] += replicas_counts[r][n]

                    if len(n_counts) >= 3:
                        log(
                            " ".join(
                                [
                                    f"Column {r}:",
                                    f"total {rack_replica_counts[r][rack_id]}",
                                    f"minimum {n_counts[0]}",
                                    f"median {n_counts[len(n_counts) // 2]}",
                                    f"maximum {n_counts[-1]}",
                                ]
                            )
                        )
                    elif len(n_counts) == 2:
                        log(
                            " ".join(
                                [
                                    f"Column {r}:",
                                    f"total {rack_replica_counts[r][rack_id]}",
                                    f"minimum {n_counts[0]} maximum {n_counts[-1]}",
                                ]
                            )
                        )
                    else:
                        log(
                            " ".join(
                                [
                                    f"Column {r}:",
                                    f"total {rack_replica_counts[r][rack_id]}",
                                ]
                            )
                        )

                if is_min_rack:
                    node_spread_values = sorted(node_spread_values.values())
                    node_spreads.append(node_spread_values[-1] - node_spread_values[0])

    column_spread = sorted(column_spreads)[-1]
    node_spread = sorted(node_spreads)[-1]

    return column_spread, node_spread


def compare_maps(pmap1, pmap2, racks):
    n_nodes_affected = abs(len(pmap1[0]) - len(pmap2[0]))

    n_changed = 0
    n_large_changes = 0
    all_changed = 0

    replica_map1 = [r[: config.REPLICATION_FACTOR] for r in pmap1]
    replica_map2 = [r[: config.REPLICATION_FACTOR] for r in pmap2]

    for pid, (replicas1, replicas2) in enumerate(zip(replica_map1, replica_map2)):
        intersect = set(replicas1) & set(replicas2)

        if len(intersect) < len(replicas1):
            n_changed += len(replicas1) - len(intersect)

        if len(replicas1) - len(intersect) > n_nodes_affected:
            n_large_changes += 1

        if not intersect:
            all_changed += 1

    log(
        ", ".join(
            [
                f"all-replicas-changed: {all_changed}",
                f"n-large-changes({n_nodes_affected + 1}): {n_large_changes}",
                f"total-changes: {n_changed}",
            ]
        )
    )


def simulate(balance_fn, do_drop):
    random.seed(config.SEED)

    n_nodes = sum(config.RACKS)
    init_nodes = tuple(sorted(random.sample(config.NODE_NAMES, n_nodes)))
    init_racks = {}
    node_ix = 0

    for rack_id, count in enumerate(config.RACKS, 1):
        for node in init_nodes[node_ix : node_ix + count]:
            init_racks[node] = rack_id

        node_ix += count

    with log("Start"):
        init_map = balance_fn(init_nodes, init_racks)

        column_max_excess, node_max_excess = describe_map(
            init_nodes, init_racks, init_map
        )

    if do_drop:
        with log("Removed a node"):
            remove_node = init_nodes[1:]
            remove_node_map = balance_fn(remove_node, init_racks)

            describe_map(remove_node, init_racks, remove_node_map)
            compare_maps(init_map, remove_node_map, init_racks)

    return column_max_excess, node_max_excess


def main():
    balance_lowered = []

    for i in [1024]:
        pass
        # fn_name = "with_lowered_{}".format(i)
        # naive_balance_lowered.append(sim_partial(
        #     naive_balance, lowered=i, fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)
        # naive_balance_lowered.append(sim_partial(
        #     naive_balance_enhanced_v1, lowered=i, fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)
        # naive_balance_lowered.append(then_rack_aware(sim_partial(
        #     naive_balance_enhanced_v2, lowered=i, fn_name=fn_name)))

        # fn_name = "with_lowered_{}".format(i)
        # balance_lowered.append(sim_partial(rack_balance, lowered=i,
        #                                    fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)
        # balance_lowered.append(sim_partial(simple_rack_balance, lowered=i,
        #                                    fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)
        # balance_lowered.append(sim_partial(rack_balance_order, lowered=i,
        #                                    fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)  # FAIL
        # naive_balance_lowered.append(sim_partial(
        #     rack_balance_orderish, lowered=i, fn_name=fn_name))

    balance_fns = [
        then_rack_aware(standard_balance),
        then_rack_aware2(standard_balance),
        # multipass,
        # stable_rack_aware_positions,
    ]

    balance_fns.extend(balance_lowered)

    n_racks = len(config.RACKS)
    n_nodes = sum(config.RACKS)

    for balance_fn in balance_fns:
        with log(
            " ".join(
                [
                    f"Simulating '{balance_fn.__name__}' - seed {config.SEED}",
                    f"n_nodes {n_nodes} RF {config.REPLICATION_FACTOR}",
                    f"n_racks {n_racks} racks {config.RACKS}",
                ]
            )
        ):
            simulate(balance_fn, config.DO_DROP)

    if config.N_RUNS == 0:
        exit()

    global do_run  # hack for ProcessPoolExecutor

    def do_run(seed):
        config.SEED = seed  # set seed for this process
        r = {}

        for fn in balance_fns:
            r[fn.__name__] = simulate(fn, False)

        return r

    with log(
        " ".join(
            [
                f"Comparing: seed {config.SEED} n_runs {config.N_RUNS}",
                f"n_nodes {n_nodes} RF {config.REPLICATION_FACTOR}",
                f"n_racks {n_racks} racks {config.RACKS}",
            ]
        )
    ):
        prior_output = config.OUTPUT
        config.OUTPUT = False
        run_results = []

        executor = ProcessPoolExecutor(max_workers=config.N_PROCS)
        run_results = executor.map(
            do_run,
            (random.randint(1000, 9999) for _ in range(config.N_RUNS)),
            chunksize=config.N_RUNS // config.N_PROCS,
        )

        config.OUTPUT = prior_output

        results = {fn.__name__: [] for fn in balance_fns}

        for run_result in run_results:
            for name, value in run_result.items():
                results[name].append(value)

        for fn in balance_fns:
            name = fn.__name__
            fn_results = results[name]
            column_peaks, node_peaks = zip(*fn_results)

            column_peaks = sorted(column_peaks)
            column_minimum = column_peaks[0]
            column_median = column_peaks[len(column_peaks) // 2]
            column_maximum = column_peaks[-1]
            column_runs_gt_1 = (
                sum(1 for p in column_peaks if p > 1) / len(column_peaks)
            ) * 100

            node_peaks = sorted(node_peaks)
            node_median = node_peaks[len(node_peaks) // 2]
            node_minimum = node_peaks[0]
            node_maximum = node_peaks[-1]
            runs_excess = sum(1 for p in node_peaks if p > 0)
            node_runs_gt_1 = (
                sum(1 for p in node_peaks if p > 1) / len(node_peaks)
            ) * 100

            with log(f"{name} excesses"):
                log(
                    " ".join(
                        [
                            f"Columns: minimum {column_minimum}",
                            f"median {column_median}",
                            f"maximum {column_maximum}",
                            f"runs_gt_1 {column_runs_gt_1:2.2f}%",
                        ]
                    )
                )
                log(
                    " ".join(
                        [
                            f"Nodes: minimum {node_minimum}",
                            f"median {node_median}",
                            f"maximum {node_maximum}",
                            f"n_runs_exceeding {runs_excess}",
                            f"runs_gt_1 {node_runs_gt_1:2.2f}%",
                        ]
                    )
                )


if __name__ == "__main__":
    main()
