#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
import random
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from functools import partial, update_wrapper
from itertools import combinations

# seeds 2185, 8448, 3274 are particularly bad for 5 node rf 2.

N_PARTITIONS = 4096
RACKS = [10, 10, 10]
REPLICATION_FACTOR = 3
N_RUNS = 100

NODE_NAMES = [chr(65 + i) for i in range(26)]
NODE_NAMES = [name for sublist in [[n + str(s) for n in NODE_NAMES]
                                   for s in range(10)] for name in sublist]
SEED = 7973  # random.SystemRandom().randint(1000, 9999)  # 7973

OUTPUT = True
DISABLE_MAX_OUTAGE = True
DO_DROP = True
N_PROCS = os.cpu_count()


def sim_partial(*args, **kwargs):
    fn_name = kwargs.pop('fn_name', '')
    fn = partial(*args, **kwargs)
    update_wrapper(fn, args[0])

    if fn_name:
        fn.__name__ = "{}_{}".format(fn.__name__, fn_name)

    return fn


class log(object):
    indent_level = 0
    indentation = ''

    def __init__(self, fmt, *args, **kwargs):
        if OUTPUT:
            msg = fmt.format(*args, **kwargs)

            print("{}{}".format(log.indentation, msg))

    def __enter__(self):
        log.indent_level += 1
        self._update_indentation()

    def __exit__(self, *args):
        log.indent_level -= 1
        self._update_indentation()

    def _update_indentation(self):
        indentation = '│  ' * (log.indent_level - 1)

        if log.indent_level > 0:
            indentation += '├─ '

        log.indentation = indentation


def create_hash(partition, node):
    return (hashlib.md5("{}{}".format(partition, node).encode()).hexdigest(), node)


def make_map(nodes):
    partition_map = []

    for partition in range(0, N_PARTITIONS):
        row = [create_hash(partition, n) for n in nodes]
        row = sorted(row, key=lambda hn: hn[0])
        row = [hn[1] for hn in row]

        partition_map.append(row)

    return partition_map


def describe_map(nodes, racks, pmap):
    n_racks = len(set(racks.values()))

    if n_racks > 1:
        return describe_rack_aware_map(nodes, racks, pmap)

    replica_map = (r[:REPLICATION_FACTOR] for r in pmap)
    replicas_counts = [Counter() for _ in range(REPLICATION_FACTOR)]
    expected = (N_PARTITIONS + len(nodes) - 1) // len(nodes)

    for replicas in replica_map:
        for r, node in enumerate(replicas):
            replicas_counts[r][node] += 1

    if DISABLE_MAX_OUTAGE:
        combos = []
        max_outage = -1
    else:
        combos = combinations(nodes, REPLICATION_FACTOR)
        max_outage = 0

    # This is expensive, disable for high n_node values (bearable at
    # n_nodes = 64).
    for combo in combos:
        combo = set(combo)
        outage = 0

        for row in pmap:
            if combo == set(row[:REPLICATION_FACTOR]):
                outage += 1

        if outage > max_outage:
            max_outage = outage

    stats = [sorted(counts.values()) for counts in replicas_counts]
    stats = [(values[0], values[-1], values[-1] - expected,
              sum(1 for v in values if v > expected))
             for values in stats]

    nodes_counts = Counter()

    for replica_counts in replicas_counts:
        nodes_counts.update(replica_counts)

    # Compute node excess.
    remainder = N_PARTITIONS % len(nodes)
    extra_per_node = (remainder * REPLICATION_FACTOR + len(nodes) - 1) // len(nodes)
    max_per_node = REPLICATION_FACTOR * (N_PARTITIONS // len(nodes)) + extra_per_node
    node_excess = [sv if sv > 0 else 0 for sv
                   in sorted(v - max_per_node
                             for v in nodes_counts.values())]

    # Compute column excess.
    column_max_excess = 0

    with log("pmap with rf {} n_nodes {} expected {} max_rf_loss_outage {}",
             REPLICATION_FACTOR, len(nodes), expected, max_outage):
        for r, (minimum, maximum, excess, n_exceeding) in enumerate(stats):
            if excess > column_max_excess:
                column_max_excess = excess

            log("Column {}: minimum {} maximum {} excess {} n_exceeding {}",
                r, minimum, maximum, excess, n_exceeding)

        log("Node excess: minimum {} median {} maximum {}",
            node_excess[0], node_excess[len(node_excess) // 2],
            node_excess[-1])

    return column_max_excess, node_excess[-1]


def describe_rack_aware_map(nodes, racks, pmap):
    replica_map = (r[:REPLICATION_FACTOR] for r in pmap)
    replicas_counts = [Counter() for _ in range(REPLICATION_FACTOR)]
    node_counts = Counter()

    for replicas in replica_map:
        for r, node in enumerate(replicas):
            replicas_counts[r][node] += 1
            node_counts[node] += 1

    n_racks = len(set(racks.values()))
    n_rows_violating = 0
    n_replica_racks = min([n_racks, REPLICATION_FACTOR, len(nodes)])
    is_rack_aware = True

    for row in pmap:
        replica_racks = set()

        for r in range(n_replica_racks):
            replica_racks.add(racks[row[r]])

        if len(replica_racks) != n_replica_racks:
            is_rack_aware = False
            n_rows_violating += 1

    with log("Rack aware: n_replica_racks {}", n_replica_racks):
        if not is_rack_aware:
            log("WARNING - DID NOT SATISFY RACK AWARE {}", n_rows_violating)
            return 0, 0

        rack_ids = sorted(set(racks.values()))
        rack_replica_counts = [Counter() for r in range(REPLICATION_FACTOR)]
        rack_counts = Counter()
        rack_n_nodes = Counter()

        for n in nodes:
            rack_n_nodes[racks[n]] += 1

        for row in pmap:
            for r in range(REPLICATION_FACTOR):
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

            with log("Rack {}: n_nodes {} count {}",
                     rack_id, rack_n_nodes[rack_id],
                     rack_counts[rack_id]):
                for r in range(REPLICATION_FACTOR):
                    n_counts = []

                    for n in rack_nodes[rack_id]:
                        n_counts.append(replicas_counts[r][n])

                    if is_min_rack:
                        column_spreads.append(n_counts[-1] - n_counts[0])

                        for n in rack_nodes[rack_id]:
                            node_spread_values[n] += replicas_counts[r][n]

                    if len(n_counts) >= 3:
                        log("Column {}: total {} minimum {} median {} maximum {}",
                            r, rack_replica_counts[r][rack_id], n_counts[0],
                            n_counts[len(n_counts) // 2], n_counts[-1])
                    elif len(n_counts) == 2:
                        log("Column {}: total {} minimum {} maximum {}",
                            r, rack_replica_counts[r][rack_id], n_counts[0],
                            n_counts[-1])
                    else:
                        log("Column {}: total {}",
                            r, rack_replica_counts[r][rack_id])

                if is_min_rack:
                    node_spread_values = sorted(node_spread_values.values())
                    node_spreads.append(
                        node_spread_values[-1] - node_spread_values[0])

    column_spread = sorted(column_spreads)[-1]
    node_spread = sorted(node_spreads)[-1]

    return column_spread, node_spread


def compare_maps(pmap1, pmap2):
    n_changed = 0
    all_changed = 0

    replica_map1 = [r[:REPLICATION_FACTOR] for r in pmap1]
    replica_map2 = [r[:REPLICATION_FACTOR] for r in pmap2]

    for replicas1, replicas2 in zip(replica_map1, replica_map2):
        intersect = set(replicas1) & set(replicas2)

        if len(intersect) < len(replicas1):
            n_changed += len(replicas1) - len(intersect)

        if not intersect:
            all_changed += 1

    log("all-replicas-changed: {}, total-changes: {}", all_changed, n_changed)


def simulate(balance_fn, do_drop=DO_DROP):
    random.seed(SEED)

    n_nodes = sum(RACKS)
    init_nodes = sorted(random.sample(NODE_NAMES, n_nodes))
    init_racks = {}
    node_ix = 0

    for rack_id, count in enumerate(RACKS, 1):
        for node in init_nodes[node_ix : node_ix + count]:
            init_racks[node] = rack_id

        node_ix += count

    with log('Start'):
        init_map = balance_fn(init_nodes, init_racks, make_map(init_nodes))

        column_max_excess, node_max_excess = describe_map(
            init_nodes, init_racks, init_map)

    if do_drop:
        with log('Removed a node'):
            remove_node = init_nodes[:]
            remove_node.pop()
            remove_node_map = balance_fn(
                remove_node, init_racks, make_map(remove_node))

            describe_map(remove_node, init_racks, remove_node_map)
            compare_maps(init_map, remove_node_map)

    return column_max_excess, node_max_excess


def then_rack_aware(balance_fn):
    def is_unique_before_r(racks, row, r, rack_id):
        for prior_r in range(r):
            prior_n = row[prior_r]

            if racks[prior_n] == rack_id:
                return False

        return True

    def do_rack_aware(nodes, racks, pmap, **kwargs):
        pmap = balance_fn(nodes, racks, pmap, **kwargs)

        n_needed = min(
            [len(set(racks.values())), len(nodes), REPLICATION_FACTOR])

        for row in pmap:
            next_r = n_needed

            for cur_r in range(1, n_needed):
                if is_unique_before_r(racks, row, cur_r, racks[row[cur_r]]):
                    continue

                swap_r = cur_r

                for next_r in range(next_r, len(nodes)):
                    next_n = row[next_r]

                    if is_unique_before_r(racks, row, cur_r, racks[next_n]):
                        swap_r = next_r
                        next_r += 1
                        break
                else:
                    continue

                if cur_r != swap_r:
                    swap_n = row[swap_r]
                    row[swap_r] = row[cur_r]
                    row[cur_r] = swap_n

        return pmap

    do_rack_aware.__name__ = balance_fn.__name__ + "_then_rack_aware"

    return do_rack_aware


def standard_balance(nodes, racks, pmap):
    return pmap


def naive_balance(nodes, racks, pmap, lowered=0):
    replica_counts = [Counter() for _ in range(REPLICATION_FACTOR)]
    target_ptns = (N_PARTITIONS - lowered) // len(nodes)

    for pid, row in enumerate(pmap):
        for r in range(REPLICATION_FACTOR):
            counts = replica_counts[r]

            if counts[row[r]] >= target_ptns:
                min_r = r

                for next_r in range(r + 1, len(nodes)):
                    if counts[row[next_r]] < counts[row[min_r]]:
                        min_r = next_r

                n = row[min_r]
                row[min_r] = row[r]
                row[r] = n

            counts[row[r]] += 1

    return pmap


def naive_balance_enhanced_v1(nodes, racks, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) // len(nodes)

    min_claims = N_PARTITIONS // len(nodes)
    replicas_target_claims = [{n: min_claims for n in nodes}
                              for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    for row in pmap:
        for r in range(REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]

            if replica_claims[row[r]] >= smooth_balance_mark:
                swap_r = None
                swap_claims = None

                for next_r in range(r, len(nodes)):
                    next_claims = replica_claims[row[next_r]]
                    next_target_claims = replica_target_claims[row[next_r]]

                    if swap_r is None:
                        if next_claims < next_target_claims:
                            swap_r = next_r
                            swap_claims = next_claims

                        continue

                    if next_claims < swap_claims and \
                       next_claims < next_target_claims:
                        swap_r = next_r
                        swap_claims = next_claims

                if swap_r is not None and swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1

    return pmap


def naive_balance_enhanced_v2(nodes, racks, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) // len(nodes)

    min_claims = N_PARTITIONS // len(nodes)
    replicas_target_claims = [{n: min_claims for n in nodes}
                              for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    for row in pmap:
        for r in range(REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]

            if replica_claims[row[r]] >= smooth_balance_mark:
                swap_r = r
                swap_target_claims = replica_target_claims[row[r]]
                swap_score = swap_target_claims - replica_claims[row[r]]

                for next_r in range(r + 1, len(nodes)):
                    next_claims = replica_claims[row[next_r]]
                    next_target_claims = replica_target_claims[row[next_r]]
                    next_score = next_target_claims - next_claims

                    if next_score > swap_score:
                        swap_r = next_r
                        swap_score = next_score
                    elif next_score == swap_score:
                        if swap_target_claims > next_target_claims:
                            # This seems to help... not sure why.
                            swap_r = next_r
                            swap_target_claims = next_target_claims
                            swap_score = next_score

                if swap_r is not None and swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1

    return pmap


def rack_unique_before_r(racks, row, r, cur_rack):
    for prior_r in range(r):
        prior_rack = racks[row[prior_r]]

        if cur_rack == prior_rack:
            return False

    return True


def rack_balance(nodes, racks, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) // len(nodes)

    min_claims = N_PARTITIONS // len(nodes)
    replicas_target_claims = [{n: min_claims for n in nodes}
                              for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)
    n_racks = len(set(racks.values()))

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    for pid, row in enumerate(pmap):
        for r in range(REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_score = swap_target_claims - r_claims
            swap_is_rack_safe = swap_r == 0 or swap_r >= n_racks or \
                rack_unique_before_r(racks, row, swap_r, racks[row[r]])

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r + 1, len(nodes)):
                    if r < n_racks:
                        if not rack_unique_before_r(
                                racks, row, r, racks[row[next_r]]):
                            continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_target_claims = replica_target_claims[row[next_r]]
                    next_claims = replica_claims[row[next_r]]
                    next_score = next_target_claims - next_claims

                    if not swap_is_rack_safe:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                        swap_is_rack_safe = True

                        if r_claims < smooth_balance_mark:
                            break

                        continue

                    if next_score > swap_score:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                    elif next_score == swap_score:
                        if swap_target_claims > next_target_claims:
                            # This seems to help... not sure why.
                            swap_r = next_r
                            swap_target_claims = next_target_claims
                            swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1

    return pmap


def simple_rack_balance(nodes, racks, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) // len(nodes)

    min_claims = N_PARTITIONS // len(nodes)
    replicas_target_claims = [{n: min_claims for n in nodes}
                              for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)
    n_racks = len(set(racks.values()))

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    for pid, row in enumerate(pmap):
        for r in range(REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_score = swap_target_claims - r_claims
            swap_is_rack_safe = swap_r == 0 or swap_r >= n_racks or \
                rack_unique_before_r(racks, row, swap_r, racks[row[r]])

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r + 1, len(nodes)):
                    if r < n_racks:
                        if not rack_unique_before_r(
                                racks, row, r, racks[row[next_r]]):
                            continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_target_claims = replica_target_claims[row[next_r]]
                    next_claims = replica_claims[row[next_r]]
                    next_score = next_target_claims - next_claims

                    if not swap_is_rack_safe:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                        swap_is_rack_safe = True

                        if r_claims < smooth_balance_mark:
                            break

                        continue

                    if next_score > swap_score:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1

    return pmap


def rack_balance_order(nodes, racks, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) // len(nodes)

    min_claims = N_PARTITIONS // len(nodes)
    replicas_target_claims = [{n: min_claims for n in nodes}
                              for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)
    n_racks = len(set(racks.values()))

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    def rack_unique_in_evaled(racks, row, evaluated_replicas, cur_rack):
        for prior_r in evaluated_replicas:
            prior_rack = racks[row[prior_r]]

            if cur_rack == prior_rack:
                return False

        return True

    for pid, row in enumerate(pmap):
        evaluated_replicas = []

        for _ in range(REPLICATION_FACTOR):
            min_replica = None
            min_claims = None

            for r in range(REPLICATION_FACTOR):
                if r in evaluated_replicas:
                    continue

                r_claims = replicas_claims[r][row[r]]

                if min_replica is None or r_claims < min_claims:
                    min_replica = r
                    min_claims = r_claims

            r = min_replica

            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]

            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_score = swap_target_claims - r_claims
            swap_is_rack_safe = any([
                len(evaluated_replicas) == 0,
                len(evaluated_replicas) >= n_racks,
                rack_unique_in_evaled(racks, row, evaluated_replicas,
                                      racks[row[r]])])

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(0, len(nodes)):
                    if next_r in evaluated_replicas:
                        continue

                    if len(evaluated_replicas) < n_racks:
                        if not rack_unique_in_evaled(
                                racks, row, evaluated_replicas, racks[row[next_r]]):
                            continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_target_claims = replica_target_claims[row[next_r]]
                    next_claims = replica_claims[row[next_r]]
                    next_score = next_target_claims - next_claims

                    if not swap_is_rack_safe:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                        swap_is_rack_safe = True

                        if r_claims < smooth_balance_mark:
                            break

                        continue

                    if next_score > swap_score:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                    elif next_score == swap_score:
                        if swap_target_claims > next_target_claims:
                            # This seems to help... not sure why.
                            swap_r = next_r
                            swap_target_claims = next_target_claims
                            swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1
            evaluated_replicas.append(r)

    return pmap


def rack_balance_orderish(nodes, racks, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) // len(nodes)

    min_claims = N_PARTITIONS // len(nodes)
    replicas_target_claims = [{n: min_claims for n in nodes}
                              for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)
    n_racks = len(set(racks.values()))

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    def rack_unique_in_evaled(racks, row, evaluated_replicas, cur_rack):
        for prior_r in evaluated_replicas:
            prior_rack = racks[row[prior_r]]

            if cur_rack == prior_rack:
                return False

        return True

    for pid, row in enumerate(pmap):
        evaluated_replicas = []

        for z in range(REPLICATION_FACTOR):
            min_replica = None
            min_score = None

            for r in range(REPLICATION_FACTOR):
                if r in evaluated_replicas:
                    continue

                r_claims = replicas_claims[z][row[r]]
                r_target_claims = replicas_target_claims[z][row[r]]
                r_score = r_target_claims - r_claims

                if min_replica is None or r_score < min_score:
                    min_replica = r
                    min_score = r_claims

            min_replica = z

            n = row[z]
            row[z] = row[min_replica]
            row[min_replica] = row[z]
            r = z

            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]

            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_score = swap_target_claims - r_claims
            swap_is_rack_safe = any([
                len(evaluated_replicas) == 0,
                len(evaluated_replicas) >= n_racks,
                rack_unique_in_evaled(racks, row, evaluated_replicas,
                                      racks[row[r]])])

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r + 1, len(nodes)):
                    if next_r in evaluated_replicas:
                        continue

                    if len(evaluated_replicas) < n_racks:
                        if not rack_unique_in_evaled(
                                racks, row, evaluated_replicas, racks[row[next_r]]):
                            continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_target_claims = replica_target_claims[row[next_r]]
                    next_claims = replica_claims[row[next_r]]
                    next_score = next_target_claims - next_claims

                    if not swap_is_rack_safe:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                        swap_is_rack_safe = True

                        if r_claims < smooth_balance_mark:
                            break

                        continue

                    if next_score > swap_score:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                    elif next_score == swap_score:
                        if swap_target_claims > next_target_claims:
                            # This seems to help... not sure why.
                            swap_r = next_r
                            swap_target_claims = next_target_claims
                            swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1
            evaluated_replicas.append(r)

    return pmap


def rack_balance_andys_tie(nodes, racks, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) // len(nodes)

    min_claims = N_PARTITIONS // len(nodes)
    replicas_target_claims = [{n: min_claims for n in nodes}
                              for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)
    n_racks = len(set(racks.values()))
    n_ties = 0

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    for pid, row in enumerate(pmap):
        for r in range(REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_score = swap_target_claims - r_claims
            swap_is_rack_safe = swap_r == 0 or swap_r >= n_racks or \
                rack_unique_before_r(racks, row, swap_r, racks[row[r]])
            tie = False

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r + 1, len(nodes)):
                    if r < n_racks:
                        if not rack_unique_before_r(
                                racks, row, r, racks[row[next_r]]):
                            continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_target_claims = replica_target_claims[row[next_r]]
                    next_claims = replica_claims[row[next_r]]
                    next_score = next_target_claims - next_claims

                    if not swap_is_rack_safe:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                        swap_is_rack_safe = True

                        if r_claims < smooth_balance_mark:
                            break

                        continue

                    if next_score > swap_score:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                        tie = False
                    elif next_score == swap_score:
                        if r != REPLICATION_FACTOR - 1:
                            tie_swap_target_claims = replicas_target_claims[r + 1][row[swap_r]]
                            tie_swap_claims = replicas_claims[r + 1][row[swap_r]]
                            tie_swap_score = tie_swap_target_claims - tie_swap_claims

                            tie_next_target_claims = replicas_target_claims[r + 1][row[next_r]]
                            tie_next_claims = replicas_claims[r + 1][row[next_r]]
                            tie_next_score = tie_next_target_claims - tie_next_claims

                            if tie_next_score < tie_swap_score:
                                tie = True
                                swap_r = next_r
                                swap_target_claims = next_target_claims
                                swap_score = next_score
                            elif tie_next_score == tie_swap_score:
                                if swap_target_claims > next_target_claims:
                                    # This seems to help... not sure why.
                                    swap_r = next_r
                                    swap_target_claims = next_target_claims
                                    swap_score = next_score
                        elif swap_target_claims > next_target_claims:
                            # This seems to help... not sure why.
                            swap_r = next_r
                            swap_target_claims = next_target_claims
                            swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

                if tie:
                    n_ties += 1

            replica_claims[row[r]] += 1

    return pmap


def main():
    global OUTPUT, SEED

    balance_lowered = []

    for i in [64, 128, 256, 512, 1024]:
        # fn_name = "with_lowered_{}".format(i)
        # naive_balance_lowered.append(sim_partial(
        #     naive_balance, lowered=i, fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)
        # naive_balance_lowered.append(sim_partial(
        #     naive_balance_enhanced_v1, lowered=i, fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)
        # naive_balance_lowered.append(then_rack_aware(sim_partial(
        #     naive_balance_enhanced_v2, lowered=i, fn_name=fn_name)))

        fn_name = "with_lowered_{}".format(i)
        balance_lowered.append(sim_partial(rack_balance, lowered=i,
                                           fn_name=fn_name))

        fn_name = "with_lowered_{}".format(i)
        balance_lowered.append(sim_partial(simple_rack_balance, lowered=i,
                                           fn_name=fn_name))

        fn_name = "with_lowered_{}".format(i)
        balance_lowered.append(sim_partial(rack_balance_order, lowered=i,
                                           fn_name=fn_name))

        # fn_name = "with_lowered_{}".format(i)  # FAIL
        # naive_balance_lowered.append(sim_partial(
        #     rack_balance_orderish, lowered=i, fn_name=fn_name))

    balance_fns = [
        # then_rack_aware(standard_balance),
    ]

    balance_fns.extend(balance_lowered)

    n_racks = len(RACKS)
    n_nodes = sum(RACKS)

    for balance_fn in balance_fns:
        with log("Simulating '{}' - seed {} n_nodes {} RF {} n_racks {} RACKS {}",
                 balance_fn.__name__, SEED, n_nodes, REPLICATION_FACTOR,
                 n_racks, RACKS):
            simulate(balance_fn)
            log("")

    if N_RUNS == 0:
        exit()

    global do_run  # hack for ProcessPoolExecutor

    def do_run(seed):
        global SEED

        SEED = seed  # set seed for this process
        r = {}

        for fn in balance_fns:
            r[fn.__name__] = simulate(fn, do_drop=False)

        return r

    with log("Comparing: seed {} N_RUNS {} n_nodes {} RF {} n_racks {} RACKS {}",
             SEED, N_RUNS, n_nodes, REPLICATION_FACTOR, n_racks, RACKS):
        OUTPUT = False
        run_results = []

        executor = ProcessPoolExecutor(max_workers=N_PROCS)
        run_results = executor.map(
            do_run, (random.randint(1000, 9999) for _ in range(N_RUNS)),
            chunksize=N_RUNS // N_PROCS)

        OUTPUT = True

        results = {fn.__name__: [] for fn in balance_fns}

        for run_result in run_results:
            for name, value in run_result.items():
                results[name].append(value)

        for fn in balance_fns:
            name = fn.__name__
            fn_results = results[name]
            column_peaks, node_peaks = zip(*fn_results)

            column_peaks = sorted(column_peaks)
            column_median = column_peaks[len(column_peaks) // 2]
            column_maximum = column_peaks[-1]
            column_runs_gt_1 = (sum(1 for p in column_peaks if p > 1) / len(column_peaks)) * 100

            node_peaks = sorted(node_peaks)
            node_median = node_peaks[len(node_peaks) // 2]
            node_maximum = node_peaks[-1]
            runs_excess = sum(1 for p in node_peaks if p > 0)
            node_runs_gt_1 = (sum(1 for p in node_peaks if p > 1) / len(node_peaks)) * 100

            with log("{} excesses", name):
                log("Columns: median {} maximum {} runs_gt_1 {:2.2f}%",
                    column_median, column_maximum, column_runs_gt_1)
                log("Nodes: median {} maximum {} n_runs_exceeding {} runs_gt_1 {:2.2f}%",
                    node_median, node_maximum, runs_excess, node_runs_gt_1)


if __name__ == '__main__':
    main()
