# -*- coding: utf-8 -*-

import hashlib
import random
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from functools import partial, update_wrapper
from itertools import combinations

# seeds 2185, 8448, 3274 are particularly bad for 5 node rf 2.

N_PARTITIONS = 4096
N_NODES = 96
REPLICATION_FACTOR = 3
N_RUNS = 100

NODE_NAMES = [chr(65 + i) for i in range(26)]
NODE_NAMES = [name for sublist in [[n + str(s) for n in NODE_NAMES]
                                   for s in range(10)] for name in sublist]
SEED = 7973  # random.SystemRandom().randint(1000, 9999)

OUTPUT = True
DISABLE_MAX_OUTAGE = True
DO_DROP = True
N_PROCS = 4
N_CHUNKS_PER_PROC = 8


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

            print "{}{}".format(log.indentation, msg)

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
    return (hashlib.md5("{}{}".format(partition, node)).hexdigest(), node)


def make_map(nodes):
    partition_map = []

    for partition in xrange(0, N_PARTITIONS):
        row = [create_hash(partition, n) for n in nodes]
        row = sorted(row, key=lambda hn: hn[0])
        row = [hn[1] for hn in row]

        partition_map.append(row)

    return partition_map


def describe_map(nodes, pmap):
    replica_map = [r[:REPLICATION_FACTOR] for r in pmap]
    replicas_counts = [Counter() for _ in range(REPLICATION_FACTOR)]
    expected = (N_PARTITIONS + len(nodes) - 1) / len(nodes)

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
    # N_NODES = 64).
    for combo in combos:
        combo = set(combo)
        outage = 0

        for row in pmap:
            if combo == set(row[:REPLICATION_FACTOR]):
                outage += 1

        if outage > max_outage:
            max_outage = outage

    stats = (sorted(counts.values()) for counts in replicas_counts)
    stats = [(values[0], values[-1], values[-1] - expected,
              sum(1 for v in values if v > expected))
             for values in stats]

    nodes_counts = Counter()

    for replica_counts in replicas_counts:
        nodes_counts.update(replica_counts)

    # Compute node excess.
    remainder = N_PARTITIONS % len(nodes)
    extra_per_node = (remainder * REPLICATION_FACTOR + len(nodes) - 1) / len(nodes)
    max_per_node = REPLICATION_FACTOR * (N_PARTITIONS / len(nodes)) + extra_per_node
    node_excess = [sv if sv > 0 else 0 for sv
                   in sorted(v - max_per_node
                             for v in nodes_counts.values())]
    # print extra_per_node, expected_per_node, sorted(nodes_counts.values()), node_excess

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
            node_excess[0], node_excess[len(node_excess) / 2],
            node_excess[-1])

    return column_max_excess, node_excess[-1]


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
    init_nodes = random.sample(NODE_NAMES, N_NODES)

    with log('Start'):
        initial_map = balance_fn(init_nodes, make_map(init_nodes))

        column_max_excess, node_max_excess = describe_map(
            init_nodes, initial_map)

    if do_drop:
        with log('Removed a node'):
            remove_node = init_nodes[:]
            remove_node.pop()
            remove_node_map = balance_fn(remove_node, make_map(remove_node))

            describe_map(remove_node, remove_node_map)
            compare_maps(initial_map, remove_node_map)

    return column_max_excess, node_max_excess


def standard_balance(nodes, pmap):
    return pmap


def ashish_balance(nodes, pmap):
    node_loads = [Counter() for _ in range(len(nodes))]
    max_load_per_col = ((N_PARTITIONS + len(nodes) - 1) / len(nodes))

    for row in pmap:
        for col in range(len(nodes)):
            if node_loads[col][row[col]] < max_load_per_col:
                # This node's load is within the bounds. Confirm the assignment.
                node_loads[col][row[col]] += 1
                continue

            min_swap_col = -1
            min_swap_cost = 99999999

            # Swap with a node having the least cost of swap still within max
            # load.
            for next_col in range(col + 1, len(nodes)):
                if node_loads[col][row[next_col]] >= max_load_per_col:
                    continue

                # Compute a cost for this swap.
                swap_cost = node_loads[next_col][row[col]] + node_loads[col][
                    row[next_col]]

                if swap_cost < min_swap_cost:
                    min_swap_col = next_col
                    min_swap_cost = swap_cost

            if min_swap_col >= 0:
                # Swap the nodes.
                n = row[min_swap_col]
                row[min_swap_col] = row[col]
                row[col] = n

            # Update the load for the node finally at this column.
            node_loads[col][row[col]] += 1

    return pmap


def naive_balance(nodes, pmap, lowered=0):
    replica_counts = [Counter() for _ in range(REPLICATION_FACTOR)]
    target_ptns = (N_PARTITIONS - lowered) / len(nodes)

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


def naive_balance_with_backtrack(nodes, pmap, lowered=0):
    replica_counts = [Counter() for _ in range(REPLICATION_FACTOR)]
    target_ptns = (N_PARTITIONS + (len(nodes) - 1)) / len(nodes)

    if lowered != 0:
        aggressive_target = (N_PARTITIONS - lowered) / len(nodes)
    else:
        aggressive_target = target_ptns

    for pid in xrange(N_PARTITIONS):
        row = pmap[pid]

        for r in range(REPLICATION_FACTOR):
            counts = replica_counts[r]

            if counts[row[r]] >= aggressive_target:
                min_r = r

                for next_r in range(r + 1, len(nodes)):
                    if counts[row[next_r]] < counts[row[min_r]]:
                        min_r = next_r

                n = row[min_r]
                row[min_r] = row[r]
                row[r] = n

            counts[row[r]] += 1

    if len(nodes) <= REPLICATION_FACTOR:
        # No need balancing columns in this case, all nodes will own all data
        # and master column will be evenly distributed by the basic algorithm.
        # return pmap
        pass

    ptns_needed = [Counter() for _ in range(REPLICATION_FACTOR)]
    n_swaps = 0

    for needed, counts in zip(ptns_needed, replica_counts):
        n_match_target = N_PARTITIONS % len(nodes)

        if n_match_target == 0:
            n_match_target = len(nodes)

        t_counts = []

        for node, count in counts.items():
            if n_match_target > 0 and count == target_ptns:
                n_match_target -= 1
                continue

            t_counts.append((count, node))

        desc_counts = sorted(t_counts, reverse=True)

        for value, node in desc_counts:
            if n_match_target > 0:
                target = target_ptns
                n_match_target -= 1
            else:
                target = target_ptns - 1

            n_needed = value - target

            if n_needed > 0:
                n_swaps += n_needed

            needed[node] = n_needed

    if n_swaps == 0:
        return pmap

    for pid in xrange(N_PARTITIONS):
        row = pmap[pid]

        for r in range(REPLICATION_FACTOR):
            needed = ptns_needed[r]

            if needed[row[r]] <= 0:
                continue

            swap_r = r

            for next_r in range(r + 1, len(nodes)):
                if next_r < REPLICATION_FACTOR:
                    # For migration purposes, we prefer these swaps, but they
                    # are not very common. Since these do not offer much
                    # benefit, maybe we shouldn't include them.

                    # FIXME: There is a bug - does better without the following.
                    continue

                    if ptns_needed[next_r][row[r]] > 0 and \
                       ptns_needed[r][row[next_r]] < 0 and \
                       ptns_needed[next_r][row[next_r]] > 0:
                        ptns_needed[r][row[r]] -= 1
                        ptns_needed[r][row[next_r]] += 1
                        ptns_needed[next_r][row[r]] += 1
                        ptns_needed[next_r][row[next_r]] -= 1

                        swap_r = next_r
                        n_swaps -= 2
                else:
                    if needed[row[next_r]] < 0:
                        needed[row[r]] -= 1
                        needed[row[next_r]] += 1

                        swap_r = next_r
                        n_swaps -= 1

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

                    if n_swaps == 0:
                        return pmap

                    break

    return pmap


def two_pass_balance_v1(nodes, pmap):
    replica_counts = [Counter() for _ in range(REPLICATION_FACTOR)]

    for row in pmap:
        for r in range(REPLICATION_FACTOR):
            replica_counts[r][row[r]] += 1

    target_ptns = (N_PARTITIONS + (len(nodes) - 1)) / len(nodes)
    remainder = N_PARTITIONS % len(nodes)

    if remainder == 0:
        replica_targets = [{n: target_ptns for n in nodes}
                           for _ in range(REPLICATION_FACTOR)]
    else:
        distribution = [target_ptns for _ in range(remainder)]
        distribution.extend(
            [target_ptns - 1 for _ in range(len(nodes) - remainder)])
        replica_targets = [{n: v for n, v
                           in zip(nodes, distribution[i:] + distribution[:i])}
                           for i in range(REPLICATION_FACTOR)]

    for pid, row in enumerate(pmap):
        for r, n in enumerate(row[:REPLICATION_FACTOR]):
            counts_at_r = replica_counts[r]
            targets_at_r = replica_targets[r]

            actual_n_at_r = counts_at_r[n]
            target_n_at_r = targets_at_r[n]

            if actual_n_at_r <= target_n_at_r:
                continue
            # else - n needs to gain at r

            swap = False

            for next_r, next_n in enumerate(row[r + 1:len(nodes)], r + 1):
                actual_next_at_r = counts_at_r[next_n]
                target_next_at_r = targets_at_r[next_n]

                if actual_next_at_r >= target_next_at_r:
                    continue
                # else - next_n needs to lose at r

                if next_r < REPLICATION_FACTOR:
                    counts_at_next = replica_counts[next_r]
                    targets_at_next = replica_targets[next_r]

                    actual_n_at_next = counts_at_next[n]
                    target_n_at_next = targets_at_next[n]

                    if actual_n_at_next <= target_n_at_next:
                        continue
                    # else - n needs to gain at next_r

                    actual_next_at_next = counts_at_next[next_n]
                    target_next_at_next = targets_at_next[next_n]

                    if actual_next_at_next >= target_next_at_next:
                        continue
                    # else - next needs to lose at next_r.

                    counts_at_next[n] += 1
                    counts_at_next[next_n] -= 1

                swap = True
                break

            if swap:
                row[next_r] = n
                row[r] = next_n

                counts_at_r[n] -= 1
                counts_at_r[next_n] += 1

    return pmap


def two_pass_balance_v2(nodes, pmap):
    replica_counts = [Counter() for _ in range(REPLICATION_FACTOR)]

    for row in pmap:
        for r in range(REPLICATION_FACTOR):
            replica_counts[r][row[r]] += 1

    target_ptns = N_PARTITIONS / len(nodes)
    replica_targets = [{n: target_ptns for n in nodes}
                       for _ in range(REPLICATION_FACTOR)]

    remainder = N_PARTITIONS % len(nodes)

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replica_targets[0].keys())

        while replica < REPLICATION_FACTOR:
            if n_added < remainder:
                replica_targets[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == len(nodes):
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    for pid, row in enumerate(pmap):
        for r, n in enumerate(row[:REPLICATION_FACTOR]):
            counts_at_r = replica_counts[r]
            targets_at_r = replica_targets[r]

            actual_n_at_r = counts_at_r[n]
            target_n_at_r = targets_at_r[n]

            if actual_n_at_r <= target_n_at_r:
                continue
            # else - n needs to gain at r

            swap_r = r
            actual_swap_at_r = actual_n_at_r

            for next_r, next_n in enumerate(row[r + 1:len(nodes)], r + 1):
                actual_next_at_r = counts_at_r[next_n]
                target_next_at_r = targets_at_r[next_n]

                if actual_next_at_r >= target_next_at_r:
                    continue
                # else - next_n needs to lose at r

                if next_r < REPLICATION_FACTOR:
                    counts_at_next = replica_counts[next_r]
                    targets_at_next = replica_targets[next_r]

                    actual_n_at_next = counts_at_next[n]
                    target_n_at_next = targets_at_next[n]

                    if actual_n_at_next <= target_n_at_next:
                        continue
                    # else - n needs to gain at next_r

                    actual_next_at_next = counts_at_next[next_n]
                    target_next_at_next = targets_at_next[next_n]

                    if actual_next_at_next >= target_next_at_next:
                        continue
                    # else - next needs to lose at next_r.

                if actual_next_at_r < actual_swap_at_r:
                    swap_r = next_r
                    actual_swap_at_r = actual_next_at_r

            if swap_r != r:
                swap_n = row[swap_r]
                row[swap_r] = n
                row[r] = swap_n

                counts_at_r[n] -= 1
                counts_at_r[swap_n] += 1

                if swap_r < REPLICATION_FACTOR:
                    replica_counts[swap_r][n] += 1
                    replica_counts[swap_r][swap_n] -= 1

    return pmap


def naive_balance_enhanced_v1(nodes, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) / len(nodes)

    min_claims = N_PARTITIONS / len(nodes)
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


def naive_balance_enhanced_v2(nodes, pmap, lowered=0):
    replicas_claims = [Counter() for _ in range(REPLICATION_FACTOR)]
    smooth_balance_mark = (N_PARTITIONS - lowered) / len(nodes)

    min_claims = N_PARTITIONS / len(nodes)
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


def main():
    global OUTPUT, SEED

    naive_balance_lowered = []

    for i in [128]:  # , 256]:  # , 512, 1024]:
        fn_name = "with_lowered_{}".format(i)
        naive_balance_lowered.append(sim_partial(
            naive_balance, lowered=i, fn_name=fn_name))

        fn_name = "with_lowered_{}".format(i)
        naive_balance_lowered.append(sim_partial(
            naive_balance_enhanced_v1, lowered=i, fn_name=fn_name))

        fn_name = "with_lowered_{}".format(i)
        naive_balance_lowered.append(sim_partial(
            naive_balance_enhanced_v2, lowered=i, fn_name=fn_name))

    balance_fns = [
        standard_balance,
        # naive_balance_with_backtrack,
        # two_pass_balance_v1,
        two_pass_balance_v2,
        # ashish_balance,
    ]

    balance_fns.extend(naive_balance_lowered)

    for balance_fn in balance_fns:
        with log("Simulating '{}' - seed {} N_NODES {} RF {}",
                 balance_fn.__name__, SEED, N_NODES, REPLICATION_FACTOR):
            simulate(balance_fn)
            log("")

    def do_run(seed):
        global SEED

        SEED = seed  # set seed for this process
        r = {}

        for fn in balance_fns:
            r[fn.__name__] = simulate(fn, do_drop=False)

        return r

    with log("Comparing: seed {} N_RUNS {} N_NODES {} RF {}",
             SEED, N_RUNS, N_NODES, REPLICATION_FACTOR):
        OUTPUT = False
        run_results = []

        executor = ProcessPoolExecutor(max_workers=4)
        n_chunks = N_PROCS * N_CHUNKS_PER_PROC
        run_results = executor.map(
            do_run, (random.randint(1000, 9999) for _ in xrange(N_RUNS)),
            chunksize=N_RUNS / n_chunks)

        OUTPUT = True

        results = {fn.__name__: [] for fn in balance_fns}

        for run_result in run_results:
            for name, value in run_result.items():
                results[name].append(value)

        for name, results in sorted(results.items()):
            column_peaks, node_peaks = zip(*results)

            column_peaks = sorted(column_peaks)
            column_median = column_peaks[len(column_peaks) / 2]
            column_maximum = column_peaks[-1]

            node_peaks = sorted(node_peaks)
            node_median = node_peaks[len(node_peaks) / 2]
            node_maximum = node_peaks[-1]
            runs_excess = sum(1 for p in node_peaks if p > 0)

            with log("{} excesses", name):
                log("Columns: median {} maximum {}", column_median,
                    column_maximum)
                log("Nodes: median {} maximum {} n_runs_exceeding {}",
                    node_median, node_maximum, runs_excess)


if __name__ == '__main__':
    main()
