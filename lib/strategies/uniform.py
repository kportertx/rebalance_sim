from collections import Counter

from lib import config, rackaware
from lib.strategies.standard import standard_balance


def rack_balance(nodes, racks, lowered=0):
    """
    Approximation to the shipped uniform balance algorithm since 4.3.
    """
    pmap = standard_balance(nodes)
    replicas_claims = [Counter() for _ in range(config.REPLICATION_FACTOR)]
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // len(nodes)

    min_claims = config.N_PARTITIONS // len(nodes)
    replicas_target_claims = [
        {n: min_claims for n in nodes} for _ in range(config.REPLICATION_FACTOR)
    ]

    remainder = config.N_PARTITIONS % len(nodes)
    n_racks = len(set(racks.values()))

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < config.REPLICATION_FACTOR:
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
        for r in range(config.REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_score = swap_target_claims - r_claims
            swap_is_rack_safe = (
                swap_r == 0
                or swap_r >= n_racks
                or rackaware.rack_unique_before_r(racks, row, swap_r, racks[row[r]])
            )

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r + 1, len(nodes)):
                    if r < n_racks:
                        if not rackaware.rack_unique_before_r(
                            racks, row, r, racks[row[next_r]]
                        ):
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
                            swap_r = next_r
                            swap_target_claims = next_target_claims
                            swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1

    return pmap


def simple_rack_balance(nodes, racks, lowered=0):
    """
    Alternative approximation to the shipped uniform balance algorithm since
    4.3.
    """
    pmap = standard_balance(nodes)
    replicas_claims = [Counter() for _ in range(config.REPLICATION_FACTOR)]
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // len(nodes)

    min_claims = config.N_PARTITIONS // len(nodes)
    replicas_target_claims = [
        {n: min_claims for n in nodes} for _ in range(config.REPLICATION_FACTOR)
    ]

    remainder = config.N_PARTITIONS % len(nodes)
    n_racks = len(set(racks.values()))

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())

        while replica < config.REPLICATION_FACTOR:
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
        for r in range(config.REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_score = swap_target_claims - r_claims
            swap_is_rack_safe = (
                swap_r == 0
                or swap_r >= n_racks
                or rackaware.rack_unique_before_r(racks, row, swap_r, racks[row[r]])
            )

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r + 1, len(nodes)):
                    if r < n_racks:
                        if not rackaware.rack_unique_before_r(
                            racks, row, r, racks[row[next_r]]
                        ):
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


def multipass(nodes, racks):
    pmap = standard_balance(nodes)
    n_nodes = len(nodes)

    # Init stats
    replicas_claims = [Counter() for _ in range(config.REPLICATION_FACTOR)]
    min_claims = config.N_PARTITIONS // n_nodes
    replicas_target_claims = [
        {n: min_claims for n in nodes} for _ in range(config.REPLICATION_FACTOR)
    ]

    remainder = config.N_PARTITIONS % n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0
        sl_ix = sorted(replicas_target_claims[0].keys())
        replica_racks = set()

        while replica < config.REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][sl_ix[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == n_nodes:
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    # Maps node & replica column to list of pids having the node in the column.
    node_replica_to_pids = {}

    # Pass 1 - collect stats
    ra_pids = set()
    non_ra_pids = set()

    for pid, row in enumerate(pmap):
        replica_racks = set()

        for r in range(config.REPLICATION_FACTOR):
            node = row[r]
            rack = racks[node]
            replica_racks.add(rack)

            replica_to_pids = node_replica_to_pids = node_replica_to_pids.get(node, {})
            pids = replica_to_pids = replica_to_pids.get(r, [])
            pids.append(pid)

            replica_claims = replicas_claims[r]
            replica_claims[node] += 1

        # Check if partition is rack-aware.
        if len(replica_racks) >= n_racks_needed:
            ra_pids.add(pid)
        else:
            non_ra_pids.add(pid)

    # Pass 2 - balance pmap
    # Goal: Balance nodes furthest from idea for a column first.

    # from pprint import pprint

    # pprint(replica_claims)
    # pprint(racks)
    # pprint(replicas_target_claims)
    # print(f"{n_racks_needed} {len(ra_pids)}, {len(non_ra_pids)}")
    # exit()

    return pmap
