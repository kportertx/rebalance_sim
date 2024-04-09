from collections import Counter

from lib import config, rackaware
from lib.strategies.standard import standard_balance, then_rack_aware2


def init_claims(nodes):
    n_nodes = len(nodes)
    replicas_claims = [Counter() for _ in range(config.REPLICATION_FACTOR)]
    min_claims = config.N_PARTITIONS // n_nodes
    replicas_target_claims = [
        {n: min_claims for n in nodes} for _ in range(config.REPLICATION_FACTOR)
    ]

    remainder = config.N_PARTITIONS % n_nodes

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

                if target_n == n_nodes:
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    return replicas_claims, replicas_target_claims


def rack_balance(nodes, racks, lowered=0):
    """
    Approximation to the shipped uniform balance algorithm since 4.3.
    """
    pmap = standard_balance(nodes)
    n_nodes = len(nodes)
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

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
                or swap_r >= n_racks_needed
                or rackaware.rack_unique_before_r(racks, row, r, swap_r)
            )

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r + 1, n_nodes):
                    if r < n_racks_needed and not rackaware.rack_unique_before_r(
                        racks, row, r, next_r
                    ):
                        continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_node = row[next_r]
                    next_target_claims = replica_target_claims[next_node]
                    next_claims = replica_claims[next_node]
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
                    elif (
                        next_score == swap_score
                        and swap_target_claims > next_target_claims
                    ):
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1

    return pmap


def rack_balance2(nodes, racks, lowered=0):
    """
    Imporoves placement flexibility of 'rack_balance' by allowing subsequent
    replicas to swap with prior replicas. Leads to odd behavior in that replica
    0 may swap with 1 and then 1 may swap back with 0.
    """
    pmap = then_rack_aware2(standard_balance)(nodes, racks)
    n_nodes = len(nodes)
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

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
                or swap_r >= n_racks_needed
                or rackaware.rack_unique_before_r(racks, row, r, swap_r)
            )

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(0, n_nodes):
                    if r < n_racks_needed and not rackaware.rack_unique_before_r(
                        racks, row, r, next_r
                    ):
                        continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_node = row[next_r]
                    next_target_claims = replica_target_claims[next_node]
                    next_claims = replica_claims[next_node]
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
                    elif (
                        next_score == swap_score
                        and swap_target_claims > next_target_claims
                    ):
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score

                if swap_r != r:
                    node = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = node

            replica_claims[row[r]] += 1

    return pmap


def rack_balance2_1(nodes, racks, lowered=0):
    """
    Attempts to remove the "odd behavior" from rack_balance2 - poor resulting
    balance.
    """
    pmap = then_rack_aware2(standard_balance)(nodes, racks)
    n_nodes = len(nodes)
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

    def rack_unique_before_r(racks, row, r, cur_ix):
        cur_node = row[r] if cur_ix <= r else row[cur_ix]
        cur_rack = racks[cur_node]

        for prior_r in range(r):
            prior_rack = racks[row[prior_r]]

            if cur_rack == prior_rack:
                return False

        return True

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
                or swap_r >= n_racks_needed
                or rack_unique_before_r(racks, row, r, swap_r)
            )

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(0, n_nodes):
                    if r < n_racks_needed and not rack_unique_before_r(
                        racks, row, r, next_r
                    ):
                        continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_node = row[next_r]
                    next_target_claims = replica_target_claims[next_node]
                    next_claims = replica_claims[next_node]
                    next_score = next_target_claims - next_claims

                    if not swap_is_rack_safe:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                        swap_is_rack_safe = True

                        continue

                    if (
                        r_claims < smooth_balance_mark
                        and next_r >= config.REPLICATION_FACTOR
                    ):
                        break

                    if next_score > swap_score:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score
                    elif (
                        next_score == swap_score
                        and swap_target_claims > next_target_claims
                    ):
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score

                if swap_r != r:
                    n = row[swap_r]
                    row[swap_r] = row[r]
                    row[r] = n

            replica_claims[row[r]] += 1

    return pmap


def rack_balance3(nodes, racks, lowered=0):
    """ """
    pmap = then_rack_aware2(standard_balance)(nodes, racks)
    n_nodes = len(nodes)
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

    def row_score(row, replicas_claims, replicas_target_claims):
        total = 0

        for r in range(config.REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            target_claims = replica_target_claims[row[r]]
            score = target_claims - r_claims

            total += score

        return total

    def swap(row, a, b):
        n = row[a]
        row[a] = row[b]
        row[b] = n

        return row

    for pid, row in enumerate(pmap):
        for r in range(config.REPLICATION_FACTOR):
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            swap_row_score = row_score(row, replicas_claims, replicas_target_claims)

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_is_rack_safe = (
                swap_r == 0
                or swap_r >= n_racks_needed
                or rackaware.rack_unique_before_r(racks, row, r, swap_r)
            )

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(r, n_nodes):
                    if r < n_racks_needed:
                        if not rackaware.rack_unique_before_r(racks, row, r, next_r):
                            continue  # not rack-safe
                    # else - next_r is rack-safe.

                    next_target_claims = replica_target_claims[row[next_r]]
                    next_row_score = row_score(
                        swap(row[:], r, next_r), replicas_claims, replicas_target_claims
                    )

                    if not swap_is_rack_safe:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_row_score = next_row_score
                        swap_is_rack_safe = True

                        if r_claims < smooth_balance_mark:
                            break

                        continue

                    if next_row_score > swap_row_score:
                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_row_score = next_row_score
                    elif next_row_score == swap_row_score:
                        if swap_target_claims > next_target_claims:
                            swap_r = next_r
                            swap_target_claims = next_target_claims
                            swap_row_score = next_row_score

                if swap_r != r:
                    swap(row, swap_r, r)

            replica_claims[row[r]] += 1

    return pmap


def multipass(nodes, racks):
    pmap = then_rack_aware2(standard_balance)(nodes, racks)
    n_nodes = len(nodes)

    # Init stats
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

    # Maps node & replica column to list of pids having the node in the column.
    node_replica_to_pids = {}

    # Pass 1 - collect stats

    for pid, row in enumerate(pmap):
        replica_racks = set()

        for r in range(config.REPLICATION_FACTOR):
            node = row[r]
            rack = racks[node]
            replica_racks.add(rack)

            node_replica = (node, r)
            pids = node_replica_to_pids[node_replica] = node_replica_to_pids.get(
                node_replica, []
            )
            pids.append(pid)

            replica_claims = replicas_claims[r]
            replica_claims[node] += 1

    # Pass 2 - balance pmap
    # Goal: Balance nodes furthest from ideal for a column first.

    # from pprint import pprint

    # pprint(replica_claims)
    # pprint(racks)
    # pprint(replicas_target_claims)
    # print(f"{n_racks_needed} {len(ra_pids)}, {len(non_ra_pids)}")
    # exit()

    return pmap
