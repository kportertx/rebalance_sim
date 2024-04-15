import hashlib

from lib import config, rackaware


def _create_hash(partition, node):
    return (hashlib.md5(f"{partition}{node}".encode()).hexdigest(), node)


def standard_balance(nodes, racks=None):
    """
    Approximation to the standard rebalance algorithm since 3.13.
    """

    pmap = []

    for partition in range(0, config.N_PARTITIONS):
        row = [_create_hash(partition, n) for n in nodes]
        row.sort(key=lambda hn: hn[0])
        row = [hn[1] for hn in row]

        pmap.append(row)

    return pmap


def then_rack_aware(balance_fn):
    """
    Approximation to the shipped rack-aware algorithm since 3.13.
    """

    def do_rack_aware(nodes, racks, **kwargs):
        pmap = balance_fn(nodes, racks, **kwargs)

        n_needed = min(
            [len(set(racks.values())), len(nodes), config.REPLICATION_FACTOR]
        )

        for row in pmap:
            next_r = n_needed

            for cur_r in range(1, n_needed):
                if rackaware.rack_unique_before_r(racks, row, cur_r, cur_r):
                    continue

                swap_r = cur_r

                for next_r in range(next_r, len(nodes)):
                    next_n = row[next_r]

                    if rackaware.rack_unique_before_r(racks, row, cur_r, next_n):
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

    do_rack_aware.__name__ = f"{balance_fn.__name__}_then_rack_aware"

    return do_rack_aware


def then_rack_aware2(balance_fn):
    """
    Changes the rack-aware adjustments to allow adjusting within the RF nodes.
    This reduces the number of replica positions affected by adding/removing a
    node[s] to the number of nodes added/removed.

    Observing about 10% reduced migrations counts.
    """

    def do_rack_aware(nodes, racks, **kwargs):
        pmap = balance_fn(nodes, racks, **kwargs)

        n_needed = min(
            [len(set(racks.values())), len(nodes), config.REPLICATION_FACTOR]
        )

        for row in pmap:
            for cur_r in range(1, n_needed):
                swap_r = cur_r

                for next_r in range(cur_r, len(nodes)):
                    if rackaware.rack_unique_before_r(racks, row, cur_r, next_r):
                        swap_r = next_r
                        break

                if cur_r != swap_r:
                    swap_n = row[swap_r]
                    row[swap_r] = row[cur_r]
                    row[cur_r] = swap_n

        return pmap

    do_rack_aware.__name__ = f"{balance_fn.__name__}_then_rack_aware2"

    return do_rack_aware


def stable_rack_aware_positions(nodes, racks):
    """
    The idea was to stabilize the rack positions using consistent hashing. This
    seemed like a good idea at the time, but this would actually add additional
    constraints to a UB adjuster making it harder to achieve UB.
    The "the_rack_aware2" achieve the same results but allows fleximble rack
    placement.
    """
    pmap = standard_balance(nodes)
    rack_ids = tuple(set(racks.values()))
    n_racks = len(rack_ids)

    if n_racks == 1:
        return pmap

    rack_pmap = standard_balance(rack_ids)
    n_nodes = len(nodes)

    for (rack_row, node_row) in zip(rack_pmap, pmap):
        for r in range(min(n_racks, config.REPLICATION_FACTOR)):
            target_rack = rack_row[r]

            for n in range(r, n_nodes):
                cur_rack = racks[node_row[n]]

                if cur_rack != target_rack:
                    continue

                if r == n:
                    break  # nothing to do

                swap_node = node_row[r]
                node_row[r] = node_row[n]

                # Shift nodes right to fill in node_row[n].
                while n - 1 > r:
                    node_row[n] = node_row[n - 1]
                    n -= 1

                node_row[r + 1] = swap_node

                break

    return pmap
