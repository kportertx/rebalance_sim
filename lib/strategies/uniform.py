import heapq
from collections import Counter

from lib import config, rackaware
from lib.strategies.standard import standard_balance, then_rack_aware2


def init_claims(nodes):
    n_nodes = len(nodes)
    replicas_claims = [Counter() for _ in range(config.REPLICATION_FACTOR)]
    min_claims = config.N_PARTITIONS // n_nodes
    replicas_target_claims = [
        Counter({n: min_claims for n in nodes})
        for _ in range(config.REPLICATION_FACTOR)
    ]

    remainder = config.N_PARTITIONS % n_nodes

    if remainder != 0:
        target_n = 0
        replica = 0
        n_added = 0

        while replica < config.REPLICATION_FACTOR:
            if n_added < remainder:
                replicas_target_claims[replica][nodes[target_n]] += 1

                n_added += 1
                target_n += 1

                if target_n == n_nodes:
                    target_n = 0

                if n_added == remainder:
                    n_added = 0
                    replica += 1

    return replicas_claims, replicas_target_claims


def rack_balance(nodes, racks, lowered=0, pmap=None):
    """
    Approximation to the shipped uniform balance algorithm since 4.3.
    """
    if pmap is None:
        pmap = standard_balance(nodes)

    n_nodes = len(nodes)
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

    for row in pmap:
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
    Somehow improved balance by applying `rack_aware2` first.
    """
    pmap = then_rack_aware2(standard_balance)(nodes, racks)
    n_nodes = len(nodes)
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

    for row in pmap:
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

    for row in pmap:
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

    for row in pmap:
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


def rack_balance4(nodes, racks, lowered=0):
    """
    Priortized replica swaps with the lowest initial score.
    Improves balance but also results in a of extra migrations.
    """
    pmap = then_rack_aware2(standard_balance)(nodes, racks)
    n_nodes = len(nodes)
    replicas_claims, replicas_target_claims = init_claims(nodes)
    smooth_balance_mark = (config.N_PARTITIONS - lowered) // n_nodes
    n_racks = len(set(racks.values()))
    n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

    for row in pmap:
        scores = sorted(
            (replicas_target_claims[r][row[r]] - replicas_claims[r][row[r]], r)
            for r in range(config.REPLICATION_FACTOR)
        )
        replica_racks = set()

        for swap_score, r in scores:
            replica_claims = replicas_claims[r]
            replica_target_claims = replicas_target_claims[r]
            r_claims = replica_claims[row[r]]

            swap_r = r
            swap_target_claims = replica_target_claims[row[swap_r]]
            swap_is_rack_safe = (
                r >= n_racks_needed or racks[row[r]] not in replica_racks
            )

            if not swap_is_rack_safe or r_claims >= smooth_balance_mark:
                for next_r in range(0, n_nodes):
                    if r < n_racks_needed and racks[row[next_r]] in replica_racks:
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

            replica_racks.add(racks[row[r]])
            replica_claims[row[r]] += 1

        assert len(replica_racks) >= n_racks_needed

    return pmap


class Multipass:
    __name__ = "multipass"

    def excess_count(
        self,
        row: list,
        node_claims: dict,
        node_target_claims: int,
        node_extra_claim: dict,
    ) -> int:
        n_excess = 0

        for r in range(config.REPLICATION_FACTOR):
            node = row[r]

            if node_claims[node] > node_target_claims + node_extra_claim[node]:
                n_excess += 1

        return n_excess

    def swap_nodes(self, row: list, a_ix: int, b_ix: int) -> None:
        """
        Swap node or move b to a_ix and a_ix to the beginning
        of the non-replicas.
        """
        swap = row[a_ix]
        row[a_ix] = row[b_ix]
        row[b_ix] = swap

    def move_nodes(self, row: list, a_ix: int, b_ix: int, prior_moves: int) -> None:
        """
        Move b_ix to a_ix and a_ix to the beginning + prior_moves of the
        non-replicas.
        """

        assert a_ix < b_ix and b_ix >= config.REPLICATION_FACTOR

        b = row.pop(b_ix)
        a = row[a_ix]

        row[a_ix] = b
        row.insert(config.REPLICATION_FACTOR + prior_moves, a)

    def __call__(self, nodes, racks):
        pmap = then_rack_aware2(standard_balance)(nodes, racks)

        # Phase 1 - Balance nodes.
        # Create heapq of partitions scored by number of replica nodes holding
        # excessive partitions.

        node_claims = Counter()

        for row in pmap:
            for r in range(config.REPLICATION_FACTOR):
                node_claims[row[r]] += 1

        n_nodes = len(nodes)
        node_target_claims = (
            config.N_PARTITIONS * config.REPLICATION_FACTOR
        ) // n_nodes
        node_target_remainder = (
            config.N_PARTITIONS * config.REPLICATION_FACTOR
        ) % n_nodes
        node_extra_claim = {
            node: 1 if i < node_target_remainder else 0
            for (i, node) in enumerate(nodes)
        }
        heap = []
        n_racks = len(set(racks.values()))
        n_racks_needed = min(n_nodes, n_racks, config.REPLICATION_FACTOR)

        for pid, row in enumerate(pmap):
            n_excess = -self.excess_count(
                row, node_claims, node_target_claims, node_extra_claim
            )

            if n_excess == 0:
                continue

            heap.append((n_excess, pid))

        heapq.heapify(heap)

        # print(n_nodes, node_target_claims, node_target_remainder, node_extra_claim)
        # print(node_claims)
        # print(sorted(heap))

        while len(heap) > 0:
            prior_excess, pid = heapq.heappop(heap)
            row = pmap[pid]

            cur_excess = -self.excess_count(
                row, node_claims, node_target_claims, node_extra_claim
            )

            if cur_excess == 0:
                continue

            if cur_excess != prior_excess:
                heapq.heappush(heap, (cur_excess, pid))
                continue

            prior_moves = 0

            for r in range(config.REPLICATION_FACTOR):
                node = row[r]
                score = node_claims[node]

                if score <= node_target_claims + node_extra_claim[node]:
                    continue

                # TODO: Would be better to just exclude other racks below rf.
                target_rack = racks[node]

                swap_ix = r
                swap_score = score

                for next_r in range(config.REPLICATION_FACTOR, n_nodes):
                    next_node = row[next_r]
                    next_score = node_claims[next_node]

                    if next_score < swap_score and racks[next_node] == target_rack:
                        swap_ix = next_r
                        swap_score = next_score

                if swap_ix != r:
                    node = row[r]
                    swap_node = row[swap_ix]

                    self.move_nodes(row, r, swap_ix, prior_moves)
                    prior_moves += 1
                    node_claims[node] -= 1
                    node_claims[swap_node] += 1

            replica_racks = set()

            for r in range(config.REPLICATION_FACTOR):
                replica_racks.add(racks[row[r]])

            assert len(replica_racks) >= n_racks_needed, (
                pid,
                replica_racks,
                row,
                racks,
            )

        # Phase 2 - balance replicas only (doesn't result in extra migrations.
        replicas_claims, replicas_target_claims = init_claims(nodes)

        for row in pmap:
            for r in range(config.REPLICATION_FACTOR):
                replicas_claims[r][row[r]] += 1

        print(
            "start",
            list(
                dict(sorted((rtc - rc).items()))
                for rtc, rc in zip(replicas_target_claims, replicas_claims)
            ),
        )

        for row in pmap:
            for r in range(config.REPLICATION_FACTOR):
                orig_node = row[r]
                replica_claims = replicas_claims[r]
                replica_target_claims = replicas_target_claims[r]
                r_claims = replica_claims[orig_node]

                swap_r = r
                swap_target_claims = replica_target_claims[orig_node]
                swap_score = swap_target_claims - r_claims

                for next_r in range(config.REPLICATION_FACTOR):
                    if r == next_r:
                        continue

                    next_node = row[next_r]
                    next_target_claims = replica_target_claims[next_node]
                    next_claims = replica_claims[next_node]
                    next_score = next_target_claims - next_claims

                    if next_score > swap_score or (
                        next_score == swap_score
                        and swap_target_claims > next_target_claims
                    ):
                        dest_replica_claims = replicas_claims[next_r]
                        dest_replica_target_claims = replicas_target_claims[next_r]
                        dest_claims = dest_replica_claims[orig_node]
                        dest_target_claims = dest_replica_target_claims[orig_node]
                        dest_score = dest_target_claims - dest_claims

                        if dest_score < next_score:
                            continue

                        swap_r = next_r
                        swap_target_claims = next_target_claims
                        swap_score = next_score

                if swap_r != r:
                    orig_node = row[r]
                    swap_node = row[swap_r]
                    orig_replica_claims = replicas_claims[r]
                    swap_replica_claims = replicas_claims[swap_r]

                    self.swap_nodes(row, r, swap_r)
                    orig_replica_claims[orig_node] -= 1
                    swap_replica_claims[orig_node] += 1

                    orig_replica_claims[swap_node] += 1
                    swap_replica_claims[swap_node] -= 1

        # pmap = rack_balance(nodes, racks, lowered=128, pmap=pmap)

        # Pass 3 - balance pmap
        # Goal: Balance nodes furthest from ideal for a column first.
        print(
            "end",
            list(
                dict(sorted((rtc - rc).items()))
                for rtc, rc in zip(replicas_target_claims, replicas_claims)
            ),
        )
        # exit()
        # pprint(racks)
        # print(f"{n_racks_needed} {len(ra_pids)}, {len(non_ra_pids)}")
        # exit()

        # print(
        #     "before scores",
        #     list(a - b for (a, b) in zip(replicas_target_claims, before_claims)),
        # )
        # print(
        #     "after scores",
        #     list(a - b for (a, b) in zip(replicas_target_claims, replicas_claims)),
        # )

        return pmap


multipass = Multipass()
