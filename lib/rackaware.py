"""Rack-aware helpers."""


def rack_unique_before_r(racks, row, r, cur_ix):
    cur_rack = racks[row[cur_ix]]

    for prior_r in range(r):
        if cur_rack == racks[row[prior_r]]:
            return False

    return True
