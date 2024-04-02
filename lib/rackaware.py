"""Rack-aware helpers."""


def rack_unique_before_r(racks, row, r, cur_rack):
    for prior_r in range(r):
        prior_rack = racks[row[prior_r]]

        if cur_rack == prior_rack:
            return False

    return True
