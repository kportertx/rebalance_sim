import os
import random


N_PARTITIONS = 4096
RACKS = (4, 4, 4)
REPLICATION_FACTOR = max(2, len(RACKS))
N_RUNS = 100

NODE_NAMES = tuple(
    name
    for sublist in (
        (n + str(s) for n in (chr(65 + i) for i in range(26))) for s in range(10)
    )
    for name in sublist
)
SEED = 7973  # random.SystemRandom().randint(1000, 9999)  # 7973

OUTPUT = True
DISABLE_MAX_OUTAGE = True
DO_DROP = True
_cpu_count = os.cpu_count()
N_PROCS = 1 if _cpu_count is None else _cpu_count
