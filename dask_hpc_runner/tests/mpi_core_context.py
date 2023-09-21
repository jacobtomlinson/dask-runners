from time import sleep

from distributed import Client
from distributed.metrics import time

from dask_hpc_runner import MPIRunner

with MPIRunner() as runner:
    with Client(runner) as client:
        client.wait_for_workers(2)

        assert client.submit(lambda x: x + 1, 10).result() == 11
        assert client.submit(lambda x: x + 1, 20, workers=2).result() == 21
