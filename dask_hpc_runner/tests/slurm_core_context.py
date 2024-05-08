from dask.distributed import Client
from dask_hpc_runner import SlurmRunner

with SlurmRunner() as runner:
    with Client(runner) as client:
        assert client.submit(lambda x: x + 1, 10).result() == 11
        assert client.submit(lambda x: x + 1, 20, workers=2).result() == 21
