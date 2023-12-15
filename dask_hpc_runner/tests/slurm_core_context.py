from dask.distributed import Client
from dask_hpc_runner import  SlurmRunner

with SlurmRunner(
    scheduler_options={"scheduler_file":"scheduler.json"},
    worker_options={"scheduler_file":"scheduler.json"},
) as runner:
    with Client(scheduler_file="scheduler.json") as client:
        assert client.submit(lambda x: x + 1, 10).result() == 11
        assert client.submit(lambda x: x + 1, 20, workers=2).result() == 21
