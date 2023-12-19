import asyncio
import atexit
import json
import os
from pathlib import Path

import dask
from dask.distributed import Scheduler
from .base import Role, BaseRunner

_RUNNER_REF = None


class WorldTooSmallException(RuntimeError):
    """Not enough Slurm tasks to start all required processes."""


class SlurmRunner(BaseRunner):
    def __init__(self, *args, scheduler_file="scheduler-{}.json", **kwargs):
        try:
            self.rank = int(os.environ["SLURM_PROCID"])
            self.world_size = self.n_workers = int(os.environ["SLURM_NTASKS"])
            self.job_id = int(os.environ["SLURM_JOB_ID"])
        except KeyError as e:
            raise RuntimeError("SLURM_PROCID, SLURM_NTASKS, and SLURM_JOB_ID must be present "
                               "in the environment."
                               ) from e
        if not scheduler_file:
            scheduler_file = kwargs.get("scheduler_options",{}).get("scheduler_file")

        if not scheduler_file:
            raise RuntimeError("scheduler_file must be specified in either the "
                               "scheduler_options or as keyword argument to SlurmRunner.")

        # Encourage filename uniqueness by inserting the job ID
        scheduler_file = scheduler_file.format(self.job_id)
        scheduler_file = Path(scheduler_file)

        if isinstance(kwargs.get("scheduler_options"), dict):
            kwargs["scheduler_options"]["scheduler_file"] = scheduler_file
        else:
            kwargs["scheduler_options"] = {"scheduler_file": scheduler_file}
        if isinstance(kwargs.get("worker_options"), dict):
            kwargs["worker_options"]["scheduler_file"] = scheduler_file
        else:
            kwargs["worker_options"] = {"scheduler_file": scheduler_file}

        self.scheduler_file = scheduler_file

        super().__init__(*args, **kwargs)

    async def get_role(self) -> str:
        if self.scheduler and self.client and self.world_size < 3:
            raise WorldTooSmallException(
                f"Not enough Slurm tasks to start cluster, found {self.world_size}, "
                "needs at least 3, one each for the scheduler, client and a worker."
            )
        elif self.scheduler and self.world_size < 2:
            raise WorldTooSmallException(
                f"Not enough Slurm tasks to start cluster, found {self.world_size}, "
                "needs at least 2, one each for the scheduler and a worker."
            )
        self.n_workers -= int(self.scheduler) + int(self.client)
        if self.rank == 0 and self.scheduler:
            return Role.scheduler
        elif self.rank == 1 and self.client:
            return Role.client
        else:
            return Role.worker

    async def set_scheduler_address(self, scheduler: Scheduler) -> None:
        return

    async def get_scheduler_address(self) -> str:
        return

    async def on_scheduler_start(self, scheduler: Scheduler) -> None:
        return

    async def before_worker_start(self) -> None:
        while not self.scheduler_file.exists():
            await asyncio.sleep(0.2)
        self.load_scheduler_address()

    async def before_client_start(self) -> None:
        while not self.scheduler_file.exists():
            await asyncio.sleep(0.2)
        self.load_scheduler_address()

    def load_scheduler_address(self):
        with self.scheduler_file.open() as f:
            cfg = json.load(f)
        self.scheduler_address = cfg["address"]

    async def get_worker_name(self) -> str:
        return self.rank

    async def _close(self):
        await super()._close()


def initialize(
    interface=None,
    nthreads=1,
    local_directory="",
    memory_limit="auto",
    nanny=False,
    dashboard=True,
    dashboard_address=":8787",
    protocol=None,
    worker_class="distributed.Worker",
    worker_options=None,
    scheduler_file="scheduler-{}.json",
):
    """
    Initialize a Dask cluster using Slurm

    Using Slurm, the user launches 3 or more tasks, usually with the 'srun' command.
    The first task becomes the scheduler, the second task becomes the client, and the
    remaining tasks become workers.  Each task identifies its task ID using the
    'SLURM_PROCID' environment variable, which is like the MPI rank.  The scheduler
    address is communicated using the 'scheduler_file' keyword argument.

    Parameters
    ----------
    interface : str
        Network interface like 'eth0' or 'ib0'
    nthreads : int
        Number of threads per worker
    local_directory : str
        Directory to place worker files
    memory_limit : int, float, or 'auto'
        Number of bytes before spilling data to disk.  This can be an
        integer (nbytes), float (fraction of total memory), or 'auto'.
    nanny : bool
        Start workers in nanny process for management (deprecated, use worker_class instead)
    dashboard : bool
        Enable Bokeh visual diagnostics
    dashboard_address : str
        Bokeh port for visual diagnostics
    protocol : str
        Protocol like 'inproc' or 'tcp'
    worker_class : str
        Class to use when creating workers
    worker_options : dict
        Options to pass to workers
    scheduler_file : str
        Filename to use when saving scheduler connection information. A format placeholder
        will be replaced with the job ID.
    """

    scheduler_options = {
        "interface": interface,
        "protocol": protocol,
        "dashboard": dashboard,
        "dashboard_address": dashboard_address,
        "scheduler_file": scheduler_file,
    }
    worker_options = {
        "interface": interface,
        "protocol": protocol,
        "nthreads": nthreads,
        "memory_limit": memory_limit,
        "local_directory": local_directory,
    }
    worker_class = "dask.distributed.Nanny" if nanny else "dask.distributed.Worker"
    runner = SlurmRunner(
        scheduler_options=scheduler_options,
        worker_class=worker_class,
        worker_options=worker_options,
    )
    dask.config.set(scheduler_file=scheduler_file)
    _RUNNER_REF = runner  # Keep a reference to avoid gc
    atexit.register(_RUNNER_REF.close)
    return runner
