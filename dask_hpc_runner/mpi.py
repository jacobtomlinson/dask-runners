import atexit

import dask
from dask.distributed import Scheduler
from .base import Role, BaseRunner

_RUNNER_REF = None


class WorldTooSmallException(RuntimeError):
    """Not enough MPI ranks to start all required processes."""


class MPIRunner(BaseRunner):
    def __init__(self, *args, comm=None, **kwargs):
        self.comm = comm
        if not self.comm:
            from mpi4py import MPI

            self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.world_size = self.n_workers = self.comm.Get_size()
        super().__init__(*args, **kwargs)

    async def get_role(self) -> str:
        if self.scheduler and self.client and self.world_size < 3:
            raise WorldTooSmallException(
                f"Not enough MPI ranks to start cluster, found {self.world_size}, "
                "needs at least 3, one each for the scheduler, client and a worker."
            )
        elif self.scheduler and self.world_size < 2:
            raise WorldTooSmallException(
                f"Not enough MPI ranks to start cluster, found {self.world_size}, "
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
        self.comm.bcast(scheduler.address, root=0)

    async def get_scheduler_address(self) -> str:
        return self.comm.bcast(None, root=0)

    async def on_scheduler_start(self, scheduler: Scheduler) -> None:
        self.comm.Barrier()

    async def before_worker_start(self) -> None:
        self.comm.Barrier()

    async def before_client_start(self) -> None:
        self.comm.Barrier()

    async def get_worker_name(self) -> str:
        return self.rank

    async def _close(self):
        await super()._close()
        self.comm.Abort()


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
):
    """
    Initialize a Dask cluster using mpi4py

    Using mpi4py, MPI rank 0 launches the Scheduler, MPI rank 1 passes through to the
    client script, and all other MPI ranks launch workers.  All MPI ranks other than
    MPI rank 1 block while their event loops run and exit once shut down.

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
    """

    scheduler_options = {
        "interface": interface,
        "protocol": protocol,
        "dashboard": dashboard,
        "dashboard_address": dashboard_address,
    }
    worker_options = {
        "interface": interface,
        "protocol": protocol,
        "nthreads": nthreads,
        "memory_limit": memory_limit,
        "local_directory": local_directory,
    }
    worker_class = "dask.distributed.Nanny" if nanny else "dask.distributed.Worker"
    runner = MPIRunner(
        scheduler_options=scheduler_options,
        worker_class=worker_class,
        worker_options=worker_options,
    )
    dask.config.set(scheduler_address=runner.scheduler_address)
    _RUNNER_REF = runner  # Keep a reference to avoid gc
    atexit.register(_RUNNER_REF.close)
    return runner
