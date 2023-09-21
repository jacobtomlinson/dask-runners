import os
from dask.distributed import Scheduler
from .base import Role, BaseRunner

_RUNNER_REF = None


class WorldTooSmallException(RuntimeError):
    """Not enough MPI ranks to start all required processes."""


class MPIRunner(BaseRunner):
    def __init__(self, *args, **kwargs):
        from mpi4py import MPI

        try:
            self.rank = int(os.environ["SLURM_NODEID"])
            self.world_size = int(os.environ["SLURM_JOB_NUM_NODES"])
        except KeyError:
            raise RuntimeError("Not a SLURM job")
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
