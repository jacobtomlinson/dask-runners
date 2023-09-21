# Dask HPC Runner

> [!WARNING]
> This repo is experimental.

## Overview

Inspired by the unmerged changes in [dask/distributed#4710](https://github.com/dask/distributed/pull/4710) and [dask/dask-mpi#69](https://github.com/dask/dask-mpi/pull/69) this repo implements a `Runner` deployment model for Dask.

### `Runner` vs `Cluster`

To understand what a `Runner` is let's first discuss Dask's cluster manager deployment model. Dask had many implementations of a cluster manager which is a Python class that handles the creation of a Dask cluster. It does this by creating the processes required to run the cluster via some kind of resource manager.

For example the `dask.distributed.LocalCluster` class creates subprocesses of the parent process with the scheduler and worker components, the `dask.distributed.SSHCluster` opens SSH connections to other systems and starts processes there, and the `dask_jobqueue.SLURMCluster` class uses the [SLURM Workload Manager](https://slurm.schedmd.com/documentation.html) to submit jobs to an HPC system for each Dask process.

Critically the cluster manager coordinates service discovery externally. It creates the scheduler first, and therefore knows the network address of the scheduler, then it creates the workers and tells them where to find the scheduler at creation time. This generally feels like reaching out from the machine you are working on and creating a distributed cluster from nothing.

The model of a `Runner` is very different. Instead of creating processes/jobs/containers/etc it acts from within an existing multi-process application. For example on an HPC system users may submit a job that requires hundreds of cores, and the workload manager will allocate that on many nodes of the machine and then start the same script/application on every node.

It is then the job of each instance of the application to discover and coordinate with the other nodes. There is no central point of creation that has knowledge of the scheduler address, the processes have to fend for themselves and ultimately choose who will be the scheduler. This model feels more like blowing up a balloon inside a box, we aren't creating a cluster from nothing, instead we are taking an existing space and populating it with one.

In order to initialise a cluster in this distributed fashion the processes need some kind of method of communication, some systems will tell each process enough information for it to work out what to do, others will require some kind of [distributed consensus mechanism](https://en.wikipedia.org/wiki/Raft_(algorithm)) to conduct leadership election.

## Implementations

This repo contains a `BaseRunner` class to help implement runners for various systems via Python context managers.

The runner assumes that many instances of itself are being created, and it needs a method of communication and ideally a method if self-identification in order to construct a healthy cluster. Only one process will execute the client code (the contents of the context manager), the rest will instead start Dask components and then exit when the context manager exits on the client node.

### `MPIRunner`

Inspired by `dask-mpi` the `MPIRunner` class uses [MPI](https://en.wikipedia.org/wiki/Message_Passing_Interface) to handle communication and identification.

Each process can use the `mpi4py.MPI.COMM_WORLD` to communicate with other processes and find out it's rank (a unique monotonic index that is assigned to each process) to use as it's ID and the world size (how many processes there are in total).

- The process with rank `0` assumes it is the scheduler, it starts the scheduler process and broadcasts it's network address over the MPI comm.
- The process with rank `1` assumes it should run the client code, it waits for the scheduler address to be broadcast and then continues running the contents of the context manager.
- All processes with rank `2` and above assume they are workers, they wait for the scheduler address to be broadcast and then start worker processes that connect to the scheduler.

```python
from dask.distributed import Client
from dask_hpc_runner import MPIRunner

# Only rank 1 will execute the contents of the context manager
# the rest will start the Dask cluster components instead
with MPIRunner() as runner:
    # The runner object contains the scheduler address and can be passed directly to a client
    with Client(runner) as client:
        # We can wait for all the workers to be ready before continuing
        client.wait_for_workers(runner.n_workers)

        # Then we can submit some work to the cluster
        assert client.submit(lambda x: x + 1, 10).result() == 11
        assert client.submit(lambda x: x + 1, 20, workers=2).result() == 21
```

### `SLURMRunner`

> [!NOTE]
> `SLURMRunner` is just an idea that could be implemented in the future.

This runner could use the environment variables set in each process for self identification and a shared filesystem for communication.

Each process can use the `SLURM_NODEID` for it's ID (a unique monotonic index that is assigned to each process) and the `SLURM_JOB_NUM_NODES` to know the total number of processes.

- The process with rank `0` assumes it is the scheduler, it writes a [scheduler file](https://docs.dask.org/en/latest/deploying-cli.html#dask-scheduler) to the shared filesystem.
- The process with rank `1` assumes it should run the client code, it waits for the scheduler file to exist and then continues running the contents of the context manager.
- All processes with rank `2` and above assume they are workers, they wait for the scheduler file to exist and then start worker processes that connect to the scheduler.
