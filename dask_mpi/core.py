import asyncio
import atexit
import sys

import dask
from dask.distributed import Client, Scheduler, Worker, Nanny


def initialize(
    scheduler_port=0,
    interface=None,
    protocol=None,
    nthreads=0,
    memory_limit="auto",
    local_directory="",
    nanny=False,
    dashboard_address=":8787",
):
    """
    Initialize a Dask cluster using mpi4py

    Using mpi4py, MPI rank 0 is allocated to the Scheduler, and MPI rank 1 is
    allocated to the client script.  All of ther MPI ranks are allocated to
    Workers (or Nannies).  This means that all MPI ranks other than MPI rank 1
    block while their event loops run and exit once shut down.

    Parameters
    ----------
    scheduler_port : int
        Specify scheduler port number.  Defaults to random.
    interface : str
        Network interface like 'eth0' or 'ib0'
    protocol : str
        Network protocol to use like TCP
    nthreads : int
        Number of threads per worker
    memory_limit : int, float, or 'auto'
        Number of bytes before spilling data to disk.  This can be an
        integer (nbytes), float (fraction of total memory), or 'auto'.
    local_directory : str
        Directory to place worker files
    nanny : bool
        Start workers in nanny process for management
    dashboard_address : str
        Address for visual diagnostics dashboard
    """
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:

        async def run_scheduler():
            async with Scheduler(
                interface=interface,
                protocol=protocol,
                dashboard_address=dashboard_address,
                port=scheduler_port,
            ) as s:
                comm.bcast(s.address, root=0)
                comm.Barrier()
                await s.finished()

        asyncio.get_event_loop().run_until_complete(run_scheduler())
        sys.exit()

    else:

        scheduler_address = comm.bcast(None, root=0)
        dask.config.set(scheduler_address=scheduler_address)
        comm.Barrier()

    if rank == 1:

        atexit.register(send_close_signal)

    elif rank > 1:

        async def run_worker():
            WorkerType = Nanny if nanny else Worker
            async with WorkerType(
                interface=interface,
                protocol=protocol,
                nthreads=nthreads,
                memory_limit=memory_limit,
                local_directory=local_directory,
                name=rank,
            ) as w:
                await w.finished()

        asyncio.get_event_loop().run_until_complete(run_worker())
        sys.exit()


def send_close_signal():

    async def stop(dask_scheduler):
        await dask_scheduler.close()
        await asyncio.sleep(0.1)
        local_loop = dask_scheduler.loop
        local_loop.add_callback(local_loop.stop)

    with Client() as c:
        c.run_on_scheduler(stop, wait=False)
