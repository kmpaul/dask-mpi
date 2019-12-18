from time import sleep
from distributed import Client
from distributed.metrics import time

from dask_mpi import initialize
from dask_mpi.tests.utils import check_port_okay

dashboard_port = 59583

initialize(dashboard_address=f":{dashboard_port}")

check_port_okay(dashboard_port)
