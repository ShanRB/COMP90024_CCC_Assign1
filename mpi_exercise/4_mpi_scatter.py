from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
size = comm.Get_size()  # gives number of ranks in comm
rank = comm.Get_rank()

numDataPerPack = 10
data = None

print('size is ', size)
if rank == 0:
    data = np.linspace(1,size*numDataPerPack, numDataPerPack*size)

recvbuf = np.empty(numDataPerPack, dtype ='d')  #allocate space for buf
comm.Scatter(data, recvbuf, root=0)

print('Rank: ', rank, ', recvbuf received: ',  recvbuf)
