from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

numDataPerPack = 10
sendbuf = np.linspace(rank*numDataPerPack, (rank+1)*numDataPerPack,numDataPerPack)
print('Rank: ',rank, ', sendbuf: ', sendbuf)

recvbuf = None
if rank == 0:
    recvbuf = np.empty(numDataPerPack*size, dtype='d')

comm.Gather(sendbuf, recvbuf, root=0)

if rank ==0:
    print('Rank: ', rank, ', recvbuf received: ', recvbuf)
