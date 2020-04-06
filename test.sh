#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=00:30:00
 
echo ' '
echo 'Run rank.py with 1 node 8 cores'

module purge
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
mpiexec python HashRank_sctr.py -f smallTwitter.json
