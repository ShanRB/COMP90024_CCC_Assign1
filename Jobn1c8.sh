#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=00:30:00
#SBATCH --output=n1c8.out
#SBATCH --error=err_n1c8.out
 
echo ' '
echo 'Run with 1 node 8 cores'

module purge
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
mpiexec python rankTwitter.py -f bigTwitter.json
