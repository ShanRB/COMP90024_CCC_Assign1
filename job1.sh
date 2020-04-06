#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:30:00
#SBATCH --output=n1c1.out
#SBATCH --error=err_n1c1.out

echo ' '
echo 'Run rank.py with 1 node 1 core'

module purge
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
mpiexec python rank.py -f bigTwitter.json
