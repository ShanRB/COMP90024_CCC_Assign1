#!/bin/bash
# Created by the University of Melbourne job script generator for SLURM
# Tue Mar 31 2020 21:53:06 GMT+1100 (澳大利亚东部夏令时间)

# Partition for the job:
#SBATCH --partition=cloud

# The name of the job:
#SBATCH --job-name="slurm_test_331"

# The project ID which this job should run under:
#SBATCH --account="CCC"

# Maximum number of tasks/CPU cores used by the job:
#SBATCH --ntasks=5



# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
# ends successfully
#SBATCH --mail-type=END

# Use this email address:
#SBATCH --mail-user=yaweis@student.unimelb.edu.au

# The maximum running time of the job in days-hours:mins:sec
#SBATCH --time=0-1:0:00

# check that the script is launched with sbatch
if [ "x$SLURM_JOB_ID" == "x" ]; then
   echo "You need to submit your job to the queuing system with sbatch"
   exit 1
fi

# Run the job from the directory where it was launched (default)

# The modules to load:
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9

# The job command(s):
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9
cd /home/yaweis
mpirun -n 8 python 6_mpi_reduce.py
