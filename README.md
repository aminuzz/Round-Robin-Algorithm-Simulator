# Round Robin CPU Scheduling Simulator

## Overview
This project simulates the Round Robin (RR) CPU scheduling algorithm in Python. It demonstrates how different time quantum values impact performance metrics like CPU utilization, throughput, waiting time, and turnaround time.

## Features
- Simulates Round Robin scheduling with custom time quantum values.
- Calculates performance metrics to analyze scheduling efficiency.
- Comes with a detailed report (PDF) containing visual analysis of results across different time quantum values.

## Usage
Run the simulator from the command line with a CSV of processes and a time quantum:

```bash
python scheduler.py processes.csv 3
```

- `processes.csv` — input file with the columns `pid,arrive,burst`
- `3` — the time quantum (any positive integer)

The program prints the overall scheduling metrics (CPU utilization, throughput, average waiting time, average turnaround time, and context switches), followed by the waiting and turnaround time for each process.

## Repository Contents
- **scheduler.py**: Python script for the simulation.
- **processes.csv**: Sample input file with process details.
- **RoundRobin_Report.pdf**: Detailed project report, including:
  - Introduction to CPU scheduling and the Round Robin algorithm.
  - Implementation details and instructions for running the simulation.
  - Performance analysis with visual results.
- **LICENSE**: MIT License.
