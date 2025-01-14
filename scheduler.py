import csv
import sys

class Process:
    def __init__(self, pid, arrival, burst):
        self.pid = pid
        self.arrival = arrival
        self.burst = burst
        self.remaining = burst
        self.completion_time = 0
        self.waiting_time = 0
        self.turnaround_time = 0

def round_robin(processes, time_quantum):
    time = 0
    queue = []
    completed_processes = []
    context_switches = 0
    idle_time = 0
    
    while processes or queue:
        # Move processes that have arrived to the queue
        while processes and processes[0].arrival <= time:
            queue.append(processes.pop(0))
        
        if queue:
            process = queue.pop(0)
            time_slice = min(time_quantum, process.remaining)
            time += time_slice
            process.remaining -= time_slice

            # If process completed, calculate its metrics
            if process.remaining == 0:
                process.completion_time = time
                process.turnaround_time = time - process.arrival
                process.waiting_time = process.turnaround_time - process.burst
                completed_processes.append(process)
            else:
                queue.append(process)
            
            context_switches += 1
        else:
            # No process in the queue, CPU is idle
            time += 1
            idle_time += 1

    total_burst_time = sum(p.burst for p in completed_processes)
    cpu_utilization = 1 - (idle_time / time)
    throughput = len(completed_processes) / time
    avg_waiting_time = sum(p.waiting_time for p in completed_processes) / len(completed_processes)
    avg_turnaround_time = sum(p.turnaround_time for p in completed_processes) / len(completed_processes)
    
    return {
        "cpu_utilization": cpu_utilization,
        "throughput": throughput,
        "avg_waiting_time": avg_waiting_time,
        "avg_turnaround_time": avg_turnaround_time,
        "context_switches": context_switches,
        "completed_processes": completed_processes
    }

def load_processes(file_path):
    processes = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            processes.append(Process(int(row['pid']), int(row['arrive']), int(row['burst'])))
    return sorted(processes, key=lambda x: x.arrival)

def main():
    if len(sys.argv) != 3:
        print("Usage: python scheduler.py <file_path> <time_quantum>")
        sys.exit(1)

    file_path = sys.argv[1]
    time_quantum = int(sys.argv[2])
    processes = load_processes(file_path)
    
    results = round_robin(processes, time_quantum)
    
    print("CPU Utilization:", results["cpu_utilization"])
    print("Throughput:", results["throughput"])
    print("Average Waiting Time:", results["avg_waiting_time"])
    print("Average Turnaround Time:", results["avg_turnaround_time"])
    print("Context Switches:", results["context_switches"])
    print("\nProcess Details:")
    for process in results["completed_processes"]:
        print(f"PID: {process.pid}, Waiting Time: {process.waiting_time}, Turnaround Time: {process.turnaround_time}")

if __name__ == "__main__":
    main()
