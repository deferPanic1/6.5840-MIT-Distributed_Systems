# MIT 6.5840 Distributed Systems

[MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/index.html). – link to the course home page

# Lab 1: MapReduce

Based on the original [MapReduce paper](https://research.google/pubs/pub62/).

---

## Overview

The system consists of two components:
- **Coordinator** — hands out tasks to workers, tracks progress, and reassigns tasks from failed workers
- **Worker** — requests tasks from the coordinator, executes Map or Reduce functions, and writes results to disk

Workers communicate with the coordinator via **Unix socket RPC**.

---

## Implementation Details

### Coordinator (`mr/coordinator.go`)

- Manages two task queues: **map tasks** and **reduce tasks**
- Tracks three phases: `MapPhase → ReducePhase → AllDone`
- Each task has a status: `Waiting → InProgress → Done`
- A background goroutine runs every 5 seconds and reassigns tasks that have been `InProgress` for more than **10 seconds** (worker assumed dead)

### Worker (`mr/worker.go`)

- Runs in a loop: requests a task → executes it → reports completion
- **Map task**: reads input file, runs `mapf`, writes intermediate key-value pairs to `mr-X-Y`
- **Reduce task**: reads all `mr-*-Y` files, sorts by key, runs `reducef`, writes output to `mr-out-Y`
- Keys are distributed across reduce buckets via `ihash(key) % NReduce`

---

## File Naming Convention

| File | Description |
|------|-------------|
| `mr-X-Y` | Intermediate file from Map task `X` for Reduce bucket `Y` |
| `mr-out-Y` | Final output from Reduce task `Y` |

For example, with 8 input files and `NReduce=10`:
- Map produces: `mr-0-0 ... mr-7-9` (80 files total)
- Reduce reads: `mr-*-Y` for each reduce task `Y`
- Reduce writes: `mr-out-0 ... mr-out-9`

---

## Running

### Build the plugin

```bash
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
```

### Start the coordinator

```bash
cd src/main
rm -f mr-out* mr-*-*
go run mrcoordinator.go sock123 pg-*.txt
```

### Start one or more workers (in separate terminals)

```bash
cd src/main
go run mrworker.go wc.so sock123
```

### Check the output

```bash
cat mr-out-* | sort | more
```

---

## Running Tests

```bash
cd src
make mr
```

Run a specific test:

```bash
make RUN="-run Wc" mr        # Word count
make RUN="-run Crash" mr     # Crash recovery
```
