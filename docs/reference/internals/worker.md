# Worker

## Sequence Diagram
The worker runs many concurrent coroutines to achieve all its tasks.

On `run` it starts most of the coroutines.
`restart_work` first cancels the work relevant ones and then restarts them cleanly.
The `work` coroutine itself uses a `dequeue` and `poll` coroutine to explicitly wait for new assignments or data from ingesters.
This is required to achieve a clean `restart_work` and cancel this waiting.
`close` cleans up all open coroutines.

```mermaid
sequenceDiagram
    Worker ->>+ manage_ingesters: run
    Worker ->>+ manage_receiver: run
    Worker ->>+ work: run
    Worker ->>+ manage_assignments: run
    Worker ->>+ update_metrics: run
    Worker ->>+ register: run

    work ->>+ dequeue: wait for new ingesterset
    dequeue ->>- work: set available

    work ->>+ poll: poll ingesters
    poll ->>- work: gathered all ingester

    poll ->> Worker: restart work
    work ->>- Worker: restart work
    manage_assignments ->>- Worker: restart work
    Worker ->>+ work: restart work
    Worker ->>+ manage_assignments: restart work
    
    manage_ingesters ->>- Worker: close
    manage_receiver ->>- Worker: close
    update_metrics ->>- Worker: close
    manage_assignments ->>- Worker: close
    dequeue ->> Worker: close
    
    
    
```

## Worker Ingester Ping Sequence

```mermaid
sequenceDiagram
    Worker ->> Ingester: ping with uuid
    Ingester ->> Redis: update connected workers

    Redis ->> Worker: fetch ingester state with connected workers

    Worker ->> Worker: Check if pings reached Ingester
    
    
    
```

::: dranspose.worker

