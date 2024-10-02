# Reducer

## Sequence Diagram
The reducer is quite simple as it has a `work` task and a `timer` task for periodic callbacks.
Both get restarted on `restart_work` and the `update` metrics and `register` run until the end.

```mermaid
sequenceDiagram
    Reducer ->>+ work: run
    Reducer ->>+ timer: run
    Reducer ->>+ update_metrics: run
    Reducer ->>+ register: run

    
    timer ->>- Reducer: restart work
    work ->>- Reducer: restart work
    Reducer ->>+ work: restart work
    Reducer ->>+ timer: restart work
    
    timer ->>- Reducer: close
    work ->>- Reducer: close
    update_metrics ->>- Reducer: close
    
    
    
```

::: dranspose.reducer