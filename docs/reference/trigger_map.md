# Trigger Map

The trigger map is the core part of dranspose and answers the following question:

!!! question
    Which *frames* from which *streams* belong to the same *event* and have to be processed by the same *worker*?

It is a matrix which can express all required combinations. Instead of assigning
workers directly, the trigger map only holds virtual workers. 
A guarantee is that if the same virtual worker is responsible for two frames, the same real worker will get both.

## Example

Stream   | Event 1 | Event 2 | Event 3 | Event 4 | Event 5 |Event 6
-------- |---------|---------|---------|---------|---------| --- 
stream 1 | all     | [1]     | [1]     | [3]     | [3]     | [5]
stream 2 | all     | [2]     | [2]     | [4]     | [4]     | [5]
low      | all     | [1,2]   | []      | [3,4]   | []      | [5]
slow     | all     | all     | none    | none    | none    | all

* The first frame of all streams is distribute to *all* workers. 
* The next 4 frames of *stream 1* are pairwise distributed to the same worker (first 1, then 3).
* Virtual workers 1 and 3 might be the same physical one, but it is not guaranteed
* The last frame of *stream 1* and *stream 2* are delivered to the same worker
* The second frame of *low* is deliverd to the same workers which are processing the second frame of *stream 1* and *stream 2*
* The third frame of *low* is delivered to no workers, i.e. discarded
* The thrid, fourth and fifth event has no frame from *slow*, this is useful for mixing streams with different sample frequencies. 