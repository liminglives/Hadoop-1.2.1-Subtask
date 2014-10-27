Hadoop-1.2.1-Subtask
====================

Map/Reduce Subtask

Brief introduction:

First, we cut one block into subblocks. 
Then we spawn some threads in map task, and we call each thread as subtask. 
As we know, each map task processes one block. 
So, for example, each subtask will process one or more subblocks. 
Therefore, block is the computing unit in original Hadoop, 
but in our way, subblock would be the computing unit. 
And this would improve parallelism of task execution and shorten the job completion time.