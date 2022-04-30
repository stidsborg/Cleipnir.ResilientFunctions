using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Watchdogs;

public class WorkQueue
{
    private int _runningWorkers;
    private readonly int _maxParallelism;
    private readonly Queue<Func<Task>> _queue = new();
    private readonly object _sync = new();

    public WorkQueue(int maxParallelism)
    {
        if (maxParallelism <= 0)
            throw new ArgumentException("Max parallelism must be a positive number", nameof(maxParallelism));
        
        _maxParallelism = maxParallelism;  
    } 

    public void Enqueue(Func<Task> task)
    {
        lock (_sync)
        {
            _queue.Enqueue(task);
            if (_runningWorkers == _maxParallelism) return;
        }

        _ = ExecuteWorkerLoop();
    }

    private async Task ExecuteWorkerLoop()
    {
        Func<Task> work;
        lock (_sync)
        {
            if (_queue.Count == 0 || _runningWorkers == _maxParallelism)
                return;
            
            work = _queue.Dequeue();
            _runningWorkers++;
        }

        do
        {
            try
            {
                await work();
            }
            catch (Exception)
            {
                // ignored
            }

            lock (_sync)
            {
                if (_queue.Count == 0)
                {
                    _runningWorkers--;
                    return;
                }
                work = _queue.Dequeue();
            }
        } while (true);
    }
}