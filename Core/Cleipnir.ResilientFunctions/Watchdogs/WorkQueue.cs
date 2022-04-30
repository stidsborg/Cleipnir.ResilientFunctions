using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Watchdogs;

public class WorkQueue
{
    private int _runningWorkers;
    private readonly int _maxParallelism;
    private readonly Dictionary<string, Func<Task>> _enqueued = new();
    private readonly object _sync = new();

    public WorkQueue(int maxParallelism)
    {
        if (maxParallelism <= 0)
            throw new ArgumentException("Max parallelism must be a positive number", nameof(maxParallelism));
        
        _maxParallelism = maxParallelism;  
    }
    
    public void Enqueue(string id, Func<Task> work)
    {
        lock (_sync)
        {
            _enqueued[id] = work;
            if (_runningWorkers >= _maxParallelism) return;
        }
        
        _ = ExecuteWorkerLoop();
    }

    public void Enqueue(IEnumerable<WorkItem> workItems)
    {
        int startNewWorkers;
        lock (_sync)
        {
            foreach (var workItem in workItems)
                _enqueued[workItem.Id] = workItem.Work;

            startNewWorkers = Math.Min(
                _maxParallelism,
                Math.Max(_enqueued.Count - _runningWorkers, 0)
            );
        }

        for (var i = 0; i < startNewWorkers; i++)
            _ = ExecuteWorkerLoop();
    }

    private async Task ExecuteWorkerLoop()
    {
        Func<Task> work;
        string id;
        lock (_sync)
        {
            if (_enqueued.Count == 0 || _runningWorkers == _maxParallelism)
                return;

            (id, work) = _enqueued.First();
            _enqueued.Remove(id);
            
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
                if (_enqueued.Count == 0)
                {
                    _runningWorkers--;
                    return;
                }
                (id, work) = _enqueued.First();
                _enqueued.Remove(id);
            }
        } while (true);
    }

    public record WorkItem(string Id, Func<Task> Work);
}