using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public record FlowStatus(StoredId Id, Action Suspend, QueueManager QueueManager, int Threads, int AwaitingThreads);

public class FlowsManager
{
    private readonly Dictionary<StoredId, FlowStatus> _dict = new();
    private readonly Lock _lock = new();

    public void AddFlow(StoredId id, Action suspend, QueueManager queueManager)
    {
        lock (_lock)
        {
            if (!_dict.ContainsKey(id))
                return;

            _dict[id] = new FlowStatus(id, suspend, queueManager, Threads: 1, AwaitingThreads: 0);
        }
    }

    public async Task Interrupt(IEnumerable<StoredId> ids)
    {
        lock (_lock)
        {
            foreach (var id in ids)
            {
                if (!_dict.ContainsKey(id))
                    continue;
                
                var queueManager = _dict[id].QueueManager;
                Task.Run(() => queueManager.FetchMessagesOnce());
            }
        }
    }

    public void AddThread(StoredId id)
    {
        throw new NotImplementedException();
    }

    public void CompletedThread(StoredId id)
    {
        throw new NotImplementedException();
    }

    public void StartedThread(StoredId id)
    {
        throw new NotImplementedException();
    }

    public void SuspendedThread(StoredId id)
    {
        throw new NotImplementedException();
    }
}