using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public sealed class FlowTimeouts : IDisposable
{
    private readonly Lock _lock = new();
    private bool _disposed;
    private Dictionary<EffectId, Tuple<DateTime, TaskCompletionSource>> Timeouts { get; } = new();

    public FlowsManager? FlowsManager { get; }

    public FlowTimeouts() { }
    public FlowTimeouts(FlowsManager flowsManager)
    {
        FlowsManager = flowsManager;
        _ = Task.Run(TimeoutCheckLoop);
    }

    private async Task TimeoutCheckLoop()
    {
        while (!_disposed)
        {
            var now = DateTime.UtcNow;
            if (HasExpiredTimeouts(now))
                SignalExpiredTimeouts(now);

            await Task.Delay(10);
        }
    }

    public DateTime? MinimumTimeout
    {
        get
        {
            lock (_lock)
                return GetMinimumTimeout();
        }
    }

    private DateTime? GetMinimumTimeout()
        => Timeouts.Values.Count != 0 ? Timeouts.Values.Min(t => t.Item1) : (DateTime?)null;

    public async Task AddTimeout(EffectId effectId, DateTime timeout, TimeSpan? maxWait = null)
    {
        TaskCompletionSource tcs;
        lock (_lock)
        {
            tcs = new TaskCompletionSource();
            Timeouts[effectId] = Tuple.Create(timeout, tcs);
        }

        if (maxWait == null || timeout <= DateTime.UtcNow)
        {
            await tcs.Task;
            return;
        }

        var completed = await Task.WhenAny(tcs.Task, Task.Delay(maxWait.Value));
        if (completed != tcs.Task)
            throw new SuspendInvocationException();
    }

    public void RemoveTimeout(EffectId effectId)
    {
        lock (_lock)
            Timeouts.Remove(effectId);
    }

    public bool HasExpiredTimeouts(DateTime now)
    {
        lock (_lock)
            return GetMinimumTimeout() <= now;
    }

    public void SignalExpiredTimeouts(DateTime now)
    {
        lock (_lock)
            if (!_disposed)
            {
                //
                foreach (var (effectId, (timeout, tcs)) in Timeouts.ToList())
                    if (timeout <= now) //here do stuff
                    {
                        
                        Timeouts.Remove(effectId);
                        Task.Run(() => tcs.TrySetResult());
                    }                
            }
    }
    
    public void Dispose()
    {
        lock (_lock)
            _disposed = true;
    }
}