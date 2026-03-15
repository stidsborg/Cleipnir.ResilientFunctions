using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public class FlowTimeouts
{
    private readonly Lock _lock = new();
    private Dictionary<EffectId, Tuple<DateTime, TaskCompletionSource>> Timeouts { get; } = new();

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
            foreach (var (effectId, (timeout, tcs)) in Timeouts.ToList())
                if (timeout <= now)
                {
                    Timeouts.Remove(effectId);
                    Task.Run(() => tcs.TrySetResult());
                }
    }
}