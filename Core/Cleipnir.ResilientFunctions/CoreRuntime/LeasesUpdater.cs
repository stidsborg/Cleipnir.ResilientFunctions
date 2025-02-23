using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class LeasesUpdater(TimeSpan leaseLength, IFunctionStore functionStore, UnhandledExceptionHandler unhandledExceptionHandler) : IDisposable
{
    private volatile bool _disposed;
    
    private readonly Lock _lock = new();
    private readonly Dictionary<StoredId, EpochAndExpiry> _executingFlows = new();
    
    public async Task Start()
    {
        if (leaseLength == TimeSpan.Zero || leaseLength == TimeSpan.MaxValue)
            _disposed = true;

        try
        {
            while (!_disposed)
            {
                long minExpiry;
                var threshold = (DateTime.UtcNow + (leaseLength / 2)).Ticks;
                lock (_lock)
                    minExpiry = _executingFlows
                        .Values
                        .Select(e => e.Expiry)
                        .Append(threshold)
                        .Min();

                var delay = minExpiry < threshold
                    ? TimeSpan.Zero
                    : leaseLength / 2;

                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay);

                await RenewLeases(); 
            }
        }
        catch (Exception exception)
        {
            unhandledExceptionHandler.Invoke(new FrameworkException("LeaseUpdater threw exception", exception));
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }

    public async Task RenewLeases()
    {
        if (_disposed) return;
        
        var thresholdDateTime = DateTime.UtcNow + (leaseLength / 2);
        var threshold = thresholdDateTime.Ticks;
        var thresholdAndPeek = (thresholdDateTime + leaseLength / 4).Ticks;
        var nextLeaseExpiry = DateTime.UtcNow.Add(leaseLength).Ticks;
        var leaseUpdates = new List<LeaseUpdate>();
        lock (_lock)
        {
            var update = false;
            foreach (var (id, epochAndExpiry) in _executingFlows.Where(kv => kv.Value.Expiry <= thresholdAndPeek))
            {
                if (epochAndExpiry.Expiry <= threshold)
                    update = true;
                
                leaseUpdates.Add(new LeaseUpdate(id, epochAndExpiry.Epoch));
            }
            
            if (!update)
                return;
        }

        try
        {
            var leasesUpdated = await functionStore.RenewLeases(
                leaseUpdates,
                nextLeaseExpiry
            );
        
            if (leasesUpdated == leaseUpdates.Count)
                lock (_lock)
                    foreach (var x in leaseUpdates)
                        ConditionalSet(x.StoredId, x.ExpectedEpoch, nextLeaseExpiry);
            else
            {
                var functionsStatus = 
                    (await functionStore.GetFunctionsStatus(leaseUpdates.Select(u => u.StoredId)))
                        .ToDictionary(s => s.StoredId);

                lock (_lock)
                    foreach (var leaseUpdate in leaseUpdates)
                    {
                        if (!functionsStatus.ContainsKey(leaseUpdate.StoredId))
                            ConditionalRemove(leaseUpdate.StoredId, leaseUpdate.ExpectedEpoch);
                    
                        var (_, status, epoch, expiry) = functionsStatus[leaseUpdate.StoredId];
                        if (epoch != leaseUpdate.ExpectedEpoch || status != Status.Executing)
                            ConditionalRemove(leaseUpdate.StoredId, leaseUpdate.ExpectedEpoch);
                        
                        ConditionalSet(leaseUpdate.StoredId, leaseUpdate.ExpectedEpoch, expiry);
                    }
            }
        }
        catch (Exception exception)
        {
            unhandledExceptionHandler.Invoke(new UnexpectedStateException("LeaseUpdater iteration failed", exception));
            await Task.Delay(250);
        }
    }
    
    private void ConditionalSet(StoredId flowId, int epoch, long expiry)
    {
        if (!_executingFlows.TryGetValue(flowId, out EpochAndExpiry? value)) 
            return;
        
        if (value.Epoch >= epoch)
            _executingFlows[flowId] = new EpochAndExpiry(epoch, expiry);
    }
    
    public IReadOnlyList<IdAndEpoch> FindAlreadyContains(IReadOnlyList<IdAndEpoch> idAndEpoches)
    {
        if (idAndEpoches.Count == 0)
            return idAndEpoches;
        
        lock (_lock)
        {
            var containsAny = false;
            foreach (var (storedId, _) in idAndEpoches)
                if (!containsAny && _executingFlows.ContainsKey(storedId))
                    containsAny = true;

            if (!containsAny)
                return [];
            
            var alreadyContains = new List<IdAndEpoch>();
            foreach (var idAndEpoch in idAndEpoches)
                if (_executingFlows.TryGetValue(idAndEpoch.FlowId, out EpochAndExpiry? value) && value.Epoch == idAndEpoch.Epoch)
                    alreadyContains.Add(idAndEpoch);
            
            return alreadyContains;
        }
    }
    
    public IReadOnlyList<IdAndEpoch> FilterOutContains(IReadOnlyList<IdAndEpoch> idAndEpoches)
    {
        if (idAndEpoches.Count == 0) return idAndEpoches;
        
        HashSet<IdAndEpoch> alreadyContained;
        lock (_lock)
            alreadyContained = FindAlreadyContains(idAndEpoches).ToHashSet();
        
        if (alreadyContained.Count == 0) return idAndEpoches;
        var toReturn = new List<IdAndEpoch>(capacity: idAndEpoches.Count);
        foreach (var idAndEpoch in idAndEpoches)
            if (!alreadyContained.Contains(idAndEpoch))
                toReturn.Add(idAndEpoch);

        return toReturn;
    }

    public void Set(StoredId flowId, int epoch, long? expiresTicks = null)
    {
        var expiry = expiresTicks ?? DateTime.UtcNow.Add(leaseLength).Ticks;
        
        lock (_lock)
            if (!_executingFlows.TryGetValue(flowId, out EpochAndExpiry? value))
                _executingFlows[flowId] = new EpochAndExpiry(epoch, expiry);
            else if (value.Epoch < epoch)
                _executingFlows[flowId] = new EpochAndExpiry(epoch, expiry);
    }

    public void ConditionalRemove(StoredId storedId, int epoch)
    {
        lock (_lock)
            if (_executingFlows.TryGetValue(storedId, out var epochAndExpiry))
                if (epochAndExpiry.Epoch == epoch)
                    _executingFlows.Remove(storedId);
    }

    public IReadOnlyDictionary<StoredId, EpochAndExpiry> GetExecutingFlows()
    {
        lock (_lock)
            return _executingFlows.ToDictionary(kv => kv.Key, kv => kv.Value);
    }
    public record EpochAndExpiry(int Epoch, long Expiry);
    
    public void Dispose() => _disposed = true;
}