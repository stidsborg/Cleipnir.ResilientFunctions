using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class ReplicaWatchdog(
    ClusterInfo clusterInfo, 
    IFunctionStore functionStore, 
    TimeSpan leaseLength, 
    UtcNow utcNow,
    UnhandledExceptionHandler unhandledExceptionHandler) : IDisposable
{
    private volatile bool _disposed;
    private bool _started;
    private bool _initialized;
    private IReplicaStore ReplicaStore => functionStore.ReplicaStore;
    
    public async Task Start()
    {
        var originalValue = Interlocked.CompareExchange(ref _started, value: true, comparand: false);
        if (originalValue)
            return;
        
        if (!_initialized)
            await Initialize(utcNow().Ticks);
        
        _ = Task.Run(Run);
    }

    public async Task Initialize(long? utcNowTicks = null)
    {
        await ReplicaStore.Insert(clusterInfo.ReplicaId, utcNowTicks ?? utcNow().Ticks);
        var replicas = await ReplicaStore.GetAll();
        var offset = CalculateOffset(replicas.Select(sr => sr.ReplicaId), clusterInfo.ReplicaId);
        if (offset is null)
            throw new InvalidOperationException("Replica offset was null after initialization");
        
        clusterInfo.ReplicaCount = (ulong) replicas.Count;
        clusterInfo.Offset = (ulong) offset.Value;
        _initialized = true;
    }

    private async Task Run()
    {
        var iteration = 0;
        while (!_disposed)
        {
            try
            {
                await PerformIteration(utcNow().Ticks);
            }
            catch (Exception ex)
            {
                unhandledExceptionHandler.Invoke(new FrameworkException("ReplicaWatchdog failed during iteration", ex));
            }
            
            await Task.Delay(leaseLength / 2);
            iteration++;

            if (iteration % 100 == 0)
            {
                _ = Task.Run(CheckForCrashedFunctions);
                iteration = 0;
            }
        }
    }

   public async Task PerformIteration(long utcNowTicks)
   {
        await ReplicaStore.UpdateHeartbeat(clusterInfo.ReplicaId, utcNowTicks);
        
        var storedReplicas = await ReplicaStore.GetAll();
        var offset = CalculateOffset(storedReplicas.Select(sr => sr.ReplicaId), clusterInfo.ReplicaId);

        var threshold = (utcNowTicks - (2 * leaseLength).Ticks);
        foreach (var crashedReplica in storedReplicas.Where(sr => sr.LatestHeartbeat < threshold))
        {
            await functionStore.RescheduleCrashedFunctions(crashedReplica.ReplicaId);
            await ReplicaStore.Delete(crashedReplica.ReplicaId);
            storedReplicas = await ReplicaStore.GetAll();
            offset = CalculateOffset(storedReplicas.Select(sr => sr.ReplicaId), clusterInfo.ReplicaId);
        }
        
        if (offset is not null)
        {
            clusterInfo.Offset = (ulong) offset.Value;
            clusterInfo.ReplicaCount = (ulong) storedReplicas.Count;
        }
        else
        {
            await Initialize();
        }
    }

   public async Task CheckForCrashedFunctions()
   {
       var activeReplicas = (await ReplicaStore.GetAll()).Select(r => r.ReplicaId).ToHashSet();
       var allOwners = await functionStore.GetOwnerReplicas();
       var crashedOwners = allOwners.Where(o => !activeReplicas.Contains(o));
       foreach (var crashedOwner in crashedOwners)
           await functionStore.RescheduleCrashedFunctions(crashedOwner);
   }

    public static int? CalculateOffset(IEnumerable<ReplicaId> allReplicaIds, ReplicaId ownReplicaId)
        => allReplicaIds
            .Select(s => s)
            .Order()
            .Select((id, i) => new { Id = id, Index = i })
            .FirstOrDefault(a => a.Id == ownReplicaId)
            ?.Index;

    public void Stop() => Dispose();
    public void Dispose() => _disposed = true;
}