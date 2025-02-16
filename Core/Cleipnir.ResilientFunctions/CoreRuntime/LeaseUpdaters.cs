using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class LeaseUpdaters(IFunctionStore functionStore, UnhandledExceptionHandler unhandledExceptionHandler)
{
    private readonly Lock _lock = new();
    private readonly Dictionary<TimeSpan, LeaseUpdatersForLeaseLength> _leaseUpdaters = new();
    
    public LeaseUpdatersForLeaseLength GetOrCreateLeaseUpdatersForLeaseLength(TimeSpan leaseLength)
    {
        lock (_lock)
            if (!_leaseUpdaters.ContainsKey(leaseLength))
            {
                var leaseUpdaterForLeaseLength = new LeaseUpdatersForLeaseLength(leaseLength, functionStore, unhandledExceptionHandler);
                _leaseUpdaters[leaseLength] = leaseUpdaterForLeaseLength;
                //todo _ = leaseUpdaterForLeaseLength.Start();
                
                return leaseUpdaterForLeaseLength;
            }
            else
                return _leaseUpdaters[leaseLength];
    }

    public IReadOnlyList<IdAndEpoch> FilterOutContains(IReadOnlyList<IdAndEpoch> idAndEpoches)
    {
        lock (_lock)
        {
            var containedInALeaseUpdater = new HashSet<IdAndEpoch>();
            foreach (var leaseUpdatersForLeaseLength in _leaseUpdaters.Values)
            {
                var alreadyContains = leaseUpdatersForLeaseLength.FindAlreadyContains(idAndEpoches);
                foreach (var alreadyContain in alreadyContains)
                    containedInALeaseUpdater.Add(alreadyContain);
            }

            return containedInALeaseUpdater.Count == 0 
                ? idAndEpoches 
                : idAndEpoches.Where(i => !containedInALeaseUpdater.Contains(i)).ToList();
        }
    }
}