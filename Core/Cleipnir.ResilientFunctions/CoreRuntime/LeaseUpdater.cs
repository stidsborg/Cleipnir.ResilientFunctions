using System;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

internal class LeaseUpdater : IDisposable
{
    private readonly StoredId _storedId;
    private readonly int _epoch;
    private readonly LeasesUpdater _leasesUpdater;

    private LeaseUpdater(StoredId storedId, int epoch, LeasesUpdater leasesUpdater)
    {
        _storedId = storedId;
        _epoch = epoch;
        _leasesUpdater = leasesUpdater;
    }

    public static IDisposable CreateAndStart(StoredId storedId, int epoch, LeasesUpdater leasesUpdater)
    {
        var leaseUpdater = new LeaseUpdater(storedId, epoch, leasesUpdater);
        leaseUpdater.Register();
        return leaseUpdater;
    }

    private void Register() => _leasesUpdater.Set(_storedId, _epoch);
    private void RemoveFromLeaseUpdaters() => _leasesUpdater.ConditionalRemove(_storedId, _epoch);
    public void Dispose() => RemoveFromLeaseUpdaters();  
}