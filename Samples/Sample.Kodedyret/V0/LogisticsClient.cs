using Serilog;

namespace Sample.Kodedyret.V0;

public interface ILogisticsClient
{
    Task ShipProducts(Guid customerId, IEnumerable<Guid> productIds);
}

public class LogisticsClientStub : ILogisticsClient
{
    public Task ShipProducts(Guid customerId, IEnumerable<Guid> productIds)
        => Task.Delay(100).ContinueWith(_ =>
            Log.Logger.Information("LOGISTICS_SERVER: Products shipped")
        );
}