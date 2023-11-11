using Serilog;

namespace Sample.OrderProcessing.Rpc;

public interface ILogisticsClient
{
    Task<TrackAndTrace> ShipProducts(Guid customerId, IEnumerable<Guid> productIds);
}

public class LogisticsClientStub : ILogisticsClient
{
    public Task<TrackAndTrace> ShipProducts(Guid customerId, IEnumerable<Guid> productIds)
        => Task.Delay(100).ContinueWith(_ =>
            {
                Log.Logger.ForContext<ILogisticsClient>().Information("LOGISTICS_SERVER: Products shipped");
                return new TrackAndTrace(Guid.NewGuid().ToString("N"));
            }
        );
}

public record TrackAndTrace(string Number);