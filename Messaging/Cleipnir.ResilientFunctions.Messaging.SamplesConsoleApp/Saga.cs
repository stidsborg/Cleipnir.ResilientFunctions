using System.Reactive.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core;

namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp;

public class Saga
{
    private readonly EventSources _eventSources;

    public Saga(EventSources eventSources)
    {
        _eventSources = eventSources;
    }

    public async Task ProcessOrder(string orderId, Scrapbook scrapbook)
    {
        var eventSource = await _eventSources
            .GetEventSource(new FunctionId("OrderProcessing", orderId));

        var next = await eventSource.All.OfType<string>().FirstAsync();
        
        Console.WriteLine(next);
    }
    
    public class Scrapbook : RScrapbook { }
}
