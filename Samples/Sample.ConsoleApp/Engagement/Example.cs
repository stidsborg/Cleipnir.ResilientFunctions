using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.Engagement;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var rAction = functions
            .RegisterAction(
             "EngagementSaga",
             (StartCustomerEngagement startCustomerEngagement, Context context) => EngagementSaga.Start(startCustomerEngagement, context)
            );

        const string customerEmail = "peter@gmail.com";
        await rAction.Schedule(
            functionInstanceId: customerEmail,
            param: new StartCustomerEngagement(customerEmail, DateTime.Today)
        );

        var eventSourceWriter = rAction.EventSourceWriters.For(customerEmail);
        await eventSourceWriter.AppendEvent(new EngagementRejected(0));
        await Task.Delay(3_000);
        await eventSourceWriter.AppendEvent(new EngagementAccepted(1));

        var controlPanel = await rAction.ControlPanels.For(customerEmail);
        await BusyWait.Until(async () =>
            {
                await controlPanel!.Refresh();
                return controlPanel.Status == Status.Succeeded;
            }, maxWait: TimeSpan.FromSeconds(10)
        );

        Console.WriteLine("EngagementSaga completed successfully");
    }
}