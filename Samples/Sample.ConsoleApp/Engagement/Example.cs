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
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var rAction = functions.RegisterAction<StartCustomerEngagement>(
            "EngagementSaga",
            EngagementReminderSaga.Start
        );

        const string customerEmail = "peter@gmail.com";
        await rAction.Schedule(
            flowInstance: customerEmail,
            param: new StartCustomerEngagement(customerEmail, DateTime.Today)
        );

        var messageWriter = rAction.MessageWriters.For(customerEmail.ToFlowInstance());
        await messageWriter.AppendMessage(new EngagementRejected(0));
        await Task.Delay(3_000);
        await messageWriter.AppendMessage(new EngagementAccepted(1));

        var controlPanel = await rAction.ControlPanel(customerEmail);
        await BusyWait.Until(async () =>
            {
                await controlPanel!.Refresh();
                return controlPanel.Status == Status.Succeeded;
            }, maxWait: TimeSpan.FromSeconds(10)
        );

        Console.WriteLine("EngagementSaga completed successfully");
    }
}