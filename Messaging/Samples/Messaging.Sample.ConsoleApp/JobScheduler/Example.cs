using Cleipnir.ResilientFunctions.Messaging.PostgreSQL;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler.ExternalEntities;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler.Saga;
using Cleipnir.ResilientFunctions.PostgreSQL;

namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler;

public static class Example
{
    public static async Task Perform()
    {
        var connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=rfunctions;";

        await DatabaseHelper.RecreateDatabase(connectionString);
            
        var functionStore = new PostgreSqlFunctionStore(connectionString);
        await functionStore.Initialize();
        var rFunctions = new FunctionContainer(functionStore);
            
        var eventStore = new PostgreSqlEventStore(connectionString);
        await eventStore.Initialize();

        var messageQueue = new MessageQueue();
        
        const int capacity = 3;
        var jobWorkers = new JobWorker[]
        {
            new(capacity, messageQueue),
            new(capacity, messageQueue),
            new(capacity, messageQueue),
        };
        
        var saga = new CoordinatorSaga(
            rFunctions,
            eventStore,
            messageQueue,
            3
        );

        var jobId = Guid.NewGuid();
        await saga.ScheduleJob(jobId.ToString(), jobId);
    }
}