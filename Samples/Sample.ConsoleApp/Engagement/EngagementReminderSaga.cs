using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.Engagement;

public static class EngagementReminderSaga
{
    public static async Task Start(StartCustomerEngagement startEngagement, Context context)
    {
        var (candidateEmail, nextReminderTime) = startEngagement;
        var es = context.Messages;

        await context.Activities.Do(
            "InitialCorrespondence",
            SendEngagementInitialCorrespondence
        );
        
        for (var i = 0; i < 10; i++)
        {
            //wait for candidate reply or timeout
            var either = await es
                .OfTypes<EngagementAccepted, EngagementRejected>()
                .Where(either =>
                    either.Match(
                        first: a => a.Iteration == i,
                        second: r => r.Iteration == i
                    )
                )
                .TakeUntilTimeout($"Timeout{i}", nextReminderTime)
                .SuspendUntilFirstOrDefault();
            
            if (either == null) //timeout
                continue;
            
            // if accepted notify hr and complete the flow
            if (either.Match(ea => true, er => false))
            {
                await context.Activities.Do("NotifyHR", work: () => NotifyHR(candidateEmail));
                await es.CancelTimeoutEvent(timeoutId: i.ToString());
                
                return;
            }

            //wait for timeout before sending next engagement reminder
            await es
                .TakeUntilTimeout($"Timeout{i}", nextReminderTime)
                .SuspendUntilFirst();
        }

        throw new Exception("Max number of retries exceeded");
    }

    private static Task NotifyHR(string candidateEmail) => Task.CompletedTask;
    private static Task SendEngagementInitialCorrespondence() => Task.CompletedTask;
    private static Task SendEngagementReminder() => Task.CompletedTask;
}

public record StartCustomerEngagement(string CandidateEmail, DateTime StartDate);
public record EngagementAccepted(int Iteration);
public record EngagementRejected(int Iteration);