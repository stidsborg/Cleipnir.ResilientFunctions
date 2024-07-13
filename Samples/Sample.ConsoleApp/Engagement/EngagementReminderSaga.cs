using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.Engagement;

public static class EngagementReminderSaga
{
    public static async Task Start(StartCustomerEngagement startEngagement, Workflow workflow)
    {
        var (candidateEmail, nextReminderTime) = startEngagement;
        var messages = workflow.Messages;

        await workflow.Effect.Capture(
            "InitialCorrespondence",
            SendEngagementInitialCorrespondence
        );
        
        for (var i = 0; i < 10; i++)
        {
            //wait for candidate reply or timeout
            var either = await messages
                .TakeUntilTimeout($"Timeout{i}", nextReminderTime)
                .OfTypes<EngagementAccepted, EngagementRejected>()
                .Where(either =>
                    either.Match(
                        first: a => a.Iteration == i,
                        second: r => r.Iteration == i
                    )
                )
                .FirstOrDefault();
            
            if (either == null) //timeout
                continue;
            
            // if accepted notify hr and complete the flow
            if (either.Match(ea => true, er => false))
            {
                await workflow.Effect.Capture("NotifyHR", work: () => NotifyHR(candidateEmail));
                await messages.CancelTimeoutEvent(timeoutId: i.ToString());
                
                return;
            }

            //wait for timeout before sending next engagement reminder
            await messages
                .TakeUntilTimeout($"Timeout{i}", nextReminderTime)
                .Completion();
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