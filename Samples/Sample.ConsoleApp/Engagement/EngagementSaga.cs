using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive;

namespace ConsoleApp.Engagement;

public static class EngagementSaga
{
    public static async Task Start(string candidateEmail, Context context)
    {
        var es = await context.EventSource;

        await es.DoAtLeastOnce(
            workId: "InitialCorrespondence",
            SendEngagementInitialCorrespondence
        );
        
        for (var i = 0; i < 10; i++)
        {
            var either = await es
                .OfTypes<EngagementAccepted, EngagementRejected>()
                .Where(either =>
                    either.Match(
                        first: a => a.Iteration == i,
                        second: r => r.Iteration == i
                    )
                )
                .SuspendUntilNext(timeoutEventId: i.ToString(), expiresIn: TimeSpan.FromHours(1));

            var flowCompleted = await either.Match(
                first: async a =>
                {
                    await es.DoAtLeastOnce(workId: "NotifyHR", () => NotifyHR(candidateEmail));
                    return true;
                },
                second: r => Task.FromResult(false)
            );

            if (flowCompleted)
                return;
            
            await es.DoAtLeastOnce(
                workId: $"Reminder#{i}",
                SendEngagementReminder
            );
        }

        throw new Exception("Max number of retries exceeded");
    }

    private static Task NotifyHR(string candidateEmail) => Task.CompletedTask;
    private static Task SendEngagementInitialCorrespondence() => Task.CompletedTask;
    private static Task SendEngagementReminder() => Task.CompletedTask;
}

public record EngagementAccepted(int Iteration);
public record EngagementRejected(int Iteration);