using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.CustomerSignUp;

public static class SignupFlow
{
    public static async Task Start(string customerEmail, Context context)
    {
        var (activities, messages) = context;
        
        await activities.Do("SendWelcomeMail", () => SendWelcomeMail(customerEmail));

        for (var i = 0; i <= 5; i++)
        {
            var emailVerifiedOption = await messages.FirstOfType<EmailVerified>($"Timeout_{i}", TimeSpan.FromDays(1));
            if (emailVerifiedOption.HasValue)
                break;
            
            await activities.Do($"Reminder_{i}", () => SendReminderMail(customerEmail));
            
            if (i == 5)
                throw new UserSignupFailedException($"User '{customerEmail}' did not activate within threshold");
        }
        
        await context.Delay("DelayUntilTomorrow", TimeSpan.FromDays(1)); //uses activity and postpone functionality?!
        await activities.Do("SendWelcomeMail", () => SendFollowUpMail(customerEmail));
    }

    private static Task SendWelcomeMail(string customerEmail) => Task.CompletedTask;
    private static Task SendReminderMail(string customerEmail) => Task.CompletedTask;
    private static Task SendFollowUpMail(string customerEmail) => Task.CompletedTask;

    public class UserSignupFailedException : Exception
    {
        public UserSignupFailedException(string? message) : base(message) {}
    }
}

public record EmailVerified(string EmailAddress);