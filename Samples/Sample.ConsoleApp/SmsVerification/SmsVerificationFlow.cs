using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.SmsVerification;

public class SmsVerificationFlow
{
    public async Task Run(string customerPhoneNumber, Workflow workflow)
    {
        var messages = workflow.Messages;
        var effect = workflow.Effect;
        var state = workflow.States.CreateOrGet<FlowState>();
        
        for (var i = 0; i < 5; i++)
        {
            var generatedCode = await effect.Capture(
                $"SendSms#{i}",
                async () =>
                {
                    var generatedCode = GenerateOneTimeCode();
                    await SendSms(customerPhoneNumber, generatedCode);
                    return generatedCode;
                }
            );
            
            var codeFromUser = await messages
                .OfType<CodeFromUser>()
                .Skip(i)
                .First();

            if (IsExpired(codeFromUser))
                state.Status = MostRecentAttempt.CodeExpired;
            else if (codeFromUser.Code == generatedCode)
            {
                state.Status = MostRecentAttempt.Success;
                return;
            }

            state.Status = MostRecentAttempt.IncorrectCode;
        }

        state.Status = MostRecentAttempt.MaxAttemptsExceeded;
    }

    private string GenerateOneTimeCode()
    {
        throw new NotImplementedException();
    }

    private Task SendSms(string customerPhoneNumber, string generatedCode)
    {
        throw new NotImplementedException();
    }

    private bool IsExpired(CodeFromUser code)
    {
        throw new NotImplementedException();
    }

    public class FlowState : Cleipnir.ResilientFunctions.Domain.FlowState
    {
        public MostRecentAttempt Status { get; set; }
    }
    
    public record CodeFromUser(string CustomerPhoneNumber, string Code, DateTime Timestamp);

    public enum MostRecentAttempt
    {
        NotStarted,
        CodeExpired,
        IncorrectCode,
        Success,
        MaxAttemptsExceeded
    }

}