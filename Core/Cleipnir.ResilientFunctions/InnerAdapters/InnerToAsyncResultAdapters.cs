using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

internal static class InnerToAsyncResultAdapters
{
    // ** !! PARAMLESS !! ** //
    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Task> inner)
        => (_, workflow) => WithSuspension<Unit>(workflow, async () =>
        {
            await inner();
            return Unit.Instance;
        });

    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Workflow, Task> inner)
        => (_, workflow) => WithSuspension<Unit>(workflow, async () =>
        {
            await inner(workflow);
            return Unit.Instance;
        });

    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Task<Result<Unit>>> inner)
        => (_, workflow) => WithSuspension(workflow, inner);

    public static Func<Unit, Workflow, Task<Result<Unit>>> ToInnerParamlessWithTaskResultReturn(Func<Workflow, Task<Result<Unit>>> inner)
        => (_, workflow) => WithSuspension(workflow, () => inner(workflow));

    // ** !! ACTION !! ** //
    // ** ASYNC ** //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task> inner) where TParam : notnull
        => (param, workflow) => WithSuspension<Unit>(workflow, async () =>
        {
            await inner(param);
            return Succeed.WithUnit;
        });

    // ** ASYNC W. workflow * //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task> inner) where TParam : notnull
        => (param, workflow) => WithSuspension<Unit>(workflow, async () =>
        {
            await inner(param, workflow);
            return Succeed.WithUnit;
        });

    // ** ASYNC W. RESULT ** //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Task<Result<Unit>>> inner) where TParam : notnull
        => (param, workflow) => WithSuspension(workflow, () => inner(param));

    // ** ASYNC W. RESULT AND workflow ** //
    public static Func<TParam, Workflow, Task<Result<Unit>>> ToInnerActionWithTaskResultReturn<TParam>(Func<TParam, Workflow, Task<Result<Unit>>> inner) where TParam : notnull
        => (param, workflow) => WithSuspension(workflow, () => inner(param, workflow));

    // ** !! FUNCTION !! ** //
    // ** ASYNC ** //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<TReturn>> inner) where TParam : notnull
        => (param, workflow) => WithSuspension<TReturn>(workflow, async () =>
        {
            var result = await inner(param);
            return Succeed.WithValue(result);
        });

    // ** ASYNC W. workflow * //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<TReturn>> inner) where TParam : notnull
        => (param, workflow) => WithSuspension<TReturn>(workflow, async () =>
        {
            var result = await inner(param, workflow);
            return Succeed.WithValue(result);
        });

    // ** ASYNC W. workflow AND RESULT * //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Workflow, Task<Result<TReturn>>> inner) where TParam : notnull
        => (param, workflow) => WithSuspension(workflow, () => inner(param, workflow));

    // ** ASYNC W. RESULT ** //
    public static Func<TParam, Workflow, Task<Result<TReturn>>> ToInnerFuncWithTaskResultReturn<TParam, TReturn>(Func<TParam, Task<Result<TReturn>>> inner) where TParam : notnull
        => (param, workflow) => WithSuspension(workflow, () => inner(param));

    // Races the inner function against the flow's suspension signal. When FlowExecutionState decides to suspend,
    // every subflow - including the one executing inner - is parked forever, so the inner task never completes;
    // the invocation instead returns a Suspend/Postpone result and the ordinary result pipeline persists it
    // (the minimum registered timeout becomes the postpone-until target).
    private static async Task<Result<T>> WithSuspension<T>(Workflow workflow, Func<Task<Result<T>>> inner)
    {
        try
        {
            var innerTask = inner();
            await Task.WhenAny(innerTask, workflow.Effect.FlowExecutionState.SuspendedTask);
            if (!innerTask.IsCompleted)
                return SuspendOrPostpone<T>(workflow);

            return await innerTask;
        }
        catch (FatalWorkflowException exception) { return Fail.WithException(exception, workflow.FlowId); }
        catch (Exception exception) { return Fail.WithException(FatalWorkflowException.CreateNonGeneric(workflow.FlowId, exception)); }
    }

    private static Result<T> SuspendOrPostpone<T>(Workflow workflow)
    {
        var minTimeout = workflow.Effect.FlowTimeouts.MinimumTimeout;
        return minTimeout == null
            ? Suspend.Invocation
            : Postpone.Until(minTimeout.Value);
    }
}
