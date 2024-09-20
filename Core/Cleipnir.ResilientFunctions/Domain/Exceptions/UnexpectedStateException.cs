﻿using System;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class UnexpectedStateException : FrameworkException
{
    public UnexpectedStateException(FlowId flowId, string message)
        : base(message, flowId) {}

    public UnexpectedStateException(FlowId flowId, string message, Exception innerException)
        : base(message, innerException, flowId) {}

    public static UnexpectedStateException NotFound(FlowId flowId) => new(flowId, $"{flowId} was not found");
    public static UnexpectedStateException EpochMismatch(FlowId flowId) => new(flowId, $"{flowId} did not have expected epoch at restart");
    public static UnexpectedStateException LeaseUpdateFailed(FlowId flowId) => new(flowId, $"{nameof(LeaseUpdater)} failed to update lease for '{flowId}'");
    public static UnexpectedStateException ConcurrentModification(FlowId flowId) => new(flowId, $"Unable to persist state for '{flowId}' due to concurrent modification");
}