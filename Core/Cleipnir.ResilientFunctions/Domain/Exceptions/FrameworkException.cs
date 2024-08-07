﻿using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FrameworkException : FlowTypeException
{
    public FrameworkException(FlowType flowType, string message) 
        : base(flowType, message) {}

    public FrameworkException(FlowType flowType, string message, Exception innerException) 
        : base(flowType, message, innerException) {}
}