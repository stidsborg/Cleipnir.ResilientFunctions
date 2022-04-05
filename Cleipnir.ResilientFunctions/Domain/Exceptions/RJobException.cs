using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class RJobException : RFunctionException
{
    public string JobId { get; set; }

    public RJobException(string jobId, string message, Exception innerException)
        : base("Job", message, innerException) => JobId = jobId;
}