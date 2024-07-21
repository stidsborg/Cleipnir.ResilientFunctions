using System;
using System.Collections.Generic;
using System.Linq;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class UnhandledExceptionCatcher
{
    private readonly object _sync = new();

    public List<Exception> ThrownExceptions
    {
        get
        {
            lock (_sync)
                return _thrownExceptions.ToList();
        }
    }
        
    private readonly List<Exception> _thrownExceptions = new();

    public void Catch(Exception e)
    {
        lock (_sync)
            _thrownExceptions.Add(e);
    }

    public void ShouldNotHaveExceptions()
    {
        if (ThrownExceptions.Count == 0)
            return;

        ThrownExceptions.ShouldBeEmpty(
            "Exceptions: " + Environment.NewLine +
            string.Join("--------------------" + Environment.NewLine, ThrownExceptions)
        );
    }
}