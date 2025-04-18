﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public delegate Task ScheduleReInvocation(StoredInstance flowInstance, int expectedEpoch);