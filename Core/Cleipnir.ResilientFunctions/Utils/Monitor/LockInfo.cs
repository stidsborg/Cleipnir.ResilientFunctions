using System;

namespace Cleipnir.ResilientFunctions.Utils.Monitor;

public record LockInfo(string GroupId, string Name, string LockId, TimeSpan? MaxWait = null);