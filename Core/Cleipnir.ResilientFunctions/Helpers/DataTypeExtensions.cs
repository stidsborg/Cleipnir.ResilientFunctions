namespace Cleipnir.ResilientFunctions.Helpers;

public static class DataTypeExtensions
{
    public static ushort ToUshort(this int value) => (ushort)value;
    public static int ToInt(this ushort value) => value;
}