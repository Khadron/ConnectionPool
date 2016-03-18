using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConnectionPool
{

    /// <summary>
    /// 服务未启动
    /// </summary>
    public class PoolNotRunException : Exception
    {
        public PoolNotRunException() : base("服务未启动") { }
        public PoolNotRunException(string message) : base(message) { }
    }
    /// <summary>
    /// 服务已经运行或者未完全结束
    /// </summary>
    public class PoolNotStopException : Exception
    {
        public PoolNotStopException() : base("服务已经运行或者未完全结束") { }
        public PoolNotStopException(string message) : base(message) { }
    }
    /// <summary>
    /// 连接池资源未全部回收
    /// </summary>
    public class ResCallBackException : Exception
    {
        public ResCallBackException() : base("连接池资源未全部回收") { }
        public ResCallBackException(string message) : base(message) { }
    }
    /// <summary>
    /// 连接池已经饱和，不能提供连接
    /// </summary>
    public class PoolFullException : Exception
    {
        public PoolFullException() : base("连接池已经饱和，不能提供连接") { }
        public PoolFullException(string message) : base(message) { }
    }
    /// <summary>
    /// 服务状态错误
    /// </summary>
    public class StateException : Exception
    {
        public StateException() : base("服务状态错误") { }
        public StateException(string message) : base(message) { }
    }
    /// <summary>
    /// 一个key对象只能申请一个连接
    /// </summary>
    public class KeyExecption : Exception
    {
        public KeyExecption() : base("一个key对象只能申请一个连接") { }
        public KeyExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 无法释放，不存在的key
    /// </summary>
    public class NotKeyExecption : Exception
    {
        public NotKeyExecption() : base("无法释放，不存在的key") { }
        public NotKeyExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 当前连接池状态不可以对属性赋值
    /// </summary>
    public class SetValueExecption : Exception
    {
        public SetValueExecption() : base("当前连接池状态不可以对属性赋值") { }
        public SetValueExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 参数范围错误
    /// </summary>
    public class ParameterBoundExecption : Exception
    {
        public ParameterBoundExecption() : base("参数范围错误") { }
        public ParameterBoundExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 无效的ConnTypeEnum类型参数
    /// </summary>
    public class ConnTypeExecption : Exception
    {
        public ConnTypeExecption() : base("无效的ConnTypeEnum类型参数") { }
        public ConnTypeExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 连接资源耗尽，或错误的访问时机。
    /// </summary>
    public class OccasionExecption : Exception
    {
        public OccasionExecption() : base("连接资源耗尽，或错误的访问时机。") { }
        public OccasionExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 连接资源已经失效。
    /// </summary>
    public class InvalidResourceExecption : Exception
    {
        public InvalidResourceExecption() : base("连接资源已经失效。") { }
        public InvalidResourceExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 连接资源不可以被分配。
    /// </summary>
    public class AllocationExecption : Exception
    {
        public AllocationExecption() : base("连接资源不可以被分配。") { }
        public AllocationExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 连接资源已经被分配并且不允许重复引用。
    /// </summary>
    public class AllocationAndRepeatExecption : AllocationExecption
    {
        public AllocationAndRepeatExecption() : base("连接资源已经被分配并且不允许重复引用") { }
        public AllocationAndRepeatExecption(string message) : base(message) { }
    }
    /// <summary>
    /// 引用记数已经为0。
    /// </summary>
    public class RepeatIsZeroExecption : Exception
    {
        public RepeatIsZeroExecption() : base("引用记数已经为0。") { }
        public RepeatIsZeroExecption(string message) : base(message) { }
    }

}
