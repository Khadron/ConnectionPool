using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConnectionPool
{
    public enum PoolState
    {
        /// <summary>
        /// 刚刚创建的对象，表示该对象未被调用过StartSeivice方法。
        /// </summary>
        UnInitialize = 0,
        /// <summary>
        /// 初始化中，该状态下服务正在按照参数初始化连接池。
        /// </summary>
        Initialize,
        /// <summary>
        /// 运行中
        /// </summary>
        Run,
        /// <summary>
        /// 停止状态
        /// </summary>
        Stop
    }

    /// <summary>
    /// 要申请链接的级别
    /// </summary>
    public enum ConnectionLevel
    {
        /// <summary>
        /// 独占方式，分配全新的连接资源，并且该连接资源在本次使用释放回连接池之前不能在分配出去。一般用于事务操作
        /// 如果连接池只能分配引用记数类型连接资源则该级别将产生一个异常，标志连接池资源耗尽
        /// </summary>
        ReadOnly = 0,
        /// <summary>
        /// 优先级-高，分配全新的连接资源，不使用引用记数技术。注：此级别不保证在分配后该连接资源后，仍然保持独立占有资源，若想独立占有资源请使用ReadOnely
        /// </summary>
        High,
        /// <summary>
        /// 优先级-中，适当应用引用记数技术分配连接
        /// </summary>
        None,
        /// <summary>
        /// 优先级-底，尽可能使用引用记数技术分配连接
        /// </summary>
        Base
    }

    /// <summary>
    /// 数据库连接类型
    /// </summary>
    public enum ConnectionType
    { /// <summary>
        /// 默认（无分配）
        /// </summary>
        None = 0,
        /// <summary>
        /// ODBC 数据源
        /// </summary>
        Odbc,
        /// <summary>
        /// OLE DB 数据源
        /// </summary>
        OleDb,
        /// <summary>
        /// SqlServer 数据库连接
        /// </summary>
        SqlClient,

    }

    /// <summary>
    /// 创建Connection工作模式
    /// </summary>
    public enum CreateThreadMode
    {
        /// <summary>
        /// 静态模式，会创建连接池配置的最小连接数目
        /// </summary>
        StaticCreateMode = 0,
        /// <summary>
        /// 动态模式，每隔一定时间就对连接池进行检测，如果发现连接数量小于最小连接数，则补充相应数量的新连接
        /// </summary>
        DynamicCreateMode = 1
    }
}
