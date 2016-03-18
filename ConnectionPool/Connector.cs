using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace ConnectionPool
{
    public class Connector : IDisposable
    {
        private static readonly object Locker = new object();

        #region Field & Property

        private bool _enable = true;
        /// <summary>
        /// 是否失效；false表示失效，只读
        /// </summary>
        public bool Enable
        {
            get { return _enable; }
            set { _enable = value; }
        }

        private bool _use = false;
        /// <summary>
        /// 是否正在被使用中，只读
        /// </summary>
        public bool IsUse
        {
            get { return _use; }
        }

        private bool _canAllocation = true;
        /// <summary>
        /// 表示连接是否可以被分配
        /// </summary>
        public bool CanAllocation
        {
            get { return _canAllocation; }
            set { _canAllocation = value; }
        }

        private DateTime _createTime;
        /// <summary>
        /// 创建时间，只读
        /// </summary>
        public DateTime CreateTime
        {
            get { return _createTime; }
        }

        private int _useTimes = 0;
        /// <summary>
        /// 被使用次数，只读
        /// </summary>
        public int UseTimes
        {
            get { return _useTimes; }
        }

        private int _currentRepeat = 0;
        /// <summary>
        /// 当前连接被重复引用多少，只读
        /// </summary>
        public int CurrentRepeat
        {
            get { return _currentRepeat; }
        }

        private bool _isRepeat = true;
        /// <summary>
        /// 连接是否可以被重复引用
        /// </summary>
        public bool IsRepeat
        {
            get { return _isRepeat; }
            set { _isRepeat = value; }
        }

        private ConnectionType _connType;
        /// <summary>
        /// 连接类型，只读
        /// </summary>
        public ConnectionType ConnectionType
        {
            get { return _connType; }
        }

        /// <summary>
        /// 当前数据库连接状态，只读
        /// </summary>
        public ConnectionState State
        {
            get { return _dbConn.State; }
        }

        private DbConnection _dbConn = null;
        /// <summary>
        /// 返回当前数据库连接，只读
        /// </summary>
        public DbConnection DbConnection
        {
            get { return _dbConn; }
        }

        private object _tag = null;
        /// <summary>
        /// 附带的信息
        /// </summary>
        public object Tag
        {
            get { return _tag; }
            set { _tag = value; }
        }

        #endregion

        #region Constructor

        public Connector(DbConnection dbConnection, ConnectionType connType)
        {
            _createTime = DateTime.Now;
            _dbConn = dbConnection;
            _connType = connType;
        }

        public Connector(DbConnection dbconnection, ConnectionType connType, DateTime dateTime)
            : this(dbconnection, connType)
        {
            _createTime = dateTime;
        }

        #endregion

        #region Method

        /// <summary>
        /// 打开数据库连接
        /// </summary>
        public void Open()
        {
            _dbConn.Open();
        }

        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        public void Close()
        {
            _dbConn.Close();
        }

        /// <summary>
        /// 将当前连接设为失效
        /// </summary>
        public void SetConnectionFailure()
        {
            _enable = false;
            _canAllocation = false;
        }

        /// <summary>
        /// 被分配出去，线程安全
        /// </summary>
        public void Repeat()
        {
            lock (Locker)
            {
                if (_enable == false)
                    throw new InvalidResourceExecption();
                if (_canAllocation == false)
                    throw new AllocationExecption();
                if (_use && _isRepeat == false)
                    throw new AllocationAndRepeatExecption();
                _currentRepeat++;//引用次数+1
                _useTimes++;//被引用次数+1
                _use = true;//被使用

            }
        }

        /// <summary>
        /// 被释放回来，线程安全
        /// </summary>
        public void Remove()
        {
            lock (Locker)
            {
                if (_enable == false)
                    throw new InvalidResourceExecption();
                if (_currentRepeat == 0)
                    throw new RepeatIsZeroExecption();
                _currentRepeat--;//引用次数-1
                _use = _currentRepeat != 0;
            }
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            _enable = false;
            _dbConn.Dispose();
            _dbConn = null;
        }

        #endregion
    }
}
