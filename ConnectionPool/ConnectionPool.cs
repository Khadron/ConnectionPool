using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Timers;
using Timer = System.Timers.Timer;

namespace ConnectionPool
{
    public class ConnectionPool : IDisposable
    {

        private static readonly object PoolLocker = new object();
        private static ConnectionPool _instance;

        private Timer _timer;//监视器记时器

        private CreateThreadMode _createThreadMode = CreateThreadMode.StaticCreateMode;
        private int _createThreadProcessNum = 0;//需要创建的连接数
        private bool _createThreadProcessRun = false;//是否决定创建线程将继续工作，如果不继续工作则线程会将自己处于阻止状态


        //当连接池的连接被分配尽时，连接池会在已经分配出去的连接中，重复分配连接（引用记数）。来缓解连接池压力
        private PoolState _ps;//连接池状态
        //内部对象
        private readonly List<Connector> connList;//实际连接
        private readonly Hashtable useConns;//正在使用的连接

        private Thread _threadCreate;//创建线程
        private bool _isThreadCheckRun = false;

        #region Field & Property

        /// <summary>
        /// 连接池服务状态
        /// </summary>
        public PoolState State
        {
            get { return _ps; }
        }

        /// <summary>
        /// 连接池是否启动，改变该属性将相当于调用StartServices或StopServices方法，注：只有连接池处于Run，Stop状态情况下才可以对此属性赋值
        /// </summary>
        public bool Enable
        {
            get
            {
                if (_ps == PoolState.Run)
                    return true;
                else
                    return false;
            }
            set
            {
                if (_ps == PoolState.Run || _ps == PoolState.Stop)
                    if (value == true)
                        StartServices();
                    else
                        StopServices();
                else
                    throw new SetValueExecption();//只有连接池处于Run，Stop状态情况下才可以对此属性赋值
            }
        }

        private ConnectionType _connType;//连接池连接类型
        /// <summary>
        /// 得到或设置连接类型
        /// </summary>
        public ConnectionType ConnectionType
        {
            get { return _connType; }
            set
            {
                if (_ps == PoolState.Stop)
                    _connType = value;
                else
                    throw new SetValueExecption();//只有在Stop状态时才可操作
            }
        }

        private string _connString = null;//连接字符串
        /// <summary>
        /// 连接池使用的连接字符串
        /// </summary>
        public string ConnectionString
        {
            get { return _connString; }
            set
            {
                if (_ps == PoolState.Stop)
                    _connString = value;
                else
                    throw new SetValueExecption();//只有在Stop状态时才可操作
            }
        }

        private DateTime _startTime;//服务启动时间
        /// <summary>
        /// 得到服务器运行时间
        /// </summary>
        public DateTime RunTime
        {
            get
            {
                if (_ps == PoolState.Stop)
                    return new DateTime(DateTime.Now.Ticks - _startTime.Ticks);
                else
                    return new DateTime(0);
            }
        }

        private int _minConnection;//最小连接数
        /// <summary>
        /// 最小连接数
        /// </summary>
        public int MinConnection
        {
            get { return _minConnection; }
            set
            {
                if (value < _maxConnection && value > 0 && value >= _keepRealConnection)
                    _minConnection = value;
                else
                    throw new ParameterBoundExecption();//参数范围应该在 0~MaxConnection之间，并且应该大于KeepConnection
            }
        }

        private int _maxConnection;//最大连接数，最大可以创建的连接数目
        /// <summary>
        /// 最大连接数，最大可以创建的连接数目
        /// </summary>
        public int MaxConnection
        {
            get { return _maxConnection; }
            set
            {
                if (value >= _minConnection && value > 0)
                    _maxConnection = value;
                else
                    throw new ParameterBoundExecption();//参数范围错误，参数应该大于minConnection
            }
        }

        /// <summary>
        /// 每次创建连接的连接数
        /// </summary>
        private int _seepConnection;
        /// <summary>
        /// 每次创建连接的连接数
        /// </summary>
        public int SeepConnection
        {
            get { return _seepConnection; }
            set
            {
                if (value > 0 && value < _maxConnection)
                    _seepConnection = value;
                else
                    throw new ParameterBoundExecption();//创建连接的步长应大于0，同时小于MaxConnection
            }
        }

        /// <summary>
        /// 保留的实际空闲连接，以攻可能出现的ReadOnly使用，当空闲连接不足该数值时，连接池将创建seepConnection个连接
        /// </summary>
        private int _keepRealConnection;
        /// <summary>
        /// 保留的实际空闲连接，以攻可能出现的ReadOnly使用
        /// </summary>
        public int KeepRealConnection
        {
            get { return _keepRealConnection; }
            set
            {
                if (value >= 0 && value < _maxConnection)
                    _keepRealConnection = value;
                else
                    throw new ParameterBoundExecption();//保留连接数应大于等于0，同时小于MaxConnection
            }
        }

        /// <summary>
        /// 自动清理连接池的时间间隔
        /// </summary>
        public double Interval
        {
            get { return _timer.Interval; }
            set { _timer.Interval = value; }
        }

        private int _exist = 20;//每个连接生存期限 20分钟
        /// <summary>
        /// 每个连接生存期限(单位分钟)，默认20分钟
        /// </summary>
        public int Exist
        {
            get { return _exist; }
            set
            {
                if (_ps == PoolState.Stop)
                    _exist = value;
                else
                    throw new PoolNotStopException();//只有在Stop状态下才可以操作
            }
        }
        /// <summary>
        /// 可以被重复使用次数（引用记数），当连接被重复分配该值所表示的次数时，该连接将不能被分配出去
        /// </summary>
        private int _maxRepeatTimes = 5;
        /// <summary>
        /// 可以被重复使用次数（引用记数）当连接被重复分配该值所表示的次数时，该连接将不能被分配出去。
        /// 当连接池的连接被分配尽时，连接池会在已经分配出去的连接中，重复分配连接（引用记数）。来缓解连接池压力
        /// </summary>
        public int MaxRepeatTimes
        {
            get { return _maxRepeatTimes; }
            set
            {
                if (value >= 0)
                    _maxRepeatTimes = value;
                else
                    throw new ParameterBoundExecption();//重复引用次数应大于等于0
            }
        }

        /// <summary>
        /// 连接池最多可以提供多少个连接
        /// </summary>
        public int MaxConnectionFromPool
        {
            get { return _maxConnection * _maxRepeatTimes; }
        }

        private int _potentRealFromPool;//连接池中存在的实际连接数(有效的实际连接)
        /// <summary>
        /// 连接池中存在的实际连接数(有效的实际连接)
        /// </summary>
        public int PotentRealFromPool
        {
            get
            {
                if (_ps == PoolState.Run)
                    return _potentRealFromPool;
                else
                    throw new PoolNotRunException();//连接池处在非运行中
            }
        }

        private int _realFromPool;//连接池中存在的实际连接数(包含失效的连接)
        /// <summary>
        /// 连接池中存在的实际连接数(包含失效的连接)
        /// </summary>
        public int RealFromPool
        {
            get
            {
                if (_ps == PoolState.Run)
                    return _realFromPool;
                else
                    throw new PoolNotRunException();//连接池处在非运行中
            }
        }

        private int _spareRealFromPool;//空闲的实际连接
        /// <summary>
        /// 空闲的实际连接
        /// </summary>
        public int SpareRealFromPool
        {
            get
            {
                if (_ps == PoolState.Run)
                    return _spareRealFromPool;
                else
                    throw new PoolNotRunException();//连接池处在非运行中
            }
        }

        private int _useRealFromPool;//已分配的实际连接
        /// <summary>
        /// 已分配的实际连接
        /// </summary>
        public int UseRealFromPool
        {
            get
            {
                if (_ps == PoolState.Run)
                    return _useRealFromPool;
                else
                    throw new PoolNotRunException();//连接池处在非运行中
            }
        }

        /// <summary>
        /// 连接池已经分配多少只读连接
        /// </summary>
        private int _readOnlyFromPool;
        /// <summary>
        /// 连接池已经分配多少只读连接
        /// </summary>
        public int ReadOnlyFromPool
        {
            get
            {
                if (_ps == PoolState.Run)
                    return _readOnlyFromPool;
                else
                    throw new PoolNotRunException();//连接池处在非运行中
            }
        }

        private int _useFromPool;//已经分配出去的连接数
        /// <summary>
        /// 已经分配的连接数
        /// </summary>
        public int UseFromPool
        {
            get
            {
                if (_ps == PoolState.Run)
                    return _useFromPool;
                else
                    throw new PoolNotRunException();//连接池处在非运行中
            }
        }


        private int _spareFromPool;//目前可以提供的连接数
        /// <summary>
        /// 目前可以提供的连接数
        /// </summary>
        public int SpareFromPool
        {
            get
            {
                if (_ps == PoolState.Run)
                    return _spareFromPool;
                else
                    throw new PoolNotRunException();//连接池处在非运行中
            }
        }


        #endregion

        #region Constructor & Initialization

        /// <summary>
        /// 初始化连接池
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="cte">数据库连接类型</param>
        /// <param name="maxConnection">最大连接数，最大可以创建的连接数目</param>
        /// <param name="minConnection">最小连接数</param>
        /// <param name="seepConnection">每次创建连接的连接数</param>
        /// <param name="keepConnection">保留连接数，当空闲连接不足该数值时，连接池将创建seepConnection个连接</param>
        /// <param name="keepRealConnection">当空闲的实际连接不足该值时创建连接，直到达到最大连接数</param>
        private ConnectionPool(string connectionString, ConnectionType cte, int maxConnection, int minConnection, int seepConnection, int keepConnection, int keepRealConnection)
        {
            this.useConns = new Hashtable();
            this.connList = new List<Connector>();
            InitConnectionPool(connectionString, cte, maxConnection, minConnection, seepConnection, keepConnection, keepRealConnection);
        }

        public static ConnectionPool GetInstance(string connectionString)
        {
            return GetInstance(connectionString, ConnectionType.Odbc, 200, 30, 10, 5, 5);
        }

        public static ConnectionPool GetInstance(string connectionString, ConnectionType connType)
        {
            return GetInstance(connectionString, connType, 200, 30, 10, 5, 5);
        }

        public static ConnectionPool GetInstance(string connectionString, ConnectionType cte, int maxConnection, int minConnection)
        {
            return GetInstance(connectionString, cte, maxConnection, minConnection, 10, 5, 5);
        }
        public static ConnectionPool GetInstance(string connectionString, ConnectionType cte, int maxConnection, int minConnection, int seepConnection, int keepConnection)
        {
            return GetInstance(connectionString, cte, maxConnection, minConnection, seepConnection, keepConnection, 5);
        }

        public static ConnectionPool GetInstance(string connectionString, ConnectionType cte, int maxConnection, int minConnection, int seepConnection, int keepConnection, int keepRealConnection)
        {
            ConnectionPool obj;
            if (_instance == null)
            {
                lock (PoolLocker)
                {
                    if (_instance == null)
                    {
                        obj = new ConnectionPool(connectionString, cte, maxConnection, minConnection, seepConnection, keepConnection, keepRealConnection);
                        _instance = obj;
                    }
                }
            }

            return _instance;
        }

        /// <summary>
        /// 初始化函数
        /// </summary>
        protected void InitConnectionPool(string connectionString, ConnectionType cte, int maxConnection, int minConnection, int seepConnection, int keepConnection, int keepRealConnection)
        {
            if (cte == ConnectionType.None)
                throw new ConnTypeExecption();//参数不能是None

            _ps = PoolState.UnInitialize;
            this._connString = connectionString;
            this._connType = cte;
            this._minConnection = minConnection;
            this._seepConnection = seepConnection;
            this._keepRealConnection = keepRealConnection;
            this._maxConnection = maxConnection;
            this._timer = new Timer(500);
            this._timer.Stop();
            this._timer.Elapsed += timer_Elapsed;
            this._threadCreate = new Thread(new ThreadStart(CreateThreadProcess));
        }


        #endregion

        #region Event

        void timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            Connector conn = null;
            _timer.Stop();
            _isThreadCheckRun = true;

            //如果正在执行创建连接则退出
            if (_threadCreate.ThreadState != ThreadState.WaitSleepJoin)
                return;

            lock (connList)
            {
                for (int i = 0; i < connList.Count; i++)
                {
                    conn = connList[i];
                    TestConnection(conn);
                    //假如是没有引用的失效连接
                    if (conn.Enable == false && conn.CurrentRepeat == 0)
                    {
                        conn.Close();
                        connList.Remove(conn);
                    }
                }
            }

            //更新pool状态
            UpdatePoolState();

            //保留空闲实际连接数不足时
            _createThreadProcessNum = _spareRealFromPool < _keepRealConnection ? GetNumOf(_readOnlyFromPool, _seepConnection, _maxConnection) : 0;


            if (_createThreadProcessNum != 0)
            {
                _createThreadMode = CreateThreadMode.DynamicCreateMode;
                _threadCreate.Interrupt();
            }

            _isThreadCheckRun = false;
            _timer.Start();
        }

        #endregion

        #region Method

        #region 开启和关闭服务

        /// <summary>
        /// 启动服务，线程安全
        /// </summary>
        public void StartServices()
        {
            StartServices(false);
        }

        /// <summary>
        /// 启动服务，线程安全
        /// </summary>
        /// <param name="asyn">是否为异步调用，true</param>
        public void StartServices(bool asyn)
        {
            lock (this)
            {
                _createThreadMode = CreateThreadMode.StaticCreateMode;
                _createThreadProcessRun = true;
                _createThreadProcessNum = _minConnection;
                if (_ps == PoolState.UnInitialize)
                    _threadCreate.Start();
                else if (_ps == PoolState.Stop)
                    _threadCreate.Interrupt();
                else
                    throw new PoolNotStopException();//服务已经运行或者未完全结束
                _timer.Start();
            }

            if (!asyn)
                while (_threadCreate.ThreadState != ThreadState.WaitSleepJoin) { Thread.Sleep(50); }//等待可能存在的创建线程结束
        }

        public void StopServices()
        {
            StopServices(false);
        }

        public void StopServices(bool needs)
        {
            lock (this)
            {
                if (_ps == PoolState.Run)
                {
                    lock (useConns)
                    {
                        if (needs == true)//必须退出
                            useConns.Clear();
                        else
                            if (useConns.Count != 0)
                                throw new ResCallBackException();//连接池资源未全部回收
                    }
                    _timer.Stop();
                    while (_isThreadCheckRun) { Thread.Sleep(50); }//等待timer事件结束
                    _createThreadProcessRun = false;
                    while (_threadCreate.ThreadState != ThreadState.WaitSleepJoin) { Thread.Sleep(50); }//等待可能存在的创建线程结束
                    lock (connList)
                    {
                        for (int i = 0; i < connList.Count; i++)
                            connList[i].Dispose();
                        connList.Clear();
                    }
                    _ps = PoolState.Stop;
                }
                else
                    throw new PoolNotRunException();//服务未启动
            }

            UpdatePoolState();//更新属性
        }

        #endregion

        #region 获得连接合释放连接

        public DbConnection GetConnection(object key)
        {
            return GetConnection(key, ConnectionLevel.None);
        }

        public DbConnection GetConnection(object key, ConnectionLevel level)
        {
            lock (PoolLocker)
            {
                if (_ps != PoolState.Run)
                    throw new StateException();//服务状态错误
                if (useConns.Count == MaxConnectionFromPool)
                    throw new PoolFullException();//连接池已经饱和，不能提供连接
                if (useConns.ContainsKey(key))
                    throw new KeyExecption();//一个key只能申请一个连接

                switch (level)
                {
                    case ConnectionLevel.ReadOnly:
                        return GetConnectionByReadOnly(key);
                    case ConnectionLevel.High:
                        return GetConnectionByHigh(key);
                    case ConnectionLevel.None:
                        return GetConnectionByNone(key);
                    default:
                        return GetConnectionByBase(key);
                }
            }
        }

        public DbConnection GetConnection(object key, ConnectionLevel level, Connector conn)
        {
            try
            {
                if (conn == null)
                    return null;

                if (conn.DbConnection.State != ConnectionState.Closed)
                    return null;

                conn.Repeat();
                useConns.Add(key, conn);
                if (level == ConnectionLevel.ReadOnly)
                {
                    conn.CanAllocation = false;
                    conn.IsRepeat = false;
                }
            }
            catch
            {
                throw new OccasionExecption();
            }
            finally
            {
                UpdatePoolState();
            }

            return conn.DbConnection;
        }

        public DbConnection GetConnectionByReadOnly(object key)
        {
            Connector conn = null;
            for (int i = 0; i < connList.Count; i++)
            {
                conn = connList[i];
                if (conn.Enable == false || conn.CanAllocation == false || conn.UseTimes == _maxRepeatTimes || conn.IsUse == true)
                    continue;

                return GetConnection(key, ConnectionLevel.ReadOnly, conn);
            }
            throw new OccasionExecption();
        }

        public DbConnection GetConnectionByHigh(object key)
        {
            Connector selectedConn = null;
            Connector curConn = null;

            for (int i = 0; i < connList.Count; i++)
            {
                curConn = connList[i];

                //不可分配的连接跳出本次循环
                if (curConn.Enable == false || curConn.CanAllocation == false || curConn.UseTimes == _maxRepeatTimes)
                    continue;

                //找到最合适的
                if (curConn.UseTimes == 0)
                {
                    selectedConn = curConn;
                    break;
                }

                if (selectedConn != null)
                {
                    if (curConn.UseTimes < selectedConn.UseTimes)
                        selectedConn = curConn;
                }
                else
                    selectedConn = curConn;
            }

            return GetConnection(key, ConnectionLevel.High, selectedConn);
        }

        public DbConnection GetConnectionByNone(object key)
        {
            var allotList = new List<Connector>();

            Connector conn = null;
            for (int i = 0; i < connList.Count; i++)
            {
                conn = connList[i];
                if (conn.Enable == false || conn.CanAllocation == false || conn.UseTimes == _maxRepeatTimes)
                    continue;

                if (conn.CanAllocation == true)
                    allotList.Add(conn);
            }

            return allotList.Count == 0 ? null : GetConnection(key, ConnectionLevel.None, allotList[allotList.Count / 2]);
        }

        public DbConnection GetConnectionByBase(object key)
        {
            Connector selectedConn = null;
            Connector curConn = null;

            for (int i = 0; i < connList.Count; i++)
            {
                curConn = connList[i];
                if (curConn.Enable == false || curConn.CanAllocation == false || curConn.UseTimes == _maxRepeatTimes)
                {
                    continue;
                }

                if (selectedConn != null)
                {
                    if (curConn.UseTimes > selectedConn.UseTimes)
                        selectedConn = curConn;
                }
                else
                    selectedConn = curConn;
            }

            return GetConnection(key, ConnectionLevel.High, selectedConn);
        }

        /// <summary>
        /// 释放申请的数据库连接对象，线程安全
        /// </summary>
        /// <param name="key"></param>
        public void DisposeConnection(object key)
        {
            lock (useConns)
            {
                Connector conn = null;
                if (_ps == PoolState.Run)
                {
                    if (!useConns.ContainsKey(key))
                        throw new NotKeyExecption(); //无法释放，Key不存在
                    conn = (Connector)useConns[key];
                    conn.IsRepeat = true;
                    if (conn.CanAllocation == false)
                        if (conn.Enable == true)
                            conn.CanAllocation = true;
                    conn.Remove();
                    useConns.Remove(key);
                }
                else
                    throw new PoolNotRunException();//服务未启动
            }

            UpdatePoolState();
        }

        #endregion


        /// <summary>
        /// 创建连接
        /// </summary>
        public void CreateThreadProcess()
        {
            bool join = false;
            int createThreadProcessNum_inside = _createThreadProcessNum;
            _ps = PoolState.Initialize;
            while (true)
            {
                join = false;
                _ps = PoolState.Run;

                if (_createThreadProcessRun == false) //终止命令
                {
                    try
                    {
                        _threadCreate.Join();
                    }
                    catch (Exception)
                    {

                        throw;
                    }
                }
                else
                {
                    if (_createThreadMode == CreateThreadMode.StaticCreateMode)
                    {
                        lock (connList)
                        {
                            if (connList.Count < createThreadProcessNum_inside)
                            {
                                connList.Add(CreateConnection(_connString, _connType));
                            }
                            else
                            {
                                join = true;
                            }
                        }

                    }
                    else if (_createThreadMode == CreateThreadMode.DynamicCreateMode)
                    {
                        lock (connList)
                        {
                            if (createThreadProcessNum_inside != 0)
                            {
                                createThreadProcessNum_inside--;
                                connList.Add(CreateConnection(_connString, _connType));
                            }
                        }
                    }
                    else
                    {
                        join = true;
                    }

                    if (join)
                    {
                        UpdatePoolState();
                        try
                        {
                            _createThreadProcessNum = 0;
                            _threadCreate.Join();
                        }
                        catch (Exception)
                        {
                            createThreadProcessNum_inside = _createThreadProcessNum;
                        }
                    }
                }
            }

        }

        /// <summary>
        /// 计算要增加的连接
        /// </summary>
        /// <returns></returns>
        private int GetNumOf(int nowNum, int seepNum, int maxNum)
        {
            return (maxNum >= nowNum + seepNum) ? seepNum : maxNum - nowNum;

        }

        private Connector CreateConnection(string connStr, ConnectionType connType)
        {
            DbConnection dbConn = null;
            switch (connType)
            {
                case ConnectionType.Odbc:
                    break;
                case ConnectionType.OleDb:
                    break;
                case ConnectionType.SqlClient:
                    break;
                default:
                    throw new ArgumentException("无效的参数");
            }
            Connector conn = new Connector(dbConn, connType, DateTime.Now);
            conn.Open();
            return conn;
        }


        private void TestConnection(Connector conn)
        {
            // 此次分配的连接是否还可以使用
            if (conn.UseTimes == _maxRepeatTimes)
                conn.SetConnectionFailure();//超过规定的使用次数
            if (conn.CreateTime.AddMinutes(_exist).Ticks <= DateTime.Now.Ticks)
                conn.SetConnectionFailure();//连接超时
            if (conn.DbConnection.State == ConnectionState.Closed)
                conn.SetConnectionFailure();//连接关闭
        }

        private void UpdatePoolState()
        {
            int tmp_readOnlyFromPool = 0;//连接池已经分配多少只读连接
            int tmp_potentRealFromPool = 0;//连接池中存在的实际连接数(有效的实际连接)
            int tmp_spareRealFromPool = 0;//空闲的实际连接
            int tmp_useRealFromPool = 0;//已分配的实际连接
            int tmp_spareFromPool = MaxConnectionFromPool;//目前可以提供的连接数

            lock (useConns)
            {
                _useFromPool = useConns.Count;
            }

            Connector conn = null;
            lock (connList)
            {
                _realFromPool = connList.Count;
                for (int i = 0; i < connList.Count; i++)
                {
                    conn = connList[i];
                    //只读
                    if (conn.CanAllocation == false && conn.IsUse == true && conn.IsRepeat == false)
                        tmp_readOnlyFromPool++;
                    //有效的实际连接
                    if (conn.Enable == true)
                        tmp_potentRealFromPool++;
                    //空闲的实际连接
                    if (conn.Enable == true && conn.IsUse == false)
                        tmp_spareRealFromPool++;
                    //已分配的实际连接
                    if (conn.IsUse == true)
                        tmp_useRealFromPool++;
                    //目前可以提供的连接数
                    if (conn.CanAllocation == true)
                        tmp_spareFromPool = tmp_spareFromPool - conn.UseTimes;
                    else
                        tmp_spareFromPool = tmp_spareRealFromPool - _maxRepeatTimes;
                }
            }

            _readOnlyFromPool = tmp_readOnlyFromPool;
            _potentRealFromPool = tmp_potentRealFromPool;
            _spareRealFromPool = tmp_spareRealFromPool;
            _spareFromPool = tmp_spareFromPool;
            _useRealFromPool = tmp_useRealFromPool;

        }



        #endregion


        public void Dispose()
        {
            try
            {
                this.StopServices();
                _threadCreate.Abort();
            }
            catch (Exception e) { }
        }
    }
}
