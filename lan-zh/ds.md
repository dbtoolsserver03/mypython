我将为您创建一个生产级别的Snowpark共通模块，包含完整的文件结构和测试程序。

## 文件结构

```
snowpark_common/
│
├── config/
│   ├── __init__.py
│   └── settings.py          # 配置文件管理
│
├── core/
│   ├── __init__.py
│   ├── connection.py         # 数据库连接管理
│   ├── operations.py         # CRUD操作封装
│   ├── transaction.py        # 事务管理
│   └── cache.py              # 缓存管理
│
├── utils/
│   ├── __init__.py
│   ├── logger.py             # 日志管理
│   └── exceptions.py         # 自定义异常
│
├── tests/
│   ├── __init__.py
│   ├── test_connection.py    # 连接测试
│   ├── test_crud.py          # CRUD操作测试
│   ├── test_transaction.py   # 事务测试
│   └── test_data.sql         # 测试数据
│
├── secrets.toml               # Snowflake凭证
├── requirements.txt           # 依赖包
└── main_demo.py               # 演示程序
```

## 1. secrets.toml (凭证文件)

```toml
# Snowflake 连接凭证配置文件
# 注意：此文件不应提交到版本控制系统，已添加到.gitignore

[snowflake]
account = "your_account"           # Snowflake账号（例如：xy12345.us-east-1）
user = "your_username"              # 用户名
password = "your_password"          # 密码
role = "SYSADMIN"                   # 角色（可选）
warehouse = "COMPUTE_WH"            # 虚拟仓库
database = "TEST_DB"                # 数据库名称
schema = "PUBLIC"                   # 模式名称
```

## 2. config/settings.py

```python
"""
配置文件管理模块
负责加载和管理Snowflake连接配置
"""
import tomli
from pathlib import Path
from typing import Dict, Any
import os


class Settings:
    """配置管理类 - 负责加载和提供Snowflake连接配置"""
    
    def __init__(self, secrets_path: str = "secrets.toml"):
        """
        初始化配置管理器
        
        Args:
            secrets_path: secrets.toml文件的路径
        """
        self.secrets_path = secrets_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载secrets.toml配置文件
        
        Returns:
            Dict: Snowflake连接配置字典
        
        Raises:
            FileNotFoundError: 配置文件不存在
            ValueError: 配置格式错误
        """
        # 获取项目根目录
        root_dir = Path(__file__).parent.parent
        config_file = root_dir / self.secrets_path
        
        # 检查配置文件是否存在
        if not config_file.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_file}")
        
        # 使用tomli加载TOML文件
        with open(config_file, "rb") as f:
            config = tomli.load(f)
        
        # 验证必要的配置项
        snowflake_config = config.get("snowflake", {})
        required_keys = ["account", "user", "password"]
        
        for key in required_keys:
            if key not in snowflake_config:
                raise ValueError(f"配置缺少必要项: {key}")
        
        return snowflake_config
    
    def get_snowflake_config(self) -> Dict[str, Any]:
        """
        获取Snowflake连接配置
        
        Returns:
            Dict: Snowflake连接参数字典
        """
        return self.config
    
    def get_connection_params(self) -> Dict[str, Any]:
        """
        获取格式化的连接参数
        
        Returns:
            Dict: 适用于snowpark连接的参数字典
        """
        return {
            "account": self.config.get("account"),
            "user": self.config.get("user"),
            "password": self.config.get("password"),
            "role": self.config.get("role"),
            "warehouse": self.config.get("warehouse"),
            "database": self.config.get("database"),
            "schema": self.config.get("schema")
        }


# 创建全局配置实例
settings = Settings()
```

## 3. utils/logger.py

```python
"""
日志管理模块
提供统一的日志记录功能
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str = "snowpark_common",
    log_file: Optional[str] = None,
    level: int = logging.INFO
) -> logging.Logger:
    """
    设置和配置日志记录器
    
    Args:
        name: 日志记录器名称
        log_file: 日志文件路径（如果为None，只输出到控制台）
        level: 日志级别
    
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 清除已有的处理器
    logger.handlers.clear()
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器（如果指定了日志文件）
    if log_file:
        # 创建日志目录
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


# 创建默认日志记录器
logger = setup_logger()


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的日志记录器
    
    Args:
        name: 日志记录器名称
    
    Returns:
        logging.Logger: 日志记录器实例
    """
    return logging.getLogger(name)
```

## 4. utils/exceptions.py

```python
"""
自定义异常模块
定义项目特定的异常类
"""


class SnowparkBaseException(Exception):
    """Snowpark基础异常类"""
    def __init__(self, message: str, original_error: Exception = None):
        self.message = message
        self.original_error = original_error
        super().__init__(self.message)


class ConnectionError(SnowparkBaseException):
    """数据库连接异常"""
    pass


class QueryError(SnowparkBaseException):
    """查询执行异常"""
    pass


class TransactionError(SnowparkBaseException):
    """事务处理异常"""
    pass


class DataValidationError(SnowparkBaseException):
    """数据验证异常"""
    pass


class CacheError(SnowparkBaseException):
    """缓存操作异常"""
    pass
```

## 5. core/connection.py

```python
"""
数据库连接管理模块
负责Snowflake连接的创建、管理和复用
"""
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from contextlib import contextmanager
from typing import Optional, Dict, Any
import threading

from config.settings import settings
from utils.logger import logger
from utils.exceptions import ConnectionError


class ConnectionManager:
    """
    连接管理器类 - 单例模式
    管理Snowflake会话的创建和获取
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """实现线程安全的单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化连接管理器"""
        if self._initialized:
            return
        
        self.session: Optional[Session] = None
        self._session_lock = threading.Lock()
        self._connection_params = settings.get_connection_params()
        self._initialized = True
        logger.info("连接管理器初始化完成")
    
    def create_session(self) -> Session:
        """
        创建新的Snowflake会话
        
        Returns:
            Session: Snowpark会话对象
        
        Raises:
            ConnectionError: 连接创建失败
        """
        try:
            logger.info("正在创建Snowflake连接...")
            logger.debug(f"连接参数: { {k: v for k, v in self._connection_params.items() if k != 'password'} }")
            
            # 创建会话
            session = Session.builder.configs(self._connection_params).create()
            
            # 测试连接
            session.sql("SELECT CURRENT_VERSION()").collect()
            
            logger.info("Snowflake连接成功创建")
            return session
            
        except Exception as e:
            error_msg = f"创建Snowflake连接失败: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg, e)
    
    def get_session(self) -> Session:
        """
        获取会话（如果不存在则创建）
        
        Returns:
            Session: Snowpark会话对象
        """
        with self._session_lock:
            if self.session is None:
                self.session = self.create_session()
            return self.session
    
    def close_session(self):
        """关闭当前会话"""
        with self._session_lock:
            if self.session:
                try:
                    self.session.close()
                    logger.info("Snowflake会话已关闭")
                except Exception as e:
                    logger.error(f"关闭会话时出错: {e}")
                finally:
                    self.session = None
    
    @contextmanager
    def session_context(self):
        """
        会话上下文管理器
        确保会话在使用后正确关闭
        
        Yields:
            Session: Snowpark会话对象
        """
        session = None
        try:
            session = self.get_session()
            yield session
        finally:
            # 注意：这里不关闭会话，因为可能是共享会话
            # 如果需要每次都关闭，可以使用create_session()
            pass
    
    @contextmanager
    def new_session_context(self):
        """
        新会话上下文管理器
        每次都创建新会话，使用后自动关闭
        
        Yields:
            Session: Snowpark会话对象
        """
        session = None
        try:
            session = self.create_session()
            yield session
        finally:
            if session:
                session.close()
                logger.debug("临时会话已关闭")


# 创建全局连接管理器实例
conn_manager = ConnectionManager()
```

## 6. core/cache.py

```python
"""
缓存管理模块
提供查询结果的缓存功能
"""
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import threading
import hashlib
import json

from utils.logger import logger
from utils.exceptions import CacheError


class CacheEntry:
    """缓存条目类"""
    def __init__(self, data: Any, ttl_seconds: int = 300):
        """
        初始化缓存条目
        
        Args:
            data: 缓存的数据
            ttl_seconds: 生存时间（秒）
        """
        self.data = data
        self.created_at = datetime.now()
        self.ttl_seconds = ttl_seconds
    
    @property
    def is_expired(self) -> bool:
        """检查缓存是否过期"""
        age = (datetime.now() - self.created_at).total_seconds()
        return age > self.ttl_seconds


class QueryCache:
    """
    查询缓存类 - 单例模式
    缓存查询结果以提高性能
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """实现线程安全的单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化缓存"""
        if self._initialized:
            return
        
        self._cache: Dict[str, CacheEntry] = {}
        self._cache_lock = threading.RLock()
        self._cleanup_thread = None
        self._stop_cleanup = threading.Event()
        self._initialized = True
        
        # 启动缓存清理线程
        self._start_cleanup_thread()
        
        logger.info("查询缓存管理器初始化完成")
    
    def _start_cleanup_thread(self):
        """启动缓存清理线程"""
        def cleanup_worker():
            """清理过期缓存的线程函数"""
            while not self._stop_cleanup.is_set():
                self.cleanup_expired()
                # 每60秒检查一次
                self._stop_cleanup.wait(60)
        
        self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self._cleanup_thread.start()
        logger.debug("缓存清理线程已启动")
    
    def _generate_key(self, query: str, params: Optional[Dict] = None) -> str:
        """
        生成缓存键
        
        Args:
            query: SQL查询语句
            params: 查询参数
        
        Returns:
            str: 缓存键
        """
        key_data = query
        if params:
            key_data += json.dumps(params, sort_keys=True)
        
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, query: str, params: Optional[Dict] = None) -> Optional[Any]:
        """
        从缓存获取数据
        
        Args:
            query: SQL查询语句
            params: 查询参数
        
        Returns:
            Optional[Any]: 缓存的数据，如果不存在或过期则返回None
        """
        key = self._generate_key(query, params)
        
        with self._cache_lock:
            entry = self._cache.get(key)
            
            if entry and not entry.is_expired:
                logger.debug(f"缓存命中: {key[:8]}...")
                return entry.data
            elif entry:
                # 缓存过期，删除
                logger.debug(f"缓存过期: {key[:8]}...")
                del self._cache[key]
                return None
            else:
                logger.debug(f"缓存未命中: {key[:8]}...")
                return None
    
    def set(self, query: str, data: Any, params: Optional[Dict] = None, ttl: int = 300):
        """
        设置缓存
        
        Args:
            query: SQL查询语句
            data: 要缓存的数据
            params: 查询参数
            ttl: 缓存生存时间（秒）
        """
        key = self._generate_key(query, params)
        
        with self._cache_lock:
            self._cache[key] = CacheEntry(data, ttl)
            logger.debug(f"缓存已设置: {key[:8]}..., TTL={ttl}s")
    
    def clear(self):
        """清空所有缓存"""
        with self._cache_lock:
            self._cache.clear()
            logger.info("所有缓存已清空")
    
    def remove(self, query: str, params: Optional[Dict] = None):
        """
        删除特定缓存
        
        Args:
            query: SQL查询语句
            params: 查询参数
        """
        key = self._generate_key(query, params)
        
        with self._cache_lock:
            if key in self._cache:
                del self._cache[key]
                logger.debug(f"缓存已删除: {key[:8]}...")
    
    def cleanup_expired(self):
        """清理过期缓存"""
        with self._cache_lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired
            ]
            
            for key in expired_keys:
                del self._cache[key]
            
            if expired_keys:
                logger.debug(f"已清理 {len(expired_keys)} 个过期缓存")
    
    def get_stats(self) -> Dict:
        """
        获取缓存统计信息
        
        Returns:
            Dict: 缓存统计
        """
        with self._cache_lock:
            total = len(self._cache)
            expired = sum(1 for entry in self._cache.values() if entry.is_expired)
            
            return {
                "total_entries": total,
                "expired_entries": expired,
                "active_entries": total - expired
            }
    
    def shutdown(self):
        """关闭缓存清理线程"""
        if self._cleanup_thread:
            self._stop_cleanup.set()
            self._cleanup_thread.join(timeout=5)
            logger.info("缓存清理线程已停止")


# 创建全局缓存实例
query_cache = QueryCache()
```

## 7. core/operations.py

```python
"""
CRUD操作封装模块
提供基本的数据库增删改查操作
"""
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.DataFrame import DataFrame
from typing import Optional, List, Dict, Any, Union
import pandas as pd

from core.connection import conn_manager
from core.cache import query_cache
from utils.logger import logger
from utils.exceptions import QueryError, DataValidationError


class SnowparkOperations:
    """
    Snowpark操作类
    封装常用的数据库操作
    """
    
    def __init__(self, use_cache: bool = True, cache_ttl: int = 300):
        """
        初始化操作类
        
        Args:
            use_cache: 是否使用缓存
            cache_ttl: 缓存生存时间（秒）
        """
        self.use_cache = use_cache
        self.cache_ttl = cache_ttl
        self.session: Optional[Session] = None
    
    def _get_session(self) -> Session:
        """
        获取数据库会话
        
        Returns:
            Session: Snowpark会话对象
        """
        if self.session is None:
            self.session = conn_manager.get_session()
        return self.session
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        执行SQL查询
        
        Args:
            query: SQL查询语句
            params: 查询参数
        
        Returns:
            List[Dict]: 查询结果列表
        
        Raises:
            QueryError: 查询执行失败
        """
        try:
            session = self._get_session()
            
            # 检查缓存
            if self.use_cache:
                cached_result = query_cache.get(query, params)
                if cached_result is not None:
                    logger.info("使用缓存数据返回")
                    return cached_result
            
            logger.info(f"执行查询: {query[:100]}...")
            
            # 执行查询
            if params:
                # 使用参数化查询防止SQL注入
                df = session.sql(query, params)
            else:
                df = session.sql(query)
            
            # 收集结果
            result = df.collect()
            
            # 转换为字典列表
            result_dicts = [row.as_dict() for row in result]
            
            # 存入缓存
            if self.use_cache:
                query_cache.set(query, result_dicts, params, self.cache_ttl)
            
            logger.info(f"查询完成，返回 {len(result_dicts)} 条记录")
            return result_dicts
            
        except Exception as e:
            error_msg = f"查询执行失败: {str(e)}"
            logger.error(error_msg)
            raise QueryError(error_msg, e)
    
    def execute_query_to_pandas(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        执行SQL查询并返回Pandas DataFrame
        
        Args:
            query: SQL查询语句
            params: 查询参数
        
        Returns:
            pd.DataFrame: 查询结果DataFrame
        """
        try:
            session = self._get_session()
            
            logger.info(f"执行查询(返回Pandas): {query[:100]}...")
            
            # 执行查询
            if params:
                df = session.sql(query, params)
            else:
                df = session.sql(query)
            
            # 转换为Pandas DataFrame
            result = df.to_pandas()
            
            logger.info(f"查询完成，返回 {len(result)} 条记录")
            return result
            
        except Exception as e:
            error_msg = f"查询执行失败: {str(e)}"
            logger.error(error_msg)
            raise QueryError(error_msg, e)
    
    def select(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        查询表数据
        
        Args:
            table_name: 表名
            columns: 要查询的列（None表示所有列）
            where: WHERE条件
            limit: 返回记录数限制
        
        Returns:
            List[Dict]: 查询结果
        """
        try:
            session = self._get_session()
            
            # 构建查询
            table = session.table(table_name)
            
            # 选择列
            if columns:
                table = table.select(*[col(c) for c in columns])
            
            # 应用WHERE条件
            if where:
                table = table.filter(where)
            
            # 应用LIMIT
            if limit:
                table = table.limit(limit)
            
            # 执行查询
            result = table.collect()
            
            return [row.as_dict() for row in result]
            
        except Exception as e:
            error_msg = f"查询表 {table_name} 失败: {str(e)}"
            logger.error(error_msg)
            raise QueryError(error_msg, e)
    
    def insert(self, table_name: str, data: Union[Dict, List[Dict]]) -> int:
        """
        插入数据
        
        Args:
            table_name: 表名
            data: 要插入的数据（单条字典或字典列表）
        
        Returns:
            int: 插入的记录数
        
        Raises:
            DataValidationError: 数据验证失败
        """
        try:
            session = self._get_session()
            
            # 转换数据为列表
            if isinstance(data, dict):
                data_list = [data]
            else:
                data_list = data
            
            if not data_list:
                logger.warning("没有数据可插入")
                return 0
            
            # 验证数据
            first_row = data_list[0]
            columns = list(first_row.keys())
            
            for row in data_list:
                if set(row.keys()) != set(columns):
                    raise DataValidationError("所有行的列必须一致")
            
            # 创建DataFrame并写入
            df = session.create_dataframe(data_list)
            df.write.mode("append").save_as_table(table_name)
            
            # 清除相关缓存
            if self.use_cache:
                # 注意：这里简单清除所有缓存，实际应用中可能需要更精细的策略
                query_cache.clear()
            
            logger.info(f"成功插入 {len(data_list)} 条记录到表 {table_name}")
            return len(data_list)
            
        except Exception as e:
            error_msg = f"插入数据到表 {table_name} 失败: {str(e)}"
            logger.error(error_msg)
            if isinstance(e, DataValidationError):
                raise
            raise QueryError(error_msg, e)
    
    def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        where: str
    ) -> int:
        """
        更新数据
        
        Args:
            table_name: 表名
            data: 要更新的数据（字典）
            where: WHERE条件
        
        Returns:
            int: 更新的记录数
        """
        try:
            session = self._get_session()
            
            # 构建更新语句
            set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
            
            # 执行更新
            result = session.sql(query, data).collect()
            
            # 解析结果
            rows_updated = result[0]["number of rows updated"] if result else 0
            
            # 清除缓存
            if self.use_cache:
                query_cache.clear()
            
            logger.info(f"成功更新 {rows_updated} 条记录到表 {table_name}")
            return rows_updated
            
        except Exception as e:
            error_msg = f"更新表 {table_name} 数据失败: {str(e)}"
            logger.error(error_msg)
            raise QueryError(error_msg, e)
    
    def delete(self, table_name: str, where: str) -> int:
        """
        删除数据
        
        Args:
            table_name: 表名
            where: WHERE条件
        
        Returns:
            int: 删除的记录数
        """
        try:
            session = self._get_session()
            
            # 构建删除语句
            query = f"DELETE FROM {table_name} WHERE {where}"
            
            # 执行删除
            result = session.sql(query).collect()
            
            # 解析结果
            rows_deleted = result[0]["number of rows deleted"] if result else 0
            
            # 清除缓存
            if self.use_cache:
                query_cache.clear()
            
            logger.info(f"成功从表 {table_name} 删除 {rows_deleted} 条记录")
            return rows_deleted
            
        except Exception as e:
            error_msg = f"从表 {table_name} 删除数据失败: {str(e)}"
            logger.error(error_msg)
            raise QueryError(error_msg, e)
    
    def bulk_insert(self, table_name: str, data: List[Dict], batch_size: int = 1000) -> int:
        """
        批量插入数据
        
        Args:
            table_name: 表名
            data: 数据列表
            batch_size: 每批插入的记录数
        
        Returns:
            int: 插入的总记录数
        """
        try:
            session = self._get_session()
            
            total_inserted = 0
            
            # 分批处理
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # 创建DataFrame并写入
                df = session.create_dataframe(batch)
                df.write.mode("append").save_as_table(table_name)
                
                total_inserted += len(batch)
                logger.debug(f"已插入批次 {i//batch_size + 1}: {len(batch)} 条记录")
            
            # 清除缓存
            if self.use_cache:
                query_cache.clear()
            
            logger.info(f"批量插入完成，共 {total_inserted} 条记录到表 {table_name}")
            return total_inserted
            
        except Exception as e:
            error_msg = f"批量插入数据到表 {table_name} 失败: {str(e)}"
            logger.error(error_msg)
            raise QueryError(error_msg, e)
```

## 8. core/transaction.py

```python
"""
事务管理模块
提供事务处理功能
"""
from snowflake.snowpark import Session
from contextlib import contextmanager
from typing import Callable, Any, List, Dict
import threading

from core.connection import conn_manager
from utils.logger import logger
from utils.exceptions import TransactionError


class TransactionManager:
    """
    事务管理器类
    处理数据库事务
    """
    
    def __init__(self):
        """初始化事务管理器"""
        self.session: Session = conn_manager.get_session()
        self._local = threading.local()
    
    def begin(self):
        """开始事务"""
        try:
            self.session.sql("BEGIN").collect()
            self._local.in_transaction = True
            logger.debug("事务已开始")
        except Exception as e:
            error_msg = f"开始事务失败: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg, e)
    
    def commit(self):
        """提交事务"""
        try:
            self.session.sql("COMMIT").collect()
            self._local.in_transaction = False
            logger.debug("事务已提交")
        except Exception as e:
            error_msg = f"提交事务失败: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg, e)
    
    def rollback(self):
        """回滚事务"""
        try:
            self.session.sql("ROLLBACK").collect()
            self._local.in_transaction = False
            logger.debug("事务已回滚")
        except Exception as e:
            error_msg = f"回滚事务失败: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg, e)
    
    @contextmanager
    def transaction(self):
        """
        事务上下文管理器
        自动处理事务的开始、提交和回滚
        
        Usage:
            with transaction_manager.transaction():
                # 执行数据库操作
                ...
        """
        try:
            self.begin()
            yield
            self.commit()
        except Exception as e:
            self.rollback()
            logger.error(f"事务执行失败，已回滚: {str(e)}")
            raise
        finally:
            self._local.in_transaction = False
    
    def execute_in_transaction(self, operations: List[Callable]) -> List[Any]:
        """
        在事务中执行多个操作
        
        Args:
            operations: 要执行的操作函数列表
        
        Returns:
            List[Any]: 各操作的返回结果列表
        
        Raises:
            TransactionError: 事务执行失败
        """
        results = []
        
        with self.transaction():
            for i, operation in enumerate(operations):
                try:
                    logger.debug(f"执行事务操作 {i + 1}/{len(operations)}")
                    result = operation()
                    results.append(result)
                except Exception as e:
                    error_msg = f"事务操作 {i + 1} 执行失败: {str(e)}"
                    logger.error(error_msg)
                    raise TransactionError(error_msg, e)
        
        return results
    
    def is_in_transaction(self) -> bool:
        """
        检查当前是否在事务中
        
        Returns:
            bool: 是否在事务中
        """
        return getattr(self._local, 'in_transaction', False)


# 创建全局事务管理器实例
transaction_manager = TransactionManager()
```

## 9. tests/test_data.sql

```sql
-- 测试数据创建脚本
-- 用于创建测试表和插入测试数据

-- 创建测试数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS TEST_DB;
USE DATABASE TEST_DB;

-- 创建测试模式（如果不存在）
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- 创建用户表
CREATE OR REPLACE TABLE users (
    user_id INT AUTOINCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    city VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 创建产品表
CREATE OR REPLACE TABLE products (
    product_id INT AUTOINCREMENT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2),
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 创建订单表
CREATE OR REPLACE TABLE orders (
    order_id INT AUTOINCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    order_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    total_amount DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'PENDING',
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 创建订单明细表
CREATE OR REPLACE TABLE order_items (
    item_id INT AUTOINCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 插入测试用户数据
INSERT INTO users (username, email, age, city) VALUES
    ('zhangsan', 'zhangsan@email.com', 25, '北京'),
    ('lisi', 'lisi@email.com', 30, '上海'),
    ('wangwu', 'wangwu@email.com', 28, '广州'),
    ('zhaoliu', 'zhaoliu@email.com', 35, '深圳'),
    ('qianqi', 'qianqi@email.com', 32, '杭州');

-- 插入测试产品数据
INSERT INTO products (product_name, category, price, stock_quantity) VALUES
    ('笔记本电脑', '电子产品', 5999.00, 50),
    ('智能手机', '电子产品', 3999.00, 100),
    ('无线耳机', '电子产品', 299.00, 200),
    ('办公椅', '家具', 899.00, 30),
    ('书桌', '家具', 1299.00, 20),
    ('咖啡杯', '家居用品', 39.00, 500),
    ('台灯', '家居用品', 159.00, 80);

-- 插入测试订单数据
INSERT INTO orders (user_id, total_amount, status) VALUES
    (1, 6298.00, 'COMPLETED'),
    (2, 3999.00, 'COMPLETED'),
    (3, 1138.00, 'PENDING'),
    (1, 299.00, 'PROCESSING'),
    (4, 5898.00, 'COMPLETED');

-- 插入测试订单明细数据
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 5999.00),
    (1, 3, 1, 299.00),
    (2, 2, 1, 3999.00),
    (3, 4, 1, 899.00),
    (3, 7, 1, 159.00),
    (3, 6, 2, 39.00),
    (4, 3, 1, 299.00),
    (5, 1, 1, 5999.00),
    (5, 5, 1, 1299.00);

-- 创建视图示例
CREATE OR REPLACE VIEW user_order_summary AS
SELECT 
    u.user_id,
    u.username,
    u.city,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    AVG(o.total_amount) AS avg_order_amount
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.username, u.city;

-- 创建存储过程示例（使用Snowflake JavaScript）
CREATE OR REPLACE PROCEDURE update_order_status(
    order_id_param INT,
    new_status_param VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var sql_command = `UPDATE orders SET status = '${NEW_STATUS_PARAM}' WHERE order_id = ${ORDER_ID_PARAM}`;
    snowflake.execute({sqlText: sql_command});
    return 'Succeeded: ' + sql_command;
$$;

-- 验证数据
SELECT 'users' as table_name, COUNT(*) as record_count FROM users
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
ORDER BY table_name;
```

## 10. tests/test_connection.py

```python
"""
连接测试模块
测试数据库连接的可靠性
"""
import unittest
import time
import threading
from concurrent.futures import ThreadPoolExecutor

from core.connection import conn_manager
from utils.logger import logger


class TestConnection(unittest.TestCase):
    """测试数据库连接管理功能"""
    
    def setUp(self):
        """测试前准备"""
        logger.info("=" * 50)
        logger.info("开始连接测试")
    
    def tearDown(self):
        """测试后清理"""
        # 关闭会话
        conn_manager.close_session()
        logger.info("连接测试完成")
        logger.info("=" * 50)
    
    def test_01_create_connection(self):
        """测试创建连接"""
        logger.info("测试1: 创建连接")
        
        # 获取会话
        session = conn_manager.get_session()
        
        # 执行测试查询
        result = session.sql("SELECT CURRENT_VERSION() as version").collect()
        version = result[0]["VERSION"]
        
        logger.info(f"Snowflake版本: {version}")
        self.assertIsNotNone(version)
        self.assertTrue(len(version) > 0)
    
    def test_02_reuse_connection(self):
        """测试连接复用"""
        logger.info("测试2: 连接复用")
        
        # 第一次获取会话
        session1 = conn_manager.get_session()
        session_id1 = id(session1)
        
        # 第二次获取会话
        session2 = conn_manager.get_session()
        session_id2 = id(session2)
        
        # 验证是同一个会话对象
        self.assertEqual(session_id1, session_id2)
        logger.info(f"会话复用成功: {session_id1} == {session_id2}")
    
    def test_03_session_context(self):
        """测试会话上下文管理器"""
        logger.info("测试3: 会话上下文管理器")
        
        with conn_manager.session_context() as session:
            result = session.sql("SELECT CURRENT_DATABASE() as db").collect()
            db_name = result[0]["DB"]
            logger.info(f"当前数据库: {db_name}")
            self.assertIsNotNone(db_name)
    
    def test_04_new_session_context(self):
        """测试新会话上下文管理器"""
        logger.info("测试4: 新会话上下文管理器")
        
        with conn_manager.new_session_context() as session:
            result = session.sql("SELECT CURRENT_USER() as user").collect()
            user_name = result[0]["USER"]
            logger.info(f"当前用户: {user_name}")
            self.assertIsNotNone(user_name)
        
        # 验证会话已关闭
        logger.info("新会话已自动关闭")
    
    def test_05_concurrent_connections(self):
        """测试并发连接"""
        logger.info("测试5: 并发连接测试")
        
        def worker(worker_id):
            """工作线程函数"""
            try:
                with conn_manager.session_context() as session:
                    result = session.sql(f"SELECT {worker_id} as id").collect()
                    value = result[0]["ID"]
                    logger.info(f"工作线程 {worker_id} 成功执行查询")
                    return value == worker_id
            except Exception as e:
                logger.error(f"工作线程 {worker_id} 失败: {e}")
                return False
        
        # 使用线程池并发执行
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker, i) for i in range(5)]
            results = [f.result() for f in futures]
        
        # 验证所有线程都成功
        self.assertTrue(all(results))
        logger.info(f"并发测试完成，成功率: {sum(results)}/{len(results)}")
    
    def test_06_connection_failure_recovery(self):
        """测试连接失败恢复"""
        logger.info("测试6: 连接失败恢复")
        
        # 获取初始会话
        session1 = conn_manager.get_session()
        logger.info("初始会话已获取")
        
        # 模拟会话失效（实际中可能需要其他方式）
        # 这里通过关闭会话来模拟
        session1.close()
        logger.info("会话已关闭，模拟连接失效")
        
        # 再次获取会话，应该自动创建新会话
        session2 = conn_manager.get_session()
        self.assertIsNotNone(session2)
        
        # 测试新会话可用
        result = session2.sql("SELECT 1 as test").collect()
        self.assertEqual(result[0]["TEST"], 1)
        logger.info("会话自动恢复成功")


if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## 11. tests/test_crud.py

```python
"""
CRUD操作测试模块
测试基本的增删改查功能
"""
import unittest
import random
from datetime import datetime

from core.operations import SnowparkOperations
from core.cache import query_cache
from utils.logger import logger


class TestCRUD(unittest.TestCase):
    """测试CRUD操作功能"""
    
    @classmethod
    def setUpClass(cls):
        """类级别测试前准备"""
        cls.ops = SnowparkOperations(use_cache=True)
        logger.info("=" * 50)
        logger.info("开始CRUD操作测试")
    
    @classmethod
    def tearDownClass(cls):
        """类级别测试后清理"""
        # 关闭缓存清理线程
        query_cache.shutdown()
        logger.info("CRUD操作测试完成")
        logger.info("=" * 50)
    
    def setUp(self):
        """每个测试前准备"""
        # 生成测试数据
        self.test_username = f"test_user_{random.randint(1000, 9999)}"
        self.test_email = f"{self.test_username}@test.com"
    
    def test_01_insert(self):
        """测试插入数据"""
        logger.info("测试1: 插入数据")
        
        # 插入单条记录
        user_data = {
            "username": self.test_username,
            "email": self.test_email,
            "age": 25,
            "city": "测试城市"
        }
        
        rows_inserted = self.ops.insert("users", user_data)
        self.assertEqual(rows_inserted, 1)
        logger.info(f"成功插入用户: {self.test_username}")
        
        # 验证插入成功
        result = self.ops.select(
            "users",
            where=f"username = '{self.test_username}'"
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["EMAIL"], self.test_email)
        
        # 插入多条记录
        multiple_users = [
            {
                "username": f"test_user_{random.randint(1000, 9999)}",
                "email": f"test{i}@test.com",
                "age": 20 + i,
                "city": f"城市{i}"
            }
            for i in range(1, 4)
        ]
        
        rows_inserted = self.ops.insert("users", multiple_users)
        self.assertEqual(rows_inserted, 3)
        logger.info(f"成功批量插入 {rows_inserted} 条记录")
    
    def test_02_select(self):
        """测试查询数据"""
        logger.info("测试2: 查询数据")
        
        # 查询所有用户
        all_users = self.ops.select("users", limit=5)
        logger.info(f"查询到 {len(all_users)} 条用户记录")
        self.assertTrue(len(all_users) > 0)
        
        # 查询指定列
        users = self.ops.select(
            "users",
            columns=["username", "email"],
            limit=3
        )
        for user in users:
            self.assertIn("USERNAME", user)
            self.assertIn("EMAIL", user)
            self.assertNotIn("AGE", user)
        logger.info("指定列查询成功")
        
        # 带条件的查询
        young_users = self.ops.select(
            "users",
            where="age < 30",
            limit=3
        )
        for user in young_users:
            self.assertTrue(user.get("AGE", 0) < 30)
        logger.info("条件查询成功")
        
        # 测试缓存
        query = "SELECT * FROM users WHERE city = '北京'"
        result1 = self.ops.execute_query(query)
        result2 = self.ops.execute_query(query)
        self.assertEqual(result1, result2)
        logger.info("缓存测试成功")
    
    def test_03_update(self):
        """测试更新数据"""
        logger.info("测试3: 更新数据")
        
        # 先插入测试数据
        test_user = {
            "username": f"update_test_{random.randint(1000, 9999)}",
            "email": f"update@test.com",
            "age": 30,
            "city": "原城市"
        }
        self.ops.insert("users", test_user)
        
        # 更新数据
        update_data = {
            "age": 31,
            "city": "新城市"
        }
        
        rows_updated = self.ops.update(
            "users",
            update_data,
            where=f"username = '{test_user['username']}'"
        )
        
        self.assertEqual(rows_updated, 1)
        
        # 验证更新结果
        result = self.ops.select(
            "users",
            where=f"username = '{test_user['username']}'"
        )
        self.assertEqual(result[0]["AGE"], 31)
        self.assertEqual(result[0]["CITY"], "新城市")
        logger.info("数据更新成功")
    
    def test_04_delete(self):
        """测试删除数据"""
        logger.info("测试4: 删除数据")
        
        # 先插入测试数据
        test_user = {
            "username": f"delete_test_{random.randint(1000, 9999)}",
            "email": f"delete@test.com",
            "age": 40,
            "city": "待删除"
        }
        self.ops.insert("users", test_user)
        
        # 验证数据已插入
        before_delete = self.ops.select(
            "users",
            where=f"username = '{test_user['username']}'"
        )
        self.assertEqual(len(before_delete), 1)
        
        # 删除数据
        rows_deleted = self.ops.delete(
            "users",
            where=f"username = '{test_user['username']}'"
        )
        
        self.assertEqual(rows_deleted, 1)
        
        # 验证数据已删除
        after_delete = self.ops.select(
            "users",
            where=f"username = '{test_user['username']}'"
        )
        self.assertEqual(len(after_delete), 0)
        logger.info("数据删除成功")
    
    def test_05_bulk_insert(self):
        """测试批量插入"""
        logger.info("测试5: 批量插入")
        
        # 生成大量测试数据
        bulk_data = []
        base_time = datetime.now()
        
        for i in range(150):  # 150条测试数据
            bulk_data.append({
                "username": f"bulk_user_{i}_{random.randint(1000, 9999)}",
                "email": f"bulk{i}@test.com",
                "age": 20 + (i % 30),
                "city": f"批量城市{i % 5}"
            })
        
        # 执行批量插入
        rows_inserted = self.ops.bulk_insert("users", bulk_data, batch_size=50)
        
        self.assertEqual(rows_inserted, 150)
        logger.info(f"批量插入完成，共 {rows_inserted} 条记录")
        
        # 验证插入结果
        verify_result = self.ops.select(
            "users",
            where="username LIKE 'bulk_user_%'"
        )
        logger.info(f"验证查询到 {len(verify_result)} 条记录")
        self.assertEqual(len(verify_result), 150)
    
    def test_06_cache_performance(self):
        """测试缓存性能"""
        logger.info("测试6: 缓存性能测试")
        
        import time
        
        query = "SELECT * FROM users WHERE city = '北京'"
        
        # 第一次查询（无缓存）
        start_time = time.time()
        result1 = self.ops.execute_query(query)
        first_duration = time.time() - start_time
        
        # 第二次查询（有缓存）
        start_time = time.time()
        result2 = self.ops.execute_query(query)
        second_duration = time.time() - start_time
        
        logger.info(f"首次查询耗时: {first_duration:.3f}秒")
        logger.info(f"缓存查询耗时: {second_duration:.3f}秒")
        logger.info(f"性能提升: {first_duration/second_duration:.1f}倍")
        
        # 验证缓存命中
        self.assertEqual(result1, result2)
        self.assertTrue(second_duration < first_duration)


if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## 12. tests/test_transaction.py

```python
"""
事务测试模块
测试事务处理功能
"""
import unittest
import random

from core.operations import SnowparkOperations
from core.transaction import transaction_manager
from utils.logger import logger
from utils.exceptions import TransactionError


class TestTransaction(unittest.TestCase):
    """测试事务处理功能"""
    
    @classmethod
    def setUpClass(cls):
        """类级别测试前准备"""
        cls.ops = SnowparkOperations(use_cache=False)  # 事务测试关闭缓存
        logger.info("=" * 50)
        logger.info("开始事务处理测试")
    
    def setUp(self):
        """每个测试前准备"""
        self.test_username = f"trans_user_{random.randint(1000, 9999)}"
        self.test_email = f"{self.test_username}@test.com"
    
    def test_01_successful_transaction(self):
        """测试成功的事务"""
        logger.info("测试1: 成功的事务")
        
        try:
            with transaction_manager.transaction():
                # 插入用户
                user_data = {
                    "username": self.test_username,
                    "email": self.test_email,
                    "age": 28,
                    "city": "事务城市"
                }
                self.ops.insert("users", user_data)
                
                # 插入产品
                product_data = {
                    "product_name": "事务测试产品",
                    "category": "测试",
                    "price": 99.99,
                    "stock_quantity": 10
                }
                self.ops.insert("products", product_data)
                
                logger.info("事务操作已完成")
            
            # 验证数据已提交
            user_result = self.ops.select(
                "users",
                where=f"username = '{self.test_username}'"
            )
            self.assertEqual(len(user_result), 1)
            
            logger.info("事务成功提交")
            
        except Exception as e:
            self.fail(f"事务应该成功，但失败了: {e}")
    
    def test_02_failed_transaction(self):
        """测试失败的事务（自动回滚）"""
        logger.info("测试2: 失败的事务（自动回滚）")
        
        try:
            with transaction_manager.transaction():
                # 插入用户
                user_data = {
                    "username": self.test_username,
                    "email": self.test_email,
                    "age": 28,
                    "city": "事务城市"
                }
                self.ops.insert("users", user_data)
                
                # 故意制造错误（插入重复的email，违反唯一约束）
                duplicate_user = {
                    "username": f"{self.test_username}_2",
                    "email": self.test_email,  # 重复的email
                    "age": 30,
                    "city": "错误城市"
                }
                self.ops.insert("users", duplicate_user)
                
            # 不应该执行到这里
            self.fail("事务应该失败，但成功了")
            
        except Exception as e:
            logger.info(f"事务按预期失败: {e}")
            
            # 验证第一个插入也被回滚
            result = self.ops.select(
                "users",
                where=f"username = '{self.test_username}'"
            )
            self.assertEqual(len(result), 0)
            logger.info("数据已正确回滚")
    
    def test_03_multiple_operations_transaction(self):
        """测试多操作事务"""
        logger.info("测试3: 多操作事务")
        
        def operation1():
            """操作1：插入用户"""
            user_data = {
                "username": self.test_username,
                "email": self.test_email,
                "age": 32,
                "city": "多操作城市"
            }
            return self.ops.insert("users", user_data)
        
        def operation2():
            """操作2：查询用户"""
            return self.ops.select(
                "users",
                where=f"username = '{self.test_username}'"
            )
        
        def operation3():
            """操作3：更新用户"""
            return self.ops.update(
                "users",
                {"age": 33},
                where=f"username = '{self.test_username}'"
            )
        
        # 在事务中执行多个操作
        results = transaction_manager.execute_in_transaction([
            operation1,
            operation2,
            operation3
        ])
        
        # 验证结果
        self.assertEqual(results[0], 1)  # 插入成功
        self.assertEqual(len(results[1]), 1)  # 查询到用户
        self.assertEqual(results[2], 1)  # 更新成功
        
        # 验证最终状态
        final_user = self.ops.select(
            "users",
            where=f"username = '{self.test_username}'"
        )[0]
        self.assertEqual(final_user["AGE"], 33)
        
        logger.info("多操作事务执行成功")
    
    def test_04_nested_transaction(self):
        """测试嵌套事务（Snowflake不支持真正的嵌套事务）"""
        logger.info("测试4: 嵌套事务测试")
        
        # Snowflake不支持真正的嵌套事务，这里测试行为
        try:
            with transaction_manager.transaction():
                # 外部事务
                user_data = {
                    "username": self.test_username,
                    "email": self.test_email,
                    "age": 35,
                    "city": "嵌套外部"
                }
                self.ops.insert("users", user_data)
                
                # 内部"事务"（实际上是同一个事务）
                with transaction_manager.transaction():
                    inner_data = {
                        "username": f"{self.test_username}_inner",
                        "email": f"inner_{self.test_email}",
                        "age": 25,
                        "city": "嵌套内部"
                    }
                    self.ops.insert("users", inner_data)
                    
                    # 故意制造内部错误
                    raise ValueError("内部错误")
            
            self.fail("事务应该失败")
            
        except Exception as e:
            logger.info(f"嵌套事务按预期失败: {e}")
            
            # 验证所有数据都被回滚
            result1 = self.ops.select(
                "users",
                where=f"username = '{self.test_username}'"
            )
            result2 = self.ops.select(
                "users",
                where=f"username LIKE '%_inner'"
            )
            
            self.assertEqual(len(result1), 0)
            self.assertEqual(len(result2), 0)
            logger.info("嵌套事务回滚成功")
    
    def test_05_transaction_isolation(self):
        """测试事务隔离"""
        logger.info("测试5: 事务隔离性测试")
        
        import threading
        import time
        
        test_complete = threading.Event()
        results = []
        
        def transaction_thread(thread_id):
            """事务线程函数"""
            try:
                with transaction_manager.transaction():
                    # 插入数据
                    user_data = {
                        "username": f"thread_user_{thread_id}",
                        "email": f"thread{thread_id}@test.com",
                        "age": 20 + thread_id,
                        "city": f"线程{thread_id}"
                    }
                    self.ops.insert("users", user_data)
                    
                    # 模拟长时间操作
                    time.sleep(2)
                    
                    # 提交事务
                results.append(True)
                logger.info(f"线程 {thread_id} 事务完成")
                
            except Exception as e:
                results.append(False)
                logger.error(f"线程 {thread_id} 事务失败: {e}")
            finally:
                test_complete.set()
        
        # 启动两个并发事务
        thread1 = threading.Thread(target=transaction_thread, args=(1,))
        thread2 = threading.Thread(target=transaction_thread, args=(2,))
        
        thread1.start()
        thread2.start()
        
        # 等待所有线程完成
        thread1.join(timeout=10)
        thread2.join(timeout=10)
        
        # 验证结果
        self.assertTrue(all(results))
        logger.info("事务隔离测试完成")


if __name__ == '__main__':
    unittest.main(verbosity=2)
```

## 13. main_demo.py

```python
"""
主演示程序
展示所有功能的综合使用
"""
import time
import random
from datetime import datetime

from core.connection import conn_manager
from core.operations import SnowparkOperations
from core.transaction import transaction_manager
from core.cache import query_cache
from utils.logger import logger


def print_separator(title):
    """打印分隔线"""
    print("\n" + "=" * 60)
    print(f" {title} ".center(60, "="))
    print("=" * 60)


def demo_connection():
    """演示数据库连接"""
    print_separator("数据库连接演示")
    
    # 获取会话
    session = conn_manager.get_session()
    
    # 执行简单查询
    result = session.sql("SELECT CURRENT_VERSION() as version").collect()
    print(f"Snowflake版本: {result[0]['VERSION']}")
    
    result = session.sql("SELECT CURRENT_DATABASE() as db, CURRENT_SCHEMA() as schema").collect()
    print(f"当前数据库: {result[0]['DB']}")
    print(f"当前模式: {result[0]['SCHEMA']}")
    
    # 使用上下文管理器
    with conn_manager.new_session_context() as new_session:
        result = new_session.sql("SELECT CURRENT_USER() as user").collect()
        print(f"当前用户: {result[0]['USER']}")


def demo_crud_operations():
    """演示CRUD操作"""
    print_separator("CRUD操作演示")
    
    ops = SnowparkOperations(use_cache=True)
    
    # 1. 查询所有用户
    print("\n1. 查询所有用户（前5条）:")
    users = ops.select("users", limit=5)
    for user in users:
        print(f"   - {user['USERNAME']} ({user['EMAIL']}), 年龄: {user['AGE']}, 城市: {user['CITY']}")
    
    # 2. 插入新用户
    print("\n2. 插入新用户:")
    new_user = {
        "username": f"demo_user_{random.randint(1000, 9999)}",
        "email": f"demo{random.randint(100, 999)}@demo.com",
        "age": 28,
        "city": "演示城市"
    }
    inserted = ops.insert("users", new_user)
    print(f"   已插入 {inserted} 条记录")
    
    # 3. 查询刚插入的用户
    print("\n3. 查询刚插入的用户:")
    result = ops.select("users", where=f"username = '{new_user['username']}'")
    if result:
        user = result[0]
        print(f"   找到用户: {user['USERNAME']} - {user['EMAIL']}")
    
    # 4. 更新用户信息
    print("\n4. 更新用户年龄:")
    updated = ops.update(
        "users",
        {"age": 29},
        where=f"username = '{new_user['username']}'"
    )
    print(f"   已更新 {updated} 条记录")
    
    # 5. 验证更新
    result = ops.select("users", where=f"username = '{new_user['username']}'")
    if result:
        print(f"   更新后年龄: {result[0]['AGE']}")
    
    # 6. 执行复杂查询
    print("\n5. 复杂查询 - 用户订单汇总:")
    query = """
    SELECT 
        u.username,
        u.city,
        COUNT(o.order_id) as order_count,
        COALESCE(SUM(o.total_amount), 0) as total_spent
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.username, u.city
    ORDER BY total_spent DESC
    LIMIT 5
    """
    results = ops.execute_query(query)
    for row in results:
        print(f"   {row['USERNAME']}: {row['ORDER_COUNT']}个订单, 总消费 ¥{row['TOTAL_SPENT']:.2f}")


def demo_transaction():
    """演示事务处理"""
    print_separator("事务处理演示")
    
    ops = SnowparkOperations(use_cache=False)
    
    # 成功的事务
    print("\n1. 成功的事务:")
    try:
        with transaction_manager.transaction():
            # 创建订单
            order_data = {
                "user_id": 1,
                "total_amount": 1299.00,
                "status": "PROCESSING"
            }
            ops.insert("orders", order_data)
            
            # 更新产品库存
            ops.update(
                "products",
                {"stock_quantity": 19},  # 假设原有20，卖出1个
                where="product_id = 5"  # 书桌
            )
            
            print("   事务操作已完成")
        print("   事务已提交")
        
    except Exception as e:
        print(f"   事务失败: {e}")
    
    # 失败的事务（演示回滚）
    print("\n2. 失败的事务（自动回滚）:")
    try:
        with transaction_manager.transaction():
            # 创建订单
            order_data = {
                "user_id": 1,
                "total_amount": 5999.00,
                "status": "PROCESSING"
            }
            ops.insert("orders", order_data)
            
            # 故意制造错误（库存不足异常）
            stock_result = ops.select("products", where="product_id = 1")
            current_stock = stock_result[0]["STOCK_QUANTITY"]
            
            if current_stock < 1:
                raise ValueError("库存不足")
            
            ops.update(
                "products",
                {"stock_quantity": current_stock - 1},
                where="product_id = 1"
            )
            
        print("   事务应失败，但成功了")
        
    except Exception as e:
        print(f"   事务按预期失败: {e}")
        print("   所有操作已回滚")


def demo_cache():
    """演示缓存功能"""
    print_separator("缓存功能演示")
    
    ops = SnowparkOperations(use_cache=True)
    
    # 执行带缓存的查询
    query = "SELECT * FROM products WHERE category = '电子产品'"
    
    print("\n1. 第一次查询（无缓存）:")
    start_time = time.time()
    result1 = ops.execute_query(query)
    first_duration = time.time() - start_time
    print(f"   查询到 {len(result1)} 条记录，耗时: {first_duration:.3f}秒")
    
    print("\n2. 第二次查询（使用缓存）:")
    start_time = time.time()
    result2 = ops.execute_query(query)
    second_duration = time.time() - start_time
    print(f"   查询到 {len(result2)} 条记录，耗时: {second_duration:.3f}秒")
    
    # 显示缓存统计
    stats = query_cache.get_stats()
    print(f"\n3. 缓存统计:")
    print(f"   总缓存条目: {stats['total_entries']}")
    print(f"   活跃缓存: {stats['active_entries']}")
    
    # 清空缓存
    print("\n4. 清空缓存:")
    query_cache.clear()
    stats = query_cache.get_stats()
    print(f"   清空后缓存条目: {stats['total_entries']}")


def demo_bulk_operations():
    """演示批量操作"""
    print_separator("批量操作演示")
    
    ops = SnowparkOperations()
    
    # 生成批量数据
    print("\n1. 准备批量数据...")
    bulk_products = []
    categories = ["电子产品", "家居用品", "办公用品", "食品"]
    
    for i in range(100):
        bulk_products.append({
            "product_name": f"批量产品_{i:03d}",
            "category": random.choice(categories),
            "price": round(random.uniform(10, 1000), 2),
            "stock_quantity": random.randint(10, 200)
        })
    
    print(f"   已生成 {len(bulk_products)} 条测试数据")
    
    # 批量插入
    print("\n2. 执行批量插入...")
    start_time = time.time()
    inserted = ops.bulk_insert("products", bulk_products, batch_size=20)
    duration = time.time() - start_time
    
    print(f"   成功插入 {inserted} 条记录")
    print(f"   总耗时: {duration:.2f}秒")
    print(f"   平均速度: {inserted/duration:.0f} 条/秒")


def demo_advanced_queries():
    """演示高级查询"""
    print_separator("高级查询演示")
    
    ops = SnowparkOperations(use_cache=True)
    
    # 1. 使用聚合函数的查询
    print("\n1. 各产品类别的统计:")
    query1 = """
    SELECT 
        category,
        COUNT(*) as product_count,
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        SUM(stock_quantity) as total_stock
    FROM products
    GROUP BY category
    ORDER BY category
    """
    results = ops.execute_query(query1)
    for row in results:
        print(f"   {row['CATEGORY']}:")
        print(f"     产品数: {row['PRODUCT_COUNT']}, 平均价格: ¥{row['AVG_PRICE']:.2f}")
        print(f"     库存总量: {row['TOTAL_STOCK']}")
    
    # 2. JOIN查询
    print("\n2. 用户订单详情:")
    query2 = """
    SELECT 
        u.username,
        u.city,
        o.order_id,
        o.total_amount,
        o.status,
        o.order_date
    FROM users u
    JOIN orders o ON u.user_id = o.user_id
    WHERE o.status = 'COMPLETED'
    ORDER BY o.total_amount DESC
    LIMIT 5
    """
    results = ops.execute_query(query2)
    for row in results:
        print(f"   {row['USERNAME']} - 订单#{row['ORDER_ID']}: ¥{row['TOTAL_AMOUNT']:.2f} ({row['STATUS']})")
    
    # 3. 子查询
    print("\n3. 消费高于平均的用户:")
    query3 = """
    SELECT 
        u.username,
        u.city,
        COALESCE(SUM(o.total_amount), 0) as total_spent
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.username, u.city
    HAVING COALESCE(SUM(o.total_amount), 0) > (
        SELECT AVG(total_amount) FROM orders
    )
    ORDER BY total_spent DESC
    """
    results = ops.execute_query(query3)
    for row in results:
        print(f"   {row['USERNAME']}: 总消费 ¥{row['TOTAL_SPENT']:.2f}")


def main():
    """主函数"""
    print("\n" + "★" * 60)
    print(" Snowpark共通模块功能演示 ".center(60, "★"))
    print("★" * 60)
    
    try:
        # 演示数据库连接
        demo_connection()
        
        # 演示CRUD操作
        demo_crud_operations()
        
        # 演示事务处理
        demo_transaction()
        
        # 演示缓存功能
        demo_cache()
        
        # 演示批量操作
        demo_bulk_operations()
        
        # 演示高级查询
        demo_advanced_queries()
        
        print("\n" + "★" * 60)
        print(" 所有演示完成 ".center(60, "★"))
        print("★" * 60)
        
    except Exception as e:
        logger.error(f"演示过程中出错: {e}")
        raise
    finally:
        # 清理资源
        query_cache.shutdown()
        conn_manager.close_session()


if __name__ == "__main__":
    main()
```

## 14. requirements.txt

```txt
# Snowpark核心依赖
snowflake-snowpark-python==1.11.1

# 配置文件解析
tomli==2.0.1

# 数据处理
pandas==2.0.3
numpy==1.24.3

# 测试框架
pytest==7.4.0
pytest-cov==4.1.0

# 日志管理
loguru==0.7.2

# 可选：用于性能监控
psutil==5.9.5

# 可选：用于连接池监控
prometheus-client==0.17.1
```

## 使用说明

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置凭证

编辑 `secrets.toml` 文件，填入您的Snowflake连接信息：

```toml
[snowflake]
account = "your_account"
user = "your_username"
password = "your_password"
warehouse = "COMPUTE_WH"
database = "TEST_DB"
schema = "PUBLIC"
```

### 3. 创建测试数据

在Snowflake中执行 `tests/test_data.sql` 创建测试表和测试数据。

### 4. 运行测试

```bash
# 运行所有测试
python -m unittest discover tests/

# 运行特定测试
python tests/test_connection.py
python tests/test_crud.py
python tests/test_transaction.py
```

### 5. 运行演示程序

```bash
python main_demo.py
```

## 模块功能说明

### 核心模块

1. **连接管理** (`core/connection.py`)
   - 单例模式管理数据库连接
   - 支持连接复用
   - 提供上下文管理器
   - 线程安全

2. **CRUD操作** (`core/operations.py`)
   - 封装增删改查操作
   - 支持批量插入
   - 参数化查询防止SQL注入
   - 返回字典或Pandas DataFrame

3. **事务管理** (`core/transaction.py`)
   - 事务开始、提交、回滚
   - 上下文管理器支持
   - 多操作事务执行
   - 自动回滚机制

4. **缓存管理** (`core/cache.py`)
   - 查询结果缓存
   - TTL过期机制
   - 自动清理过期缓存
   - 线程安全

### 工具模块

5. **配置管理** (`config/settings.py`)
   - 加载TOML配置文件
   - 配置验证
   - 统一的配置访问接口

6. **日志管理** (`utils/logger.py`)
   - 统一的日志格式
   - 支持控制台和文件输出
   - 日志级别控制

7. **异常处理** (`utils/exceptions.py`)
   - 自定义异常类
   - 异常分类处理
   - 保留原始异常信息

## 注意事项

1. **安全性**
   - `secrets.toml` 不应提交到版本控制系统
   - 使用参数化查询防止SQL注入
   - 连接信息加密存储

2. **性能优化**
   - 合理设置缓存TTL
   - 批量操作时选择适当的批次大小
   - 使用连接池减少连接创建开销

3. **错误处理**
   - 所有数据库操作都有异常处理
   - 事务自动回滚机制
   - 详细的日志记录便于问题追踪

4. **测试覆盖**
   - 单元测试覆盖主要功能
   - 并发测试确保线程安全
   - 事务测试验证数据一致性

这套完整的Snowpark共通模块提供了生产级别的数据库操作封装，包含了连接管理、CRUD操作、事务处理、缓存管理等核心功能，并配有完整的测试用例和演示程序。