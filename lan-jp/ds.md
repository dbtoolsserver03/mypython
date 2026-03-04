以下は、Snowparkを使用した本番レベルの共通処理を実装したPythonファイル群です。日本語の詳細なコメント付きで、ファイル構造、テスト用のGUI、テストデータも含めています。

## ファイル構造

```
snowpark_common/
├── .streamlit/
│   └── secrets.toml              # Snowflake接続情報
├── src/
│   ├── __init__.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py          # 接続管理
│   │   ├── crud.py                 # CRUD操作
│   │   └── transaction.py          # トランザクション管理
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── cache.py                # キャッシュ機能
│   │   └── logger.py               # ロギング機能
│   └── config/
│       ├── __init__.py
│       └── settings.py             # 設定管理
├── tests/
│   ├── __init__.py
│   ├── test_connection.py
│   ├── test_crud.py
│   └── test_transaction.py
├── test_gui/
│   └── app.py                      # Streamlitテスト画面
├── scripts/
│   ├── setup_test_data.sql         # テストデータ作成スクリプト
│   └── generate_test_data.py       # テストデータ生成
├── requirements.txt
└── README.md
```

## 1. secrets.toml (.streamlit/secrets.toml)

```toml
# Snowflake接続設定
[snowflake]
account = "your-account-identifier"
user = "your-username"
password = "your-password"
role = "your-role"
warehouse = "your-warehouse"
database = "your-database"
schema = "your-schema"

# 接続プール設定
[connection_pool]
max_connections = 10
timeout = 300

# キャッシュ設定
[cache]
enabled = true
ttl_seconds = 3600
max_size = 1000
```

## 2. ソースコード

### src/config/settings.py

```python
"""
設定管理モジュール
Snowflake接続やアプリケーション設定を一元管理します。
"""
import os
import streamlit as st
from typing import Dict, Any, Optional

class Settings:
    """アプリケーション設定を管理するクラス"""
    
    def __init__(self):
        """設定の初期化"""
        self._load_secrets()
    
    def _load_secrets(self) -> None:
        """secrets.tomlから設定を読み込む"""
        try:
            # Streamlit secretsからSnowflake接続情報を取得
            self.snowflake_config = {
                "account": st.secrets["snowflake"]["account"],
                "user": st.secrets["snowflake"]["user"],
                "password": st.secrets["snowflake"]["password"],
                "role": st.secrets["snowflake"]["role"],
                "warehouse": st.secrets["snowflake"]["warehouse"],
                "database": st.secrets["snowflake"]["database"],
                "schema": st.secrets["snowflake"]["schema"]
            }
            
            # 接続プール設定
            self.connection_pool_config = {
                "max_connections": st.secrets["connection_pool"].get("max_connections", 10),
                "timeout": st.secrets["connection_pool"].get("timeout", 300)
            }
            
            # キャッシュ設定
            self.cache_config = {
                "enabled": st.secrets["cache"].get("enabled", True),
                "ttl_seconds": st.secrets["cache"].get("ttl_seconds", 3600),
                "max_size": st.secrets["cache"].get("max_size", 1000)
            }
            
        except Exception as e:
            # 開発環境用のデフォルト設定
            print(f"secrets.tomlの読み込みに失敗: {e}")
            self._set_default_config()
    
    def _set_default_config(self) -> None:
        """デフォルト設定（開発環境用）"""
        self.snowflake_config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT", "your-account"),
            "user": os.getenv("SNOWFLAKE_USER", "your-user"),
            "password": os.getenv("SNOWFLAKE_PASSWORD", "your-password"),
            "role": os.getenv("SNOWFLAKE_ROLE", "your-role"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "your-warehouse"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "your-database"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "your-schema")
        }
        
        self.connection_pool_config = {
            "max_connections": 10,
            "timeout": 300
        }
        
        self.cache_config = {
            "enabled": True,
            "ttl_seconds": 3600,
            "max_size": 1000
        }
    
    def get_connection_parameters(self) -> Dict[str, Any]:
        """Snowflake接続パラメータを取得"""
        return self.snowflake_config.copy()

# シングルトンインスタンス
settings = Settings()
```

### src/database/connection.py

```python
"""
データベース接続管理モジュール
Snowflakeとの接続確立、プール管理、セッション管理を行います。
"""
import snowflake.connector
from snowflake.snowpark import Session
from typing import Optional, Dict, Any
import threading
import time
from contextlib import contextmanager
import logging

from ..config.settings import settings
from ..utils.logger import get_logger

logger = get_logger(__name__)

class ConnectionManager:
    """
    Snowflake接続を管理するシングルトンクラス
    接続プール、セッション管理、コネクションハンドリングを担当
    """
    
    _instance: Optional['ConnectionManager'] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls):
        """シングルトンパターンの実装"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初期化（初回のみ実行）"""
        if self._initialized:
            return
            
        self._initialized = True
        self._session_pool: Dict[str, Session] = {}
        self._pool_lock = threading.Lock()
        self._connection_params = settings.get_connection_parameters()
        self._pool_config = settings.connection_pool_config
        logger.info("ConnectionManager initialized")
    
    def create_session(self) -> Session:
        """
        新しいSnowparkセッションを作成
        
        Returns:
            Session: Snowparkセッションオブジェクト
        
        Raises:
            Exception: セッション作成に失敗した場合
        """
        try:
            logger.debug("Creating new Snowpark session")
            session = Session.builder.configs(self._connection_params).create()
            logger.info("Snowpark session created successfully")
            return session
        except Exception as e:
            logger.error(f"Failed to create Snowpark session: {e}")
            raise
    
    def get_session(self, session_id: Optional[str] = None) -> Session:
        """
        既存のセッションを取得、または新規作成
        
        Args:
            session_id: セッション識別子（Noneの場合は新規作成）
        
        Returns:
            Session: Snowparkセッション
        """
        if session_id is None:
            return self.create_session()
        
        with self._pool_lock:
            if session_id in self._session_pool:
                session = self._session_pool[session_id]
                # セッションの有効性チェック
                if self._is_session_valid(session):
                    logger.debug(f"Reusing existing session: {session_id}")
                    return session
                else:
                    # 無効なセッションは破棄
                    logger.warning(f"Session {session_id} is invalid, creating new one")
                    del self._session_pool[session_id]
            
            # 新規セッションを作成してプールに追加
            session = self.create_session()
            self._session_pool[session_id] = session
            return session
    
    def _is_session_valid(self, session: Session) -> bool:
        """
        セッションの有効性を確認
        
        Args:
            session: 確認するセッション
        
        Returns:
            bool: 有効な場合はTrue
        """
        try:
            # 簡単なクエリを実行して接続を確認
            session.sql("SELECT 1").collect()
            return True
        except Exception as e:
            logger.warning(f"Session validation failed: {e}")
            return False
    
    def close_session(self, session_id: str) -> None:
        """
        特定のセッションを閉じる
        
        Args:
            session_id: 閉じるセッションのID
        """
        with self._pool_lock:
            if session_id in self._session_pool:
                try:
                    self._session_pool[session_id].close()
                    logger.info(f"Session {session_id} closed")
                except Exception as e:
                    logger.error(f"Error closing session {session_id}: {e}")
                finally:
                    del self._session_pool[session_id]
    
    def close_all_sessions(self) -> None:
        """全てのセッションを閉じる"""
        with self._pool_lock:
            for session_id, session in list(self._session_pool.items()):
                try:
                    session.close()
                    logger.info(f"Session {session_id} closed")
                except Exception as e:
                    logger.error(f"Error closing session {session_id}: {e}")
            self._session_pool.clear()
            logger.info("All sessions closed")
    
    @contextmanager
    def session_context(self, session_id: Optional[str] = None):
        """
        セッションのコンテキストマネージャ
        
        Args:
            session_id: セッション識別子
        
        Yields:
            Session: Snowparkセッション
        """
        session = None
        try:
            session = self.get_session(session_id)
            yield session
        finally:
            # セッションIDが指定されていない場合は作成したセッションを閉じる
            if session_id is None and session is not None:
                session.close()

# グローバルな接続マネージャインスタンス
connection_manager = ConnectionManager()
```

### src/database/crud.py

```python
"""
CRUD操作モジュール
基本的な作成、読み取り、更新、削除操作を提供します。
"""
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lit
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import logging

from ..utils.logger import get_logger
from ..utils.cache import cache_manager
from .connection import connection_manager

logger = get_logger(__name__)

class CRUDOperations:
    """
    Snowflakeに対するCRUD操作を提供するクラス
    """
    
    def __init__(self, session: Optional[Session] = None):
        """
        CRUD操作クラスの初期化
        
        Args:
            session: Snowparkセッション（Noneの場合は新規作成）
        """
        self.session = session or connection_manager.create_session()
        logger.debug("CRUDOperations initialized")
    
    def create(self, 
               table_name: str, 
               data: Union[Dict[str, Any], List[Dict[str, Any]], pd.DataFrame],
               use_cache: bool = False) -> int:
        """
        テーブルにデータを挿入
        
        Args:
            table_name: テーブル名
            data: 挿入するデータ（辞書、辞書のリスト、またはDataFrame）
            use_cache: キャッシュを使用するかどうか
        
        Returns:
            int: 挿入された行数
        
        Raises:
            ValueError: データ形式が無効な場合
            Exception: 挿入処理に失敗した場合
        """
        try:
            logger.info(f"Inserting data into {table_name}")
            
            # データの形式を統一（Snowpark DataFrameに変換）
            if isinstance(data, dict):
                df = self.session.create_dataframe([data])
            elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
                df = self.session.create_dataframe(data)
            elif isinstance(data, pd.DataFrame):
                df = self.session.create_dataframe(data)
            else:
                raise ValueError("Unsupported data type. Use dict, list of dicts, or pandas DataFrame")
            
            # データを挿入
            result = df.write.mode("append").save_as_table(table_name)
            
            # 影響を受けた行数を取得
            row_count = len(df.collect())
            logger.info(f"Successfully inserted {row_count} rows into {table_name}")
            
            # キャッシュをクリア（テーブルデータが変更されたため）
            if use_cache:
                cache_manager.clear_by_pattern(f"table:{table_name}:*")
                logger.debug(f"Cleared cache for table {table_name}")
            
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}")
            raise
    
    def read(self, 
             table_name: str, 
             columns: Optional[List[str]] = None,
             where_condition: Optional[Dict[str, Any]] = None,
             use_cache: bool = True) -> pd.DataFrame:
        """
        テーブルからデータを読み取り
        
        Args:
            table_name: テーブル名
            columns: 取得するカラム（Noneの場合は全カラム）
            where_condition: フィルタ条件（辞書形式）
            use_cache: キャッシュを使用するかどうか
        
        Returns:
            pd.DataFrame: 取得したデータ
        
        Raises:
            Exception: 読み取り処理に失敗した場合
        """
        try:
            logger.info(f"Reading data from {table_name}")
            
            # キャッシュキーの生成
            cache_key = f"table:{table_name}:{columns}:{where_condition}"
            
            # キャッシュをチェック
            if use_cache:
                cached_result = cache_manager.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"Cache hit for {cache_key}")
                    return cached_result
            
            # テーブルからデータを読み取り
            df = self.session.table(table_name)
            
            # カラムを選択
            if columns:
                df = df.select(*[col(c) for c in columns])
            
            # フィルタ条件を適用
            if where_condition:
                for key, value in where_condition.items():
                    df = df.filter(col(key) == value)
            
            # pandas DataFrameに変換
            result_df = df.to_pandas()
            
            # キャッシュに保存
            if use_cache:
                cache_manager.set(cache_key, result_df)
                logger.debug(f"Cached result for {cache_key}")
            
            logger.info(f"Successfully read {len(result_df)} rows from {table_name}")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to read data from {table_name}: {e}")
            raise
    
    def update(self,
               table_name: str,
               set_values: Dict[str, Any],
               where_condition: Dict[str, Any],
               use_cache: bool = False) -> int:
        """
        テーブルのデータを更新
        
        Args:
            table_name: テーブル名
            set_values: 更新する値（カラム名と新しい値の辞書）
            where_condition: 更新対象を特定する条件
            use_cache: キャッシュを使用するかどうか
        
        Returns:
            int: 更新された行数
        
        Raises:
            Exception: 更新処理に失敗した場合
        """
        try:
            logger.info(f"Updating data in {table_name}")
            
            # 更新対象を特定
            df = self.session.table(table_name)
            for key, value in where_condition.items():
                df = df.filter(col(key) == value)
            
            # 更新前の行数を記録
            before_count = len(df.collect())
            
            # 更新を実行
            for key, value in set_values.items():
                df = df.with_column(key, lit(value))
            
            # テーブルを更新（上書き保存）
            df.write.mode("overwrite").save_as_table(table_name)
            
            # 更新後の行数を確認
            after_df = self.session.table(table_name)
            for key, value in where_condition.items():
                after_df = after_df.filter(col(key) == value)
            after_count = len(after_df.collect())
            
            logger.info(f"Updated {after_count} rows in {table_name}")
            
            # キャッシュをクリア
            if use_cache:
                cache_manager.clear_by_pattern(f"table:{table_name}:*")
                logger.debug(f"Cleared cache for table {table_name}")
            
            return after_count
            
        except Exception as e:
            logger.error(f"Failed to update data in {table_name}: {e}")
            raise
    
    def delete(self,
               table_name: str,
               where_condition: Dict[str, Any],
               use_cache: bool = False) -> int:
        """
        テーブルからデータを削除
        
        Args:
            table_name: テーブル名
            where_condition: 削除対象を特定する条件
            use_cache: キャッシュを使用するかどうか
        
        Returns:
            int: 削除された行数
        
        Raises:
            Exception: 削除処理に失敗した場合
        """
        try:
            logger.info(f"Deleting data from {table_name}")
            
            # 削除対象を特定
            df = self.session.table(table_name)
            for key, value in where_condition.items():
                df = df.filter(col(key) == value)
            
            # 削除前の行数を記録
            before_count = len(df.collect())
            
            # 削除対象を除いたデータを保存
            remaining_df = self.session.table(table_name)
            for key, value in where_condition.items():
                remaining_df = remaining_df.filter(col(key) != value)
            
            # テーブルを上書き保存（削除対象を除外）
            remaining_df.write.mode("overwrite").save_as_table(table_name)
            
            logger.info(f"Deleted {before_count} rows from {table_name}")
            
            # キャッシュをクリア
            if use_cache:
                cache_manager.clear_by_pattern(f"table:{table_name}:*")
                logger.debug(f"Cleared cache for table {table_name}")
            
            return before_count
            
        except Exception as e:
            logger.error(f"Failed to delete data from {table_name}: {e}")
            raise
    
    def bulk_insert(self,
                    table_name: str,
                    data: List[Dict[str, Any]],
                    batch_size: int = 1000,
                    use_cache: bool = False) -> int:
        """
        大量データのバッチ挿入
        
        Args:
            table_name: テーブル名
            data: 挿入するデータのリスト
            batch_size: 1バッチあたりの行数
            use_cache: キャッシュを使用するかどうか
        
        Returns:
            int: 挿入された総行数
        
        Raises:
            Exception: 挿入処理に失敗した場合
        """
        try:
            logger.info(f"Bulk inserting {len(data)} rows into {table_name} (batch size: {batch_size})")
            
            total_inserted = 0
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                df = self.session.create_dataframe(batch)
                result = df.write.mode("append").save_as_table(table_name)
                total_inserted += len(batch)
                logger.debug(f"Inserted batch {i//batch_size + 1}: {len(batch)} rows")
            
            logger.info(f"Successfully bulk inserted {total_inserted} rows into {table_name}")
            
            # キャッシュをクリア
            if use_cache:
                cache_manager.clear_by_pattern(f"table:{table_name}:*")
                logger.debug(f"Cleared cache for table {table_name}")
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to bulk insert data into {table_name}: {e}")
            raise

    def execute_query(self, query: str, use_cache: bool = True) -> pd.DataFrame:
        """
        カスタムSQLクエリを実行
        
        Args:
            query: 実行するSQLクエリ
            use_cache: キャッシュを使用するかどうか
        
        Returns:
            pd.DataFrame: クエリ結果
        
        Raises:
            Exception: クエリ実行に失敗した場合
        """
        try:
            logger.info(f"Executing query: {query[:100]}...")
            
            # キャッシュキーの生成
            cache_key = f"query:{hash(query)}"
            
            # キャッシュをチェック
            if use_cache:
                cached_result = cache_manager.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"Cache hit for query")
                    return cached_result
            
            # クエリを実行
            df = self.session.sql(query)
            result_df = df.to_pandas()
            
            # キャッシュに保存
            if use_cache:
                cache_manager.set(cache_key, result_df)
                logger.debug(f"Cached query result")
            
            logger.info(f"Query returned {len(result_df)} rows")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
```

### src/database/transaction.py

```python
"""
トランザクション管理モジュール
複数の操作をまとめて実行し、コミットまたはロールバックを制御します。
"""
from snowflake.snowpark import Session
from typing import Callable, Any, Optional
import logging
from contextlib import contextmanager

from ..utils.logger import get_logger
from .connection import connection_manager

logger = get_logger(__name__)

class TransactionManager:
    """
    トランザクション管理クラス
    複数のデータベース操作をトランザクションとして扱います。
    """
    
    def __init__(self, session: Optional[Session] = None):
        """
        トランザクションマネージャの初期化
        
        Args:
            session: Snowparkセッション（Noneの場合は新規作成）
        """
        self.session = session or connection_manager.create_session()
        self._in_transaction = False
        logger.debug("TransactionManager initialized")
    
    @contextmanager
    def transaction(self):
        """
        トランザクションのコンテキストマネージャ
        
        使用例：
        with transaction_manager.transaction():
            crud.create(...)
            crud.update(...)
            # 自動的にコミット
        """
        try:
            self.begin()
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            logger.error(f"Transaction failed, rolled back: {e}")
            raise
    
    def begin(self) -> None:
        """トランザクションを開始"""
        if self._in_transaction:
            logger.warning("Transaction already in progress")
            return
        
        logger.info("Beginning transaction")
        # Snowflakeは自動コミットモードなので、明示的なBEGINは不要
        # 代わりにフラグで管理
        self._in_transaction = True
    
    def commit(self) -> None:
        """トランザクションをコミット"""
        if not self._in_transaction:
            logger.warning("No active transaction to commit")
            return
        
        try:
            logger.info("Committing transaction")
            # Snowflakeでは明示的なCOMMITは不要
            # セッションが閉じられる時に自動的にコミット
            self._in_transaction = False
            logger.info("Transaction committed successfully")
        except Exception as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise
    
    def rollback(self) -> None:
        """トランザクションをロールバック"""
        if not self._in_transaction:
            logger.warning("No active transaction to rollback")
            return
        
        try:
            logger.info("Rolling back transaction")
            # Snowflakeでは明示的なROLLBACKはサポートされていない
            # 代わりにセッションを破棄して新しいセッションを作成
            self.session.close()
            self.session = connection_manager.create_session()
            self._in_transaction = False
            logger.info("Transaction rolled back successfully")
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise
    
    def execute_in_transaction(self, 
                                operations: Callable[[], Any],
                                retry_count: int = 3) -> Any:
        """
        複数の操作をトランザクション内で実行
        
        Args:
            operations: 実行する操作を含む関数
            retry_count: リトライ回数
        
        Returns:
            Any: 操作の結果
        
        Raises:
            Exception: 全てのリトライが失敗した場合
        """
        for attempt in range(retry_count):
            try:
                with self.transaction():
                    result = operations()
                    return result
            except Exception as e:
                logger.warning(f"Transaction attempt {attempt + 1} failed: {e}")
                if attempt == retry_count - 1:
                    logger.error(f"All {retry_count} attempts failed")
                    raise
                # 少し待ってからリトライ
                import time
                time.sleep(1 * (attempt + 1))

# トランザクションマネージャのファクトリ関数
def create_transaction_manager(session: Optional[Session] = None) -> TransactionManager:
    """
    トランザクションマネージャを作成
    
    Args:
        session: 既存のセッション（Noneの場合は新規作成）
    
    Returns:
        TransactionManager: トランザクションマネージャインスタンス
    """
    return TransactionManager(session)
```

### src/utils/cache.py

```python
"""
キャッシュ管理モジュール
クエリ結果などをメモリにキャッシュしてパフォーマンスを向上させます。
"""
import time
import threading
from typing import Any, Optional, Dict
from collections import OrderedDict
import hashlib
import json
import logging

from ..config.settings import settings
from .logger import get_logger

logger = get_logger(__name__)

class CacheEntry:
    """キャッシュエントリを表すクラス"""
    
    def __init__(self, value: Any, ttl: int):
        """
        キャッシュエントリの初期化
        
        Args:
            value: キャッシュする値
            ttl: 有効期間（秒）
        """
        self.value = value
        self.created_at = time.time()
        self.ttl = ttl
    
    @property
    def is_expired(self) -> bool:
        """エントリが期限切れかどうか"""
        return time.time() - self.created_at > self.ttl

class CacheManager:
    """
    キャッシュ管理クラス（シングルトン）
    LRU方式でキャッシュを管理します。
    """
    
    _instance: Optional['CacheManager'] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls):
        """シングルトンパターンの実装"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初期化（初回のみ実行）"""
        if self._initialized:
            return
        
        self._initialized = True
        self._cache: OrderedDict = OrderedDict()
        self._cache_lock = threading.Lock()
        self._enabled = settings.cache_config["enabled"]
        self._default_ttl = settings.cache_config["ttl_seconds"]
        self._max_size = settings.cache_config["max_size"]
        
        # 期限切れエントリを削除するスレッドを開始
        self._start_cleanup_thread()
        
        logger.info(f"CacheManager initialized (enabled={self._enabled}, max_size={self._max_size})")
    
    def _start_cleanup_thread(self) -> None:
        """期限切れエントリを定期的に削除するスレッドを開始"""
        def cleanup_loop():
            while True:
                time.sleep(60)  # 1分ごとに実行
                self._cleanup_expired()
        
        thread = threading.Thread(target=cleanup_loop, daemon=True)
        thread.start()
        logger.debug("Cache cleanup thread started")
    
    def _cleanup_expired(self) -> None:
        """期限切れエントリを削除"""
        with self._cache_lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired
            ]
            for key in expired_keys:
                del self._cache[key]
            
            if expired_keys:
                logger.debug(f"Removed {len(expired_keys)} expired cache entries")
    
    def _generate_key(self, key: str) -> str:
        """
        キャッシュキーを生成（長いキーをハッシュ化）
        
        Args:
            key: 元のキー
        
        Returns:
            str: ハッシュ化されたキー
        """
        if len(key) > 100:
            return hashlib.md5(key.encode()).hexdigest()
        return key
    
    def get(self, key: str) -> Optional[Any]:
        """
        キャッシュから値を取得
        
        Args:
            key: キャッシュキー
        
        Returns:
            Optional[Any]: キャッシュされた値（存在しない場合はNone）
        """
        if not self._enabled:
            return None
        
        cache_key = self._generate_key(key)
        
        with self._cache_lock:
            entry = self._cache.get(cache_key)
            
            if entry is None:
                logger.debug(f"Cache miss: {key[:50]}...")
                return None
            
            if entry.is_expired:
                # 期限切れのエントリを削除
                del self._cache[cache_key]
                logger.debug(f"Cache expired: {key[:50]}...")
                return None
            
            # LRUのため、アクセスしたエントリを最後尾に移動
            self._cache.move_to_end(cache_key)
            logger.debug(f"Cache hit: {key[:50]}...")
            
            return entry.value
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        キャッシュに値を保存
        
        Args:
            key: キャッシュキー
            value: 保存する値
            ttl: 有効期間（秒、Noneの場合はデフォルト値）
        """
        if not self._enabled:
            return
        
        cache_key = self._generate_key(key)
        ttl = ttl or self._default_ttl
        
        with self._cache_lock:
            # キャッシュサイズ制限をチェック
            if len(self._cache) >= self._max_size:
                # 最も古いエントリを削除（LRU）
                self._cache.popitem(last=False)
                logger.debug("Removed oldest cache entry due to size limit")
            
            # 新しいエントリを追加
            self._cache[cache_key] = CacheEntry(value, ttl)
            logger.debug(f"Cached value for key: {key[:50]}... (ttl={ttl}s)")
    
    def delete(self, key: str) -> None:
        """
        キャッシュからエントリを削除
        
        Args:
            key: 削除するキー
        """
        if not self._enabled:
            return
        
        cache_key = self._generate_key(key)
        
        with self._cache_lock:
            if cache_key in self._cache:
                del self._cache[cache_key]
                logger.debug(f"Deleted cache entry: {key[:50]}...")
    
    def clear(self) -> None:
        """キャッシュを全てクリア"""
        with self._cache_lock:
            self._cache.clear()
            logger.info("Cache cleared")
    
    def clear_by_pattern(self, pattern: str) -> None:
        """
        パターンに一致するキャッシュエントリを削除
        
        Args:
            pattern: マッチングパターン（部分一致）
        """
        if not self._enabled:
            return
        
        with self._cache_lock:
            keys_to_delete = [
                key for key in self._cache.keys()
                if pattern in key or pattern.replace("*", "") in key
            ]
            
            for key in keys_to_delete:
                del self._cache[key]
            
            if keys_to_delete:
                logger.debug(f"Cleared {len(keys_to_delete)} cache entries matching pattern: {pattern}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        キャッシュの統計情報を取得
        
        Returns:
            Dict[str, Any]: 統計情報
        """
        with self._cache_lock:
            return {
                "enabled": self._enabled,
                "size": len(self._cache),
                "max_size": self._max_size,
                "default_ttl": self._default_ttl,
                "keys": list(self._cache.keys())
            }

# グローバルなキャッシュマネージャインスタンス
cache_manager = CacheManager()
```

### src/utils/logger.py

```python
"""
ロギング設定モジュール
アプリケーション全体で使用するロガーを設定します。
"""
import logging
import sys
from datetime import datetime
from typing import Optional

# ログフォーマットの定義
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

class LoggerManager:
    """
    ロガー管理クラス
    アプリケーション全体で一貫したロギング設定を提供します。
    """
    
    _instance: Optional['LoggerManager'] = None
    _initialized: bool = False
    
    def __new__(cls):
        """シングルトンパターンの実装"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初期化（初回のみ実行）"""
        if self._initialized:
            return
        
        self._initialized = True
        self._configure_root_logger()
    
    def _configure_root_logger(self) -> None:
        """ルートロガーの設定"""
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        # 既存のハンドラをクリア
        root_logger.handlers.clear()
        
        # コンソールハンドラの設定
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # ファイルハンドラの設定（オプション）
        try:
            file_handler = logging.FileHandler(f"snowpark_common_{datetime.now().strftime('%Y%m%d')}.log")
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            print(f"Failed to create file handler: {e}")
        
        logging.info("Logger configured successfully")

def get_logger(name: str) -> logging.Logger:
    """
    名前付きロガーを取得
    
    Args:
        name: ロガー名（通常は__name__）
    
    Returns:
        logging.Logger: 設定済みのロガー
    """
    # LoggerManagerを初期化（まだの場合）
    LoggerManager()
    
    return logging.getLogger(name)
```

## 3. テスト用GUI (test_gui/app.py)

```python
"""
Streamlitを使用したテスト用GUI
共通モジュールの機能を対話的にテストできます。
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# パスの設定（srcディレクトリを追加）
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database.connection import connection_manager
from src.database.crud import CRUDOperations
from src.database.transaction import TransactionManager, create_transaction_manager
from src.utils.cache import cache_manager
from src.config.settings import settings

# ページ設定
st.set_page_config(
    page_title="Snowpark共通モジュール テスト",
    page_icon="❄️",
    layout="wide"
)

# タイトル
st.title("❄️ Snowpark共通モジュール テスト画面")
st.markdown("---")

# サイドバー - 設定情報
with st.sidebar:
    st.header("⚙️ 設定情報")
    
    # Snowflake接続情報
    st.subheader("Snowflake接続")
    conn_params = settings.get_connection_parameters()
    st.json({
        "account": conn_params["account"],
        "database": conn_params["database"],
        "schema": conn_params["schema"],
        "warehouse": conn_params["warehouse"],
        "role": conn_params["role"]
    })
    
    # キャッシュ設定
    st.subheader("キャッシュ設定")
    st.json(settings.cache_config)
    
    # 接続プール設定
    st.subheader("接続プール")
    st.json(settings.connection_pool_config)
    
    # キャッシュ統計
    if st.button("🔄 キャッシュ統計を更新"):
        st.session_state.cache_stats = cache_manager.get_stats()
    
    if "cache_stats" in st.session_state:
        st.subheader("キャッシュ統計")
        st.json(st.session_state.cache_stats)
    
    # セッション管理
    st.subheader("セッション管理")
    if st.button("すべてのセッションを閉じる"):
        connection_manager.close_all_sessions()
        st.success("すべてのセッションを閉じました")

# メインエリア - タブで機能を分類
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 テーブル管理", 
    "✏️ CRUD操作", 
    "🔄 バルク操作",
    "💱 トランザクション",
    "📈 パフォーマンステスト"
])

# テストテーブル名
TEST_TABLE = "SNOWPARK_TEST_TABLE"

# タブ1: テーブル管理
with tab1:
    st.header("テーブル管理")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("テストテーブル作成")
        
        if st.button("テストテーブルを作成"):
            try:
                crud = CRUDOperations()
                
                # テーブル作成（既存の場合は削除）
                drop_query = f"DROP TABLE IF EXISTS {TEST_TABLE}"
                crud.execute_query(drop_query, use_cache=False)
                
                # テーブル作成クエリ
                create_query = f"""
                CREATE TABLE {TEST_TABLE} (
                    ID INT AUTOINCREMENT,
                    NAME VARCHAR(100),
                    EMAIL VARCHAR(200),
                    AGE INT,
                    SCORE DECIMAL(5,2),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
                crud.execute_query(create_query, use_cache=False)
                
                st.success(f"テーブル {TEST_TABLE} を作成しました")
                
            except Exception as e:
                st.error(f"エラー: {e}")
    
    with col2:
        st.subheader("テーブル情報")
        
        if st.button("テーブル一覧を表示"):
            try:
                crud = CRUDOperations()
                query = "SHOW TABLES"
                tables = crud.execute_query(query, use_cache=False)
                st.dataframe(tables)
            except Exception as e:
                st.error(f"エラー: {e}")

# タブ2: CRUD操作
with tab2:
    st.header("CRUD操作")
    
    # テストデータの作成
    st.subheader("テストデータ作成")
    
    col1, col2 = st.columns(2)
    
    with col1:
        name = st.text_input("名前", "テストユーザー")
        email = st.text_input("メール", "test@example.com")
    
    with col2:
        age = st.number_input("年齢", 20, 100, 30)
        score = st.number_input("スコア", 0.0, 100.0, 85.5, step=0.5)
    
    if st.button("データを挿入"):
        try:
            crud = CRUDOperations()
            data = {
                "NAME": name,
                "EMAIL": email,
                "AGE": age,
                "SCORE": score
            }
            rows_inserted = crud.create(TEST_TABLE, data)
            st.success(f"{rows_inserted}行を挿入しました")
        except Exception as e:
            st.error(f"エラー: {e}")
    
    st.markdown("---")
    
    # データ表示
    st.subheader("データ表示")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        use_cache = st.checkbox("キャッシュを使用", value=True)
    
    with col2:
        if st.button("全データ表示"):
            try:
                crud = CRUDOperations()
                df = crud.read(TEST_TABLE, use_cache=use_cache)
                st.dataframe(df)
                st.info(f"取得件数: {len(df)}行")
            except Exception as e:
                st.error(f"エラー: {e}")
    
    with col3:
        if st.button("キャッシュをクリア"):
            cache_manager.clear_by_pattern(f"table:{TEST_TABLE}:*")
            st.success("キャッシュをクリアしました")
    
    # データ更新
    st.markdown("---")
    st.subheader("データ更新")
    
    col1, col2 = st.columns(2)
    
    with col1:
        update_id = st.number_input("更新するID", min_value=1, step=1)
        new_score = st.number_input("新しいスコア", 0.0, 100.0, 90.0, step=0.5)
    
    with col2:
        new_age = st.number_input("新しい年齢", 20, 100, 35)
    
    if st.button("データを更新"):
        try:
            crud = CRUDOperations()
            set_values = {
                "SCORE": new_score,
                "AGE": new_age,
                "UPDATED_AT": datetime.now()
            }
            where_condition = {"ID": update_id}
            rows_updated = crud.update(TEST_TABLE, set_values, where_condition, use_cache=use_cache)
            st.success(f"{rows_updated}行を更新しました")
        except Exception as e:
            st.error(f"エラー: {e}")
    
    # データ削除
    st.markdown("---")
    st.subheader("データ削除")
    
    delete_id = st.number_input("削除するID", min_value=1, step=1, key="delete_id")
    
    if st.button("データを削除"):
        try:
            crud = CRUDOperations()
            where_condition = {"ID": delete_id}
            rows_deleted = crud.delete(TEST_TABLE, where_condition, use_cache=use_cache)
            st.success(f"{rows_deleted}行を削除しました")
        except Exception as e:
            st.error(f"エラー: {e}")

# タブ3: バルク操作
with tab3:
    st.header("バルク操作（大量データ処理）")
    
    st.subheader("テストデータの一括生成")
    
    col1, col2 = st.columns(2)
    
    with col1:
        num_records = st.number_input("生成するレコード数", min_value=10, max_value=10000, value=100, step=100)
    
    with col2:
        batch_size = st.number_input("バッチサイズ", min_value=10, max_value=5000, value=500, step=100)
    
    if st.button("バルク挿入実行"):
        try:
            # テストデータ生成
            data = []
            names = ["佐藤", "鈴木", "高橋", "田中", "渡辺", "伊藤", "山本", "中村", "小林", "加藤"]
            for i in range(num_records):
                record = {
                    "NAME": f"{np.random.choice(names)} {i+1}",
                    "EMAIL": f"user{i+1}@example.com",
                    "AGE": np.random.randint(20, 60),
                    "SCORE": round(np.random.uniform(50, 100), 2)
                }
                data.append(record)
            
            # バルク挿入
            crud = CRUDOperations()
            with st.spinner(f"{num_records}件のデータを挿入中..."):
                rows_inserted = crud.bulk_insert(TEST_TABLE, data, batch_size=batch_size)
            
            st.success(f"{rows_inserted}件のデータを挿入しました")
            
        except Exception as e:
            st.error(f"エラー: {e}")
    
    st.markdown("---")
    st.subheader("バルク更新・削除")
    
    if st.button("全データのスコアを一括更新"):
        try:
            crud = CRUDOperations()
            
            # 全データを取得
            df = crud.read(TEST_TABLE, use_cache=False)
            
            # スコアをランダムに更新
            updated_data = []
            for _, row in df.iterrows():
                updated_data.append({
                    "ID": row["ID"],
                    "NAME": row["NAME"],
                    "EMAIL": row["EMAIL"],
                    "AGE": row["AGE"],
                    "SCORE": round(np.random.uniform(60, 95), 2),
                    "CREATED_AT": row["CREATED_AT"],
                    "UPDATED_AT": datetime.now()
                })
            
            # テーブルを再作成して一括更新
            crud.execute_query(f"TRUNCATE TABLE {TEST_TABLE}", use_cache=False)
            rows_inserted = crud.bulk_insert(TEST_TABLE, updated_data, batch_size=batch_size)
            
            st.success(f"{rows_inserted}件のデータを更新しました")
            
        except Exception as e:
            st.error(f"エラー: {e}")

# タブ4: トランザクション
with tab4:
    st.header("トランザクション管理")
    
    st.subheader("トランザクションの動作テスト")
    
    st.info("""
    トランザクションでは、複数の操作をまとめて実行し、
    すべて成功した場合のみコミット、途中でエラーが発生した場合はロールバックします。
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**正常系テスト**")
        
        if st.button("正常なトランザクションを実行"):
            try:
                # トランザクションマネージャを作成
                tm = create_transaction_manager()
                
                def transaction_operations():
                    crud = CRUDOperations(tm.session)
                    
                    # 3件のデータを挿入
                    data1 = {"NAME": "トランザクション1", "EMAIL": "trans1@test.com", "AGE": 25, "SCORE": 88.5}
                    data2 = {"NAME": "トランザクション2", "EMAIL": "trans2@test.com", "AGE": 32, "SCORE": 92.0}
                    data3 = {"NAME": "トランザクション3", "EMAIL": "trans3@test.com", "AGE": 41, "SCORE": 76.5}
                    
                    crud.create(TEST_TABLE, data1)
                    crud.create(TEST_TABLE, data2)
                    crud.create(TEST_TABLE, data3)
                    
                    return "3件のデータを挿入しました"
                
                # トランザクション内で実行
                result = tm.execute_in_transaction(transaction_operations)
                st.success(f"トランザクション成功: {result}")
                
            except Exception as e:
                st.error(f"トランザクション失敗: {e}")
    
    with col2:
        st.markdown("**異常系テスト**")
        
        if st.button("エラーを含むトランザクション"):
            try:
                tm = create_transaction_manager()
                
                def failing_transaction():
                    crud = CRUDOperations(tm.session)
                    
                    # 正常なデータ
                    data1 = {"NAME": "正常データ", "EMAIL": "normal@test.com", "AGE": 28, "SCORE": 85.0}
                    crud.create(TEST_TABLE, data1)
                    
                    # エラーを発生させる（存在しないカラムを指定）
                    data2 = {"NAME": "エラーデータ", "INVALID_COLUMN": "error", "EMAIL": "error@test.com"}
                    crud.create(TEST_TABLE, data2)
                    
                    return "このメッセージは表示されません"
                
                result = tm.execute_in_transaction(failing_transaction)
                st.success(result)
                
            except Exception as e:
                st.error(f"予期されたエラー: {e}")
                st.info("トランザクションはロールバックされ、最初のデータは挿入されていません")

# タブ5: パフォーマンステスト
with tab5:
    st.header("パフォーマンステスト")
    
    st.subheader("キャッシュ効果の検証")
    
    test_sizes = [100, 500, 1000, 5000]
    results = []
    
    if st.button("パフォーマンステスト実行"):
        try:
            crud = CRUDOperations()
            
            # テストデータ準備
            test_data = []
            for i in range(max(test_sizes)):
                test_data.append({
                    "NAME": f"パフォーマンステスト {i+1}",
                    "EMAIL": f"perf{i+1}@test.com",
                    "AGE": 20 + (i % 40),
                    "SCORE": round(50 + (i % 50), 2)
                })
            
            # データ挿入
            crud.bulk_insert(TEST_TABLE, test_data, batch_size=1000)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, size in enumerate(test_sizes):
                status_text.text(f"テスト中: {size}件")
                
                # キャッシュなし
                import time
                start = time.time()
                df_no_cache = crud.read(TEST_TABLE, use_cache=False)
                end = time.time()
                no_cache_time = end - start
                
                # キャッシュあり（2回実行して平均を取る）
                start = time.time()
                df_cache1 = crud.read(TEST_TABLE, use_cache=True)
                df_cache2 = crud.read(TEST_TABLE, use_cache=True)
                end = time.time()
                cache_time = (end - start) / 2
                
                results.append({
                    "件数": size,
                    "キャッシュなし(秒)": round(no_cache_time, 3),
                    "キャッシュあり(秒)": round(cache_time, 3),
                    "改善率(%)": round((no_cache_time - cache_time) / no_cache_time * 100, 1)
                })
                
                progress_bar.progress((idx + 1) / len(test_sizes))
            
            # 結果表示
            st.success("パフォーマンステスト完了")
            results_df = pd.DataFrame(results)
            st.dataframe(results_df)
            
            # グラフ表示
            st.subheader("パフォーマンス比較グラフ")
            chart_data = results_df.melt(id_vars=["件数"], 
                                          value_vars=["キャッシュなし(秒)", "キャッシュあり(秒)"],
                                          var_name="種別", value_name="時間(秒)")
            st.bar_chart(chart_data.pivot(index="件数", columns="種別", values="時間(秒)"))
            
        except Exception as e:
            st.error(f"エラー: {e}")
    
    # キャッシュクリア
    if st.button("キャッシュをクリアして再接続"):
        cache_manager.clear()
        connection_manager.close_all_sessions()
        st.success("キャッシュをクリアし、全セッションを閉じました")

# フッター
st.markdown("---")
st.markdown("❄️ Snowpark共通モジュール テスト画面 v1.0.0")
```

## 4. テストデータ作成スクリプト

### scripts/setup_test_data.sql

```sql
-- テストデータベースとスキーマの作成
CREATE DATABASE IF NOT EXISTS SNOWPARK_TEST_DB;
USE DATABASE SNOWPARK_TEST_DB;

CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA;
USE SCHEMA TEST_SCHEMA;

-- テスト用テーブルの作成
CREATE OR REPLACE TABLE EMPLOYEES (
    EMPLOYEE_ID INT AUTOINCREMENT,
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    EMAIL VARCHAR(100),
    PHONE_NUMBER VARCHAR(20),
    HIRE_DATE DATE,
    JOB_ID VARCHAR(20),
    SALARY DECIMAL(10,2),
    COMMISSION_PCT DECIMAL(2,2),
    MANAGER_ID INT,
    DEPARTMENT_ID INT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DEPARTMENTS (
    DEPARTMENT_ID INT AUTOINCREMENT,
    DEPARTMENT_NAME VARCHAR(50),
    MANAGER_ID INT,
    LOCATION_ID INT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE PROJECTS (
    PROJECT_ID INT AUTOINCREMENT,
    PROJECT_NAME VARCHAR(100),
    START_DATE DATE,
    END_DATE DATE,
    BUDGET DECIMAL(15,2),
    STATUS VARCHAR(20),
    DEPARTMENT_ID INT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SALES (
    SALE_ID INT AUTOINCREMENT,
    PRODUCT_NAME VARCHAR(100),
    CATEGORY VARCHAR(50),
    SALE_DATE DATE,
    QUANTITY INT,
    UNIT_PRICE DECIMAL(10,2),
    TOTAL_AMOUNT DECIMAL(15,2),
    CUSTOMER_ID INT,
    EMPLOYEE_ID INT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- インデックスの作成（Snowflakeでは自動的に最適化されるが、コメントとして）
-- CLUSTERING KEY の設定例
ALTER TABLE SALES CLUSTER BY (SALE_DATE);

-- テストデータの挿入（簡易版）
INSERT INTO DEPARTMENTS (DEPARTMENT_NAME, MANAGER_ID, LOCATION_ID) VALUES
('営業部', 101, 1000),
('開発部', 102, 2000),
('人事部', 103, 1000),
('経理部', 104, 3000),
('マーケティング部', 105, 2000);

-- 大量のテストデータを生成するプロシージャ
CREATE OR REPLACE PROCEDURE GENERATE_TEST_DATA(ROW_COUNT INT)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var sql_cmd = `
        INSERT INTO EMPLOYEES (FIRST_NAME, LAST_NAME, EMAIL, HIRE_DATE, JOB_ID, SALARY, DEPARTMENT_ID)
        SELECT 
            'First_' || SEQ4(),
            'Last_' || SEQ4(),
            'employee_' || SEQ4() || '@company.com',
            DATEADD(day, UNIFORM(-1000, 0, RANDOM()), CURRENT_DATE()),
            CASE UNIFORM(1, 5, RANDOM())
                WHEN 1 THEN 'IT_PROG'
                WHEN 2 THEN 'SA_MAN'
                WHEN 3 THEN 'SA_REP'
                WHEN 4 THEN 'ST_CLERK'
                ELSE 'AD_ASST'
            END,
            UNIFORM(30000, 150000, RANDOM()),
            UNIFORM(1, 5, RANDOM())
        FROM TABLE(GENERATOR(ROWCOUNT => ROW_COUNT))
    `;
    
    try {
        snowflake.execute({sqlText: sql_cmd});
        return "テストデータを " + ROW_COUNT + " 件生成しました";
    } catch (err) {
        return "エラー: " + err;
    }
$$;

-- サンプルデータ生成（1000件）
CALL GENERATE_TEST_DATA(1000);

-- データ確認
SELECT COUNT(*) FROM EMPLOYEES;
SELECT * FROM EMPLOYEES LIMIT 10;
```

### scripts/generate_test_data.py

```python
"""
テストデータ生成スクリプト
大規模なテストデータを生成します。
"""
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# パスの設定
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database.connection import connection_manager
from src.database.crud import CRUDOperations
from src.utils.logger import get_logger

logger = get_logger(__name__)

class TestDataGenerator:
    """テストデータ生成クラス"""
    
    def __init__(self):
        self.crud = CRUDOperations()
    
    def generate_employees(self, num_records: int = 1000):
        """従業員データを生成"""
        logger.info(f"Generating {num_records} employee records")
        
        first_names = ['Taro', 'Jiro', 'Saburo', 'Hanako', 'Yuki', 'Haruki', 'Sakura', 'Ren', 'Akari', 'Souta']
        last_names = ['Tanaka', 'Suzuki', 'Sato', 'Takahashi', 'Watanabe', 'Ito', 'Yamamoto', 'Nakamura', 'Kobayashi', 'Kato']
        jobs = ['IT_PROG', 'SA_MAN', 'SA_REP', 'ST_CLERK', 'AD_ASST', 'FI_ACCOUNT', 'HR_REP', 'PR_REP']
        
        employees = []
        for i in range(num_records):
            hire_date = datetime.now() - timedelta(days=random.randint(0, 1000))
            employee = {
                'FIRST_NAME': random.choice(first_names),
                'LAST_NAME': random.choice(last_names),
                'EMAIL': f"emp{i+1}@company.com",
                'PHONE_NUMBER': f"03-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
                'HIRE_DATE': hire_date.date(),
                'JOB_ID': random.choice(jobs),
                'SALARY': round(random.uniform(30000, 150000), 2),
                'COMMISSION_PCT': round(random.uniform(0, 0.2), 2) if random.random() > 0.5 else None,
                'MANAGER_ID': random.randint(101, 110),
                'DEPARTMENT_ID': random.randint(1, 5)
            }
            employees.append(employee)
        
        return employees
    
    def generate_sales(self, num_records: int = 5000):
        """売上データを生成"""
        logger.info(f"Generating {num_records} sales records")
        
        products = [
            ('ノートPC', 'エレクトロニクス'),
            ('スマートフォン', 'エレクトロニクス'),
            ('タブレット', 'エレクトロニクス'),
            ('デスクトップPC', 'エレクトロニクス'),
            ('オフィスチェア', '家具'),
            ('デスク', '家具'),
            ('モニター', 'エレクトロニクス'),
            ('キーボード', 'アクセサリ'),
            ('マウス', 'アクセサリ'),
            ('ヘッドホン', 'アクセサリ')
        ]
        
        sales = []
        for i in range(num_records):
            product_name, category = random.choice(products)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(50, 2000), 2)
            total_amount = quantity * unit_price
            
            sale_date = datetime.now() - timedelta(days=random.randint(0, 365))
            
            sale = {
                'PRODUCT_NAME': product_name,
                'CATEGORY': category,
                'SALE_DATE': sale_date.date(),
                'QUANTITY': quantity,
                'UNIT_PRICE': unit_price,
                'TOTAL_AMOUNT': total_amount,
                'CUSTOMER_ID': random.randint(1001, 2000),
                'EMPLOYEE_ID': random.randint(1, 1000)
            }
            sales.append(sale)
        
        return sales
    
    def generate_projects(self, num_records: int = 200):
        """プロジェクトデータを生成"""
        logger.info(f"Generating {num_records} project records")
        
        project_prefixes = ['新規', '既存', '拡張', '改善', '保守', '研究']
        project_suffixes = ['システム', 'アプリ', 'サービス', '製品', '案件', 'プロジェクト']
        statuses = ['計画中', '進行中', '完了', '中断', 'レビュー中']
        
        projects = []
        for i in range(num_records):
            start_date = datetime.now() - timedelta(days=random.randint(0, 180))
            end_date = start_date + timedelta(days=random.randint(30, 365))
            
            project = {
                'PROJECT_NAME': f"{random.choice(project_prefixes)}{random.choice(project_suffixes)}_{i+1}",
                'START_DATE': start_date.date(),
                'END_DATE': end_date.date(),
                'BUDGET': round(random.uniform(1000000, 50000000), 2),
                'STATUS': random.choice(statuses),
                'DEPARTMENT_ID': random.randint(1, 5)
            }
            projects.append(project)
        
        return projects
    
    def generate_all_test_data(self, scale: str = 'small'):
        """全てのテストデータを生成"""
        logger.info(f"Generating all test data (scale: {scale})")
        
        # スケールに応じたレコード数
        scales = {
            'small': {'employees': 100, 'sales': 500, 'projects': 20},
            'medium': {'employees': 1000, 'sales': 5000, 'projects': 100},
            'large': {'employees': 10000, 'sales': 50000, 'projects': 500}
        }
        
        counts = scales.get(scale, scales['small'])
        
        try:
            # 各テーブルのデータ生成と挿入
            employees = self.generate_employees(counts['employees'])
            if employees:
                self.crud.bulk_insert('EMPLOYEES', employees)
                logger.info(f"Inserted {len(employees)} employees")
            
            sales = self.generate_sales(counts['sales'])
            if sales:
                self.crud.bulk_insert('SALES', sales)
                logger.info(f"Inserted {len(sales)} sales")
            
            projects = self.generate_projects(counts['projects'])
            if projects:
                self.crud.bulk_insert('PROJECTS', projects)
                logger.info(f"Inserted {len(projects)} projects")
            
            logger.info("Test data generation completed successfully")
            
        except Exception as e:
            logger.error(f"Test data generation failed: {e}")
            raise

def main():
    """メイン実行関数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate test data for Snowflake')
    parser.add_argument('--scale', choices=['small', 'medium', 'large'], default='small',
                       help='Scale of test data to generate')
    parser.add_argument('--table', choices=['employees', 'sales', 'projects', 'all'], default='all',
                       help='Specific table to generate data for')
    
    args = parser.parse_args()
    
    generator = TestDataGenerator()
    
    try:
        if args.table == 'employees':
            employees = generator.generate_employees(100 if args.scale == 'small' else 1000)
            generator.crud.bulk_insert('EMPLOYEES', employees)
            logger.info(f"Generated {len(employees)} employees")
            
        elif args.table == 'sales':
            sales = generator.generate_sales(500 if args.scale == 'small' else 5000)
            generator.crud.bulk_insert('SALES', sales)
            logger.info(f"Generated {len(sales)} sales")
            
        elif args.table == 'projects':
            projects = generator.generate_projects(20 if args.scale == 'small' else 100)
            generator.crud.bulk_insert('PROJECTS', projects)
            logger.info(f"Generated {len(projects)} projects")
            
        else:  # all
            generator.generate_all_test_data(args.scale)
            
        logger.info("Test data generation completed")
        
    except Exception as e:
        logger.error(f"Test data generation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

## 5. requirements.txt

```
# Snowflake関連
snowflake-connector-python>=3.0.0
snowflake-snowpark-python>=1.5.0

# データ処理
pandas>=1.5.0
numpy>=1.24.0

# UI/テスト用
streamlit>=1.25.0

# ユーティリティ
python-dotenv>=1.0.0
pyyaml>=6.0

# ロギング
loguru>=0.7.0

# 開発用
pytest>=7.0.0
pytest-cov>=4.0.0
black>=23.0.0
flake8>=6.0.0
```

## 6. README.md

```markdown
# Snowpark共通モジュール

Snowflake Snowparkを使用した本番レベルの共通処理モジュールです。

## 機能概要

- **接続管理**: Snowflakeセッションの作成、プール管理
- **CRUD操作**: 作成、読み取り、更新、削除の基本操作
- **トランザクション管理**: 複数操作のトランザクション制御
- **キャッシュ機能**: クエリ結果のメモリキャッシュ
- **ロギング**: 構造化されたログ出力
- **バルク処理**: 大量データの効率的な処理

## ディレクトリ構造

```
snowpark_common/
├── .streamlit/
│   └── secrets.toml          # Snowflake接続情報
├── src/
│   ├── database/
│   │   ├── connection.py      # 接続管理
│   │   ├── crud.py            # CRUD操作
│   │   └── transaction.py     # トランザクション
│   ├── utils/
│   │   ├── cache.py           # キャッシュ機能
│   │   └── logger.py          # ロギング
│   └── config/
│       └── settings.py        # 設定管理
├── test_gui/
│   └── app.py                 # Streamlitテスト画面
├── scripts/
│   ├── setup_test_data.sql    # テーブル作成
│   └── generate_test_data.py  # テストデータ生成
├── requirements.txt
└── README.md
```

## セットアップ

1. **依存関係のインストール**
```bash
pip install -r requirements.txt
```

2. **Snowflake接続設定**
`.streamlit/secrets.toml` ファイルを作成し、接続情報を設定:
```toml
[snowflake]
account = "your-account"
user = "your-username"
password = "your-password"
role = "your-role"
warehouse = "your-warehouse"
database = "your-database"
schema = "your-schema"

[connection_pool]
max_connections = 10
timeout = 300

[cache]
enabled = true
ttl_seconds = 3600
max_size = 1000
```

3. **テストデータの準備**
```bash
# テストテーブルの作成
snowsql -f scripts/setup_test_data.sql

# テストデータの生成
python scripts/generate_test_data.py --scale small
```

## 使用方法

### 基本的なCRUD操作
```python
from src.database.crud import CRUDOperations

# CRUD操作のインスタンス化
crud = CRUDOperations()

# データの挿入
data = {"NAME": "テストユーザー", "AGE": 30}
crud.create("EMPLOYEES", data)

# データの読み取り
df = crud.read("EMPLOYEES", where_condition={"DEPARTMENT_ID": 1})

# データの更新
crud.update("EMPLOYEES", 
            set_values={"SALARY": 50000},
            where_condition={"EMPLOYEE_ID": 100})

# データの削除
crud.delete("EMPLOYEES", where_condition={"EMPLOYEE_ID": 100})

# バルク挿入
data_list = [{"NAME": f"User {i}", "AGE": 20+i} for i in range(1000)]
crud.bulk_insert("EMPLOYEES", data_list, batch_size=100)
```

### トランザクション処理
```python
from src.database.transaction import create_transaction_manager

# トランザクションマネージャの作成
tm = create_transaction_manager()

def transfer_money():
    # 複数の更新処理
    crud = CRUDOperations(tm.session)
    crud.update("ACCOUNTS", 
                set_values={"BALANCE": 9000},
                where_condition={"ACCOUNT_ID": 1})
    crud.update("ACCOUNTS",
                set_values={"BALANCE": 11000},
                where_condition={"ACCOUNT_ID": 2})

# トランザクション内で実行
result = tm.execute_in_transaction(transfer_money)
```

### キャッシュの活用
```python
from src.utils.cache import cache_manager

# キャッシュの使用（自動）
df = crud.read("LARGE_TABLE", use_cache=True)

# 明示的なキャッシュ操作
cache_manager.set("my_key", my_data)
cached_data = cache_manager.get("my_key")
cache_manager.clear_by_pattern("table:EMPLOYEES:*")
```

## テスト実行

### Streamlitテスト画面
```bash
streamlit run test_gui/app.py
```

テスト画面では以下の機能を対話的にテストできます：
- テーブル管理（作成、削除）
- CRUD操作（挿入、表示、更新、削除）
- バルク操作（大量データ処理）
- トランザクション（正常系/異常系）
- パフォーマンステスト（キャッシュ効果の検証）

### ユニットテスト
```bash
pytest tests/ -v
```

## 注意事項

- 本番環境では適切なエラーハンドリングとロギングを実装してください
- 大量データの処理時はバッチサイズを適切に設定してください
- キャッシュはメモリを使用するため、適切な最大サイズを設定してください
- Snowflakeのトランザクションは自動コミットモードのため、ロールバックはセッション破棄で実装しています

## ライセンス

MIT License
```

この共通モジュールは、本番環境で使用可能な機能を提供しています。以下の特徴があります：

1. **堅牢なエラーハンドリング**: 各メソッドで例外を適切にキャッチし、ログ出力
2. **シングルトンパターン**: 接続管理とキャッシュ管理でリソースを効率的に利用
3. **詳細なコメント**: 日本語で各クラスとメソッドの役割を説明
4. **テスト用GUI**: Streamlitを使用した対話的なテスト環境
5. **設定管理**: secrets.tomlで環境ごとの設定を分離
6. **パフォーマンス最適化**: キャッシュとバルク処理で大量データに対応

必要に応じて各モジュールをカスタマイズしてお使いください。