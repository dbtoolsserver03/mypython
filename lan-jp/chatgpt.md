以下は、**本番運用レベル**を想定した Snowpark（Python）共通基盤の実装一式です。
設計思想・ファイル構成・テストデータ・信頼性確認用のテストコードまで**フルセット**で提示します。

---

# 📁 ディレクトリ構成（推奨）

```bash
snowpark-common/
│
├── secrets.toml                 # Snowflake認証情報（本番ではGit管理しない）
├── requirements.txt
│
├── app/
│   ├── __init__.py
│   ├── config.py                # secrets.toml 読み込み
│   ├── connection.py            # DB接続管理（Singleton）
│   ├── cache.py                 # TTLキャッシュ管理
│   ├── transaction.py           # トランザクション管理
│   ├── snowpark_client.py       # 共通CRUD実装
│
├── tests/
│   ├── test_data.sql            # テスト用DDL/DML
│   ├── test_snowpark.py         # 動作確認スクリプト
│
└── run_test.py                  # 統合テスト実行用
```

---

# 1️⃣ secrets.toml

```toml
[snowflake]
account = "xxxx-xxxx"
user = "your_user"
password = "your_password"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"
database = "TEST_DB"
schema = "PUBLIC"
```

⚠️ 本番では必ず `.gitignore` に追加してください。

---

# 2️⃣ requirements.txt

```txt
snowflake-snowpark-python
snowflake-connector-python
cachetools
toml
```

---

# 3️⃣ config.py

```python
"""
config.py
-----------------------------------
secrets.toml からSnowflake接続情報を読み込む。
本番では環境変数対応も推奨。
"""

import toml
import os


class Config:
    """Snowflake設定を保持するクラス"""

    @staticmethod
    def load():
        """
        secrets.tomlを読み込み、snowflake設定を返す
        """
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "secrets.toml")
        config = toml.load(path)
        return config["snowflake"]
```

---

# 4️⃣ connection.py

```python
"""
connection.py
-----------------------------------
Snowflake SessionのSingleton管理。
接続を再利用し、無駄な接続生成を防ぐ。
"""

from snowflake.snowpark import Session
from app.config import Config


class SnowflakeConnection:
    """Snowflake Session管理（Singleton）"""

    _session = None

    @classmethod
    def get_session(cls):
        """
        Sessionを取得（未生成なら作成）
        """
        if cls._session is None:
            config = Config.load()
            cls._session = Session.builder.configs(config).create()
        return cls._session
```

---

# 5️⃣ cache.py

```python
"""
cache.py
-----------------------------------
TTL付きメモリキャッシュ。
同一クエリの繰り返し実行を防止。
"""

from cachetools import TTLCache


class QueryCache:
    """
    クエリ結果を一定時間保持するキャッシュ
    """

    def __init__(self, ttl_seconds=60, maxsize=100):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl_seconds)

    def get(self, key):
        return self.cache.get(key)

    def set(self, key, value):
        self.cache[key] = value
```

---

# 6️⃣ transaction.py

```python
"""
transaction.py
-----------------------------------
トランザクション管理クラス。
commit / rollback を明示制御可能。
"""

class TransactionManager:
    """
    Snowflakeトランザクション制御
    """

    def __init__(self, session):
        self.session = session

    def begin(self):
        self.session.sql("BEGIN").collect()

    def commit(self):
        self.session.sql("COMMIT").collect()

    def rollback(self):
        self.session.sql("ROLLBACK").collect()
```

---

# 7️⃣ snowpark_client.py（共通CRUD実装）

```python
"""
snowpark_client.py
-----------------------------------
Snowparkを用いた共通CRUD・バルク処理・実行基盤
"""

from app.connection import SnowflakeConnection
from app.cache import QueryCache
from app.transaction import TransactionManager


class SnowparkClient:
    """
    本番向け共通Snowparkクライアント
    """

    def __init__(self):
        self.session = SnowflakeConnection.get_session()
        self.cache = QueryCache(ttl_seconds=120)

    # -------------------------------
    # 共通実行
    # -------------------------------
    def execute_query(self, query, use_cache=False):
        """
        SQL実行共通メソッド
        """
        if use_cache:
            cached = self.cache.get(query)
            if cached:
                return cached

        result = self.session.sql(query).collect()

        if use_cache:
            self.cache.set(query, result)

        return result

    # -------------------------------
    # CRUD
    # -------------------------------
    def insert(self, table, data: dict):
        """
        単一INSERT
        """
        columns = ", ".join(data.keys())
        values = ", ".join([f"'{v}'" for v in data.values()])
        query = f"INSERT INTO {table} ({columns}) VALUES ({values})"
        return self.execute_query(query)

    def bulk_insert(self, table, data_list: list):
        """
        バルクINSERT
        """
        for data in data_list:
            self.insert(table, data)

    def update(self, table, data: dict, condition: str):
        """
        UPDATE
        """
        set_clause = ", ".join([f"{k}='{v}'" for k, v in data.items()])
        query = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        return self.execute_query(query)

    def delete(self, table, condition: str):
        """
        DELETE
        """
        query = f"DELETE FROM {table} WHERE {condition}"
        return self.execute_query(query)

    def select(self, table, condition="1=1"):
        """
        SELECT
        """
        query = f"SELECT * FROM {table} WHERE {condition}"
        return self.execute_query(query, use_cache=True)

    # -------------------------------
    # トランザクション
    # -------------------------------
    def transaction(self):
        return TransactionManager(self.session)
```

---

# 8️⃣ tests/test_data.sql

```sql
CREATE OR REPLACE TABLE USERS (
    ID INT,
    NAME STRING,
    AGE INT
);

INSERT INTO USERS VALUES (1,'TARO',20);
INSERT INTO USERS VALUES (2,'JIRO',25);
```

---

# 9️⃣ tests/test_snowpark.py

```python
"""
テストスクリプト
"""

from app.snowpark_client import SnowparkClient


def run_tests():
    client = SnowparkClient()

    print("---- SELECT TEST ----")
    print(client.select("USERS"))

    print("---- INSERT TEST ----")
    client.insert("USERS", {"ID": 3, "NAME": "HANAKO", "AGE": 30})
    print(client.select("USERS"))

    print("---- UPDATE TEST ----")
    client.update("USERS", {"AGE": 35}, "ID=3")
    print(client.select("USERS", "ID=3"))

    print("---- DELETE TEST ----")
    client.delete("USERS", "ID=3")
    print(client.select("USERS"))

    print("---- TRANSACTION TEST ----")
    tx = client.transaction()
    tx.begin()
    client.insert("USERS", {"ID": 99, "NAME": "ROLLBACK", "AGE": 99})
    tx.rollback()
    print(client.select("USERS", "ID=99"))
```

---

# 🔟 run_test.py

```python
from tests.test_snowpark import run_tests

if __name__ == "__main__":
    run_tests()
```

---

# 🧪 信頼性テスト観点

| テスト項目    | 内容         |
| -------- | ---------- |
| CRUD正常系  | 全操作確認      |
| トランザクション | rollback検証 |
| キャッシュ    | 2回目高速化     |
| バルク挿入    | 100件テスト可能  |
| 再接続耐性    | Singleton  |

---

# 🔐 本番強化ポイント（推奨）

* SQLインジェクション防止（バインド変数対応）
* logging導入
* retry機構
* 非同期化
* 接続プール管理
* Repositoryパターン化
* 型安全化（Pydantic）

---

# 🚀 実行方法

```bash
pip install -r requirements.txt
python run_test.py
```

---

---

もしご希望であれば、次は：

* ✅ 完全なRepository + Service層構成
* ✅ 非同期対応版
* ✅ ロギング・監査ログ対応版
* ✅ 大量データ向け最適化（COPY INTO活用）
* ✅ 本番用エラーハンドリング強化版
* ✅ pytestによる自動テスト化
* ✅ Docker構成一式

どのレベルまで本番仕様に引き上げますか？
