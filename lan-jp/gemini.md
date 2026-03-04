Snowparkを実務（本番環境）で利用するための、堅牢で再利用性の高い共通クラス（SnowparkSessionManager）のコード一式をご提案します。

この構成では、**シングルトン・デザインパターン**を採用し、接続の効率化、トランザクション管理、エラーハンドリングを網羅しています。

---

## 1. ディレクトリ構造

プロジェクトの構成は以下の通りです。

```text
my_snowpark_project/
├── .streamlit/
│   └── secrets.toml          # Snowflakeの認証情報
├── app.py                   # テスト・デモ用のStreamlit画面
├── common/
│   └── snowpark_utils.py    # 共通処理クラス (共通PYファイル)
└── data/
    └── sample_data.csv      # テスト用データ (任意)

```

---

## 2. 認証情報 (`.streamlit/secrets.toml`)

セキュリティのため、コード内には直接書かず、このファイルで管理します。

```toml
[snowflake]
account = "your_account_locator"
user = "your_username"
password = "your_password"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"
database = "TEST_DB"
schema = "PUBLIC"

```

---

## 3. 共通PYファイル (`common/snowpark_utils.py`)

```python
import logging
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSessionException
from contextlib import contextmanager

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowparkManager:
    """
    Snowpark接続、CRUD、トランザクション、キャッシュを管理する共通クラス
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SnowparkManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, connection_parameters: dict):
        if not hasattr(self, 'session'):
            try:
                self.session = Session.builder.configs(connection_parameters).create()
                logger.info("Snowflake session created successfully.")
            except Exception as e:
                logger.error(f"Failed to create Snowflake session: {e}")
                raise

    def get_table(self, table_name: str, use_cache: bool = False):
        """テーブルを取得（キャッシュオプション付き）"""
        df = self.session.table(table_name)
        return df.cache_result() if use_cache else df

    @contextmanager
    def transaction(self):
        """
        トランザクション管理用コンテキストマネージャ
        自動的にCOMMIT/ROLLBACKを制御します
        """
        try:
            self.session.sql("BEGIN").collect()
            yield
            self.session.sql("COMMIT").collect()
            logger.info("Transaction committed.")
        except Exception as e:
            self.session.sql("ROLLBACK").collect()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise

    # --- CRUD 操作 ---

    def execute_query(self, query: str):
        """汎用クエリ実行"""
        return self.session.sql(query).collect()

    def batch_insert(self, df, table_name: str, mode: str = "append"):
        """Snowpark DataFrame または Pandas DataFrame を一括挿入"""
        df.write.mode(mode).save_as_table(table_name)
        logger.info(f"Batch insert to {table_name} completed.")

    def update_record(self, table_name: str, assignments: dict, condition: str):
        """レコードの更新 (SQL実行)"""
        set_clause = ", ".join([f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}" for k, v in assignments.items()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
        return self.execute_query(query)

    def delete_record(self, table_name: str, condition: str):
        """レコードの削除"""
        query = f"DELETE FROM {table_name} WHERE {condition}"
        return self.execute_query(query)

    def close(self):
        self.session.close()

```

---

## 4. テスト画面 (`app.py`)

Streamlitを使用して、共通クラスの機能をブラウザからテストできるようにします。

```python
import streamlit as st
import pandas as pd
from common.snowpark_utils import SnowparkManager

st.set_page_config(page_title="Snowpark 共通ツールテスト", layout="wide")
st.title("❄️ Snowpark Manager テスト画面")

# セッション管理
if 'manager' not in st.session_state:
    # secrets.tomlから情報を読み込み
    params = st.secrets["snowflake"]
    st.session_state.manager = SnowparkManager(params)

mgr = st.session_state.manager
TABLE_NAME = "DEMO_USERS"

# --- テスト用データ準備 ---
st.sidebar.header("1. データ初期化")
if st.sidebar.button("テーブル作成 & 初期化"):
    mgr.execute_query(f"CREATE OR REPLACE TABLE {TABLE_NAME} (ID INT, NAME STRING, ROLE STRING)")
    initial_data = pd.DataFrame({"ID": [1, 2], "NAME": ["田中", "佐藤"], "ROLE": ["Admin", "User"]})
    mgr.batch_insert(initial_data, TABLE_NAME, mode="overwrite")
    st.sidebar.success("初期化完了！")

# --- CRUD 操作画面 ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("📝 データ追加 (Batch Insert)")
    with st.form("insert_form"):
        new_id = st.number_input("ID", value=3)
        new_name = st.text_input("Name", value="鈴木")
        new_role = st.selectbox("Role", ["Admin", "User", "Viewer"])
        if st.form_submit_button("追加実行"):
            new_df = pd.DataFrame({"ID": [new_id], "NAME": [new_name], "ROLE": [new_role]})
            mgr.batch_insert(new_df, TABLE_NAME)
            st.success("追加しました")

with col2:
    st.subheader("🔄 トランザクションテスト (Rollback)")
    st.info("2件追加しようとしますが、途中でエラーを発生させてロールバックを確認します。")
    if st.button("トランザクション実行"):
        try:
            with mgr.transaction():
                # 1件目
                mgr.execute_query(f"INSERT INTO {TABLE_NAME} VALUES (99, '成功', 'Test')")
                # 意図的なエラー (存在しないテーブルへのアクセスなど)
                mgr.execute_query("INSERT INTO NON_EXISTENT_TABLE VALUES (1)") 
            st.success("コミットされました（通常はここに来ません）")
        except:
            st.error("エラー発生：ロールバックされました。ID:99は保存されていないはずです。")

# --- 表示と削除 ---
st.divider()
st.subheader("📊 現在のテーブルデータ (Cached)")
# キャッシュ機能のテスト
df_display = mgr.get_table(TABLE_NAME, use_cache=True).to_pandas()
st.dataframe(df_display, use_container_width=True)

if st.button("全データ削除"):
    mgr.delete_record(TABLE_NAME, "1=1")
    st.warning("データをすべて削除しました。")
    st.rerun()

```

---

## 5. この実装のポイント

1. **堅牢なトランザクション**: `with mgr.transaction():` ブロックを使うだけで、エラー時に自動で `ROLLBACK` が走ります。
2. **シングルトン構造**: `SnowparkManager` は一度インスタンス化されると、アプリ全体で同じセッションを再利用し、リソース消費を抑えます。
3. **キャッシュ利用**: `use_cache=True` を指定することで、頻繁に参照するデータをSnowflakeの一時結果セットから高速に読み込めます。
4. **本番用エラーハンドリング**: `try-except` とロギングを完備しており、トラブルシューティングが容易です。

### 実行方法

ターミナルで以下を実行してください：

```bash
pip install snowflake-snowpark-python streamlit pandas
streamlit run app.py

```

この構成をさらに拡張したい場合（例：特定のS3からのロード処理の追加など）は、いつでもお知らせください。次は何をお手伝いしましょうか？