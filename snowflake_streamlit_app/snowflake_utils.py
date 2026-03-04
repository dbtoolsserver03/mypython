# snowflake_utils.py

import logging
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkException
from snowflake.snowpark.functions import col
from contextlib import contextmanager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SnowparkUtils")

class SnowparkManager:
    def __init__(self, connection_params):
        """
        初始化时传入连接参数
        :param connection_params: 字典格式的配置
        """
        self.params = connection_params
        self.session = None

    def connect(self):
        """
        建立或检查当前 Session
        """
        if self.session is None or self.session.is_closed():
            try:
                self.session = Session.builder.configs(self.params).create()
                logger.info("Snowflake Session 已成功创建")
            except Exception as e:
                logger.error(f"Snowflake 连接失败: {str(e)}")
                raise
        return self.session

    @contextmanager
    def transaction(self):
        """
        事务上下文管理器：确保多步操作的原子性
        """
        sess = self.connect()
        try:
            sess.sql("BEGIN").collect()
            yield sess
            sess.sql("COMMIT").collect()
        except Exception as e:
            sess.sql("ROLLBACK").collect()
            logger.error(f"事务执行失败并已回滚: {str(e)}")
            raise

    def get_table_data(self, table_name, limit_row=100):
        """
        共通查询方法：获取表数据并转换为 Pandas DataFrame (Streamlit 展示常用)
        """
        self.connect()
        try:
            # Snowpark DataFrame 转为 Pandas DataFrame 以便 Streamlit 直接渲染
            return self.session.table(table_name).limit(limit_row).to_pandas()
        except Exception as e:
            logger.error(f"读取表 {table_name} 失败: {str(e)}")
            return None

    def run_update(self, table_name, assignments, condition):
        """
        共通更新方法
        """
        self.connect()
        try:
            return self.session.table(table_name).update(assignments, condition)
        except Exception as e:
            logger.error(f"更新失败: {str(e)}")
            raise