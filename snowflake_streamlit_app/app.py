# app.py

import streamlit as st
from snowflake_utils import SnowparkManager
import pandas as pd

# --- 1. 页面基本配置 ---
st.set_page_config(page_title="Snowflake 数据管理后台", layout="wide")
st.title("❄️ Snowflake + Streamlit 控制台")

# --- 2. 使用缓存初始化 SnowparkManager ---
# st.cache_resource 确保在应用运行期间只创建一个连接对象，不会重复连接
@st.cache_resource
def get_db_manager():
    # 从 .streamlit/secrets.toml 中读取配置
    config = st.secrets["snowflake"]
    manager = SnowparkManager(config)
    manager.connect()
    return manager

# 实例化管理器
db_manager = get_db_manager()

# --- 3. UI 界面布局 ---
tab1, tab2 = st.tabs(["数据查看 (Read)", "事务操作 (CRUD)"])

# --- Tab 1: 数据展示 ---
with tab1:
    st.header("实时数据预览")
    target_table = st.text_input("输入表名", value="ORDERS")
    
    if st.button("刷新数据"):
        with st.spinner("正在从 Snowflake 读取..."):
            data = db_manager.get_table_data(target_table)
            if data is not None:
                st.success(f"成功读取 {len(data)} 行数据")
                st.dataframe(data, use_container_width=True) # 渲染表格
            else:
                st.error("无法读取数据，请检查表名。")

# --- Tab 2: 事务与更新示例 ---
with tab2:
    st.header("安全更新操作")
    st.info("演示：在事务中同时执行更新和删除操作")
    
    order_id = st.number_input("要更新的订单 ID", min_value=1, value=101)
    new_status = st.selectbox("修改状态为", ["PENDING", "PROCESSED", "SHIPPED"])

    if st.button("提交更改 (带事务)"):
        try:
            with db_manager.transaction():
                # 操作 1: 更新状态
                db_manager.run_update(
                    "ORDERS", 
                    {"STATUS": f"'{new_status}'"}, 
                    f"ORDER_ID = {order_id}"
                )
                
                # 操作 2: 记录日志（模拟另一个表的操作）
                # db_manager.session.sql(f"INSERT INTO LOG_TABLE ...").collect()
                
                st.success(f"订单 {order_id} 已更新，事务提交成功！")
        except Exception as e:
            st.error(f"操作失败，事务已安全回滚。错误详情: {e}")

# --- 侧边栏：连接状态 ---
st.sidebar.success("Snowflake 已连接")
if st.sidebar.button("断开连接"):
    db_manager.close()
    st.cache_resource.clear() # 清除缓存，下次需重新连接
    st.sidebar.warning("连接已断开")