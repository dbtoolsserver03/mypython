我一步一步用日文详细说明：
在 Snowflake Worksheet 里如何执行 Stored Procedure 并看到测试结果。

因为你现在是在 Snowflake 内部运行 Snowpark Python，所以执行入口就是 Worksheet → CALL 存储过程。

⸻

1 Snowflake Worksheet 在哪里（在哪里打开）

登录 Snowflake 后：

左侧菜单：

Projects
    ↓
Worksheets

点击 Worksheets。

会看到：

+ Worksheet

点击新建。

⸻

2 选择运行环境

在 Worksheet 上方选择：

Role
Warehouse
Database
Schema

例如：

Role: ACCOUNTADMIN
Warehouse: COMPUTE_WH
Database: DEMO_DB
Schema: PUBLIC


⸻

3 先创建测试表

在 Worksheet 里执行：

CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.USER_TEST (
    USER_ID NUMBER AUTOINCREMENT,
    USER_NAME STRING,
    EMAIL STRING,
    AGE NUMBER,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DEMO_DB.PUBLIC.USER_TEST(USER_NAME,EMAIL,AGE)
VALUES
('TARO','taro@test.com',20),
('HANAKO','hanako@test.com',25),
('JIRO','jiro@test.com',30);

点击 Run。

⸻

4 创建 Stage（SQL文件存放）

执行：

CREATE STAGE DEMO_DB.PUBLIC.SQL_STAGE;


⸻

5 上传 SQL 文件

如果使用 Snowflake UI：

左侧：

Data
 ↓
Databases
 ↓
DEMO_DB
 ↓
PUBLIC
 ↓
Stages
 ↓
SQL_STAGE

点击：

Upload

上传：

user.sql


⸻

6 创建 Snowpark Stored Procedure

在 Worksheet 执行：

CREATE OR REPLACE PROCEDURE TEST_SNOWPARK()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES=('snowflake-snowpark-python')
HANDLER='main'
AS
$$

from snowflake.snowpark import Session

def main(session: Session):

    rows = session.sql(
        "SELECT * FROM DEMO_DB.PUBLIC.USER_TEST"
    ).collect()

    for r in rows:
        print(r)

    return "TEST END"

$$;

点击 Run。

创建成功。

⸻

7 执行 Stored Procedure

在 Worksheet 执行：

CALL TEST_SNOWPARK();

点击 Run。

⸻

8 执行后的画面（测试画面）

Snowflake Worksheet 下方会显示：

Results

结果：

TEST END

同时在 Logs / Messages 中会看到：

(1,'TARO','taro@test.com',20)
(2,'HANAKO','hanako@test.com',25)
(3,'JIRO','jiro@test.com',30)


⸻

9 Worksheet执行流程总结

完整流程：

Worksheet
   ↓
创建表
   ↓
创建Stage
   ↓
上传SQL
   ↓
创建Stored Procedure
   ↓
CALL 存储过程
   ↓
查看执行日志


⸻

10 企业项目通常怎么运行

企业项目一般不是手动 CALL。

而是：

方法1 Task 定时执行

Snowflake Task
   ↓
CALL PROCEDURE

例如：

CREATE TASK USER_JOB
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 HOUR'
AS
CALL TEST_SNOWPARK();


⸻

方法2 Airflow

Airflow
   ↓
Snowflake Operator
   ↓
CALL SP


⸻

方法3 外部 API

Python
↓
Snowflake Connector
↓
CALL SP


⸻

11 Snowpark 推荐开发方式

开发流程：

VSCode
   ↓
写 Python
   ↓
上传 Stage
   ↓
创建 Procedure
   ↓
Worksheet 调试


⸻

12 常见错误（非常重要）

错误1

ModuleNotFoundError

原因：

没有指定 PACKAGES

必须：

PACKAGES=('snowflake-snowpark-python')


⸻

错误2

SQL compilation error

原因：

SQL里没有写：

DATABASE.SCHEMA.TABLE

正确：

DEMO_DB.PUBLIC.USER_TEST


⸻

错误3

Stage读取失败

必须写：

@STAGE_NAME/file.sql

例如：

@DEMO_DB.PUBLIC.SQL_STAGE/user.sql


⸻

13 我强烈建议的企业最佳实践

真实企业 Snowpark 项目一般这样：

Snowpark Project
│
├── common
├── dao
├── service
├── job
├── sql

然后：

job/main.py
↓
CREATE PROCEDURE
↓
TASK调度


⸻

如果你愿意，我可以 给你一套 Snowflake 官方架构级别的 Snowpark 项目模板（非常接近企业真实项目），包括：
	•	完整 Docker + Snowpark 本地开发
	•	SQL自动加载
	•	DAO自动映射
	•	百万数据批量写入
	•	CI/CD部署到Snowflake
	•	Task自动调度

这一套基本是 Snowpark企业标准模板。