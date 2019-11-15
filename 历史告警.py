import sqlite3,pandas
ShuJvKu=sqlite3.connect("数据库.db")
def LiShiGangJing():
    # YouBiao=ShuJvKu.cursor()
    # YouBiao.execute("select * from 历史告警 where (告警名称 not like '红外') and (告警名称 not like '门')")
    # print(YouBiao.fetchall())
    return pandas.read_sql("select * from 历史告警 where (告警名称 not like '%红外%') and (告警名称 not like '%门%')",ShuJvKu)
# print(LiShiGangJing())