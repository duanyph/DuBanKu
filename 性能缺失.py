import sqlite3,pandas
ShuJvKu=sqlite3.connect("数据库.db")
def XinNengQueShi():
    # YouBiao=ShuJvKu.cursor()
    # YouBiao.execute("select * from 性能缺失 GROUP BY 设备运维ID")
    # print(YouBiao.fetchall())
    return pandas.read_sql("select * from 性能缺失 GROUP BY 站址运维ID",ShuJvKu)
# print(len(XinNengQueShi()))