import sqlite3,pandas
ShuJvKu=sqlite3.connect("数据库.db")
def FSU_JieCuo():
    # YouBiao=ShuJvKu.cursor()
    # YouBiao.execute("select * from 疑似FSU接线错误 GROUP BY 运维ID")
    # print(YouBiao.fetchall())
    return pandas.read_sql("select * from 疑似FSU接线错误 GROUP BY 运维ID",ShuJvKu)
# print(FSU_JieCuo())