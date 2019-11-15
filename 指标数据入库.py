import sqlite3,pandas,os
try:
    os.remove("数据库.db")
except:
    pass
ShuJvKu=sqlite3.connect("数据库.db")
BiaoJi=	os.listdir("考核数据")
for Biao in BiaoJi:
    BiaoMing=Biao.split(".")[0]
    Biao="考核数据/"+Biao
    if os.path.isdir(Biao):
        continue
    BiaoDuiXiang=pandas.read_excel(Biao)
    BiaoDuiXiang.to_sql(BiaoMing,ShuJvKu,index=False)
BiaoJi=os.listdir("考核数据/入库")
for Biao in BiaoJi:
    BiaoMing=Biao.split(".")[0]
    Biao="考核数据/入库/"+Biao
    if os.path.isdir(Biao):
        continue
    BiaoDuiXiang=pandas.read_excel(Biao)
    BiaoDuiXiang.to_sql(BiaoMing,ShuJvKu,index=False)