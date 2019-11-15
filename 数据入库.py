import sqlite3,pandas,time,os
BiaoJi=	os.listdir("源数据")
def RuKu():
    # 初始化数据库
    ShuJvKu=sqlite3.connect("数据库.db")
    YouBiao=ShuJvKu.cursor()
    try:
        YouBiao.execute("DROP TABLE 站址;")
    except:
        pass
    ZhanZhiBiao=pandas.DataFrame()
    for Biao in BiaoJi:
        BiaoMing=Biao.split(".")[0]
        try:
            YouBiao.execute("DROP TABLE "+BiaoMing+";")
        except:
            pass
        Biao="源数据/"+Biao
        # b=time.time()
        # 表格二维格式化，汇总等
        if ("超频" in Biao) or ("超时" in Biao):
            BiaoDuiXiang=pandas.read_excel(Biao,sheet_name=1,header=1)
        elif "历史" in Biao:
            sheet_Ji=pandas.read_excel(Biao,sheet_name=None)
            BiaoDuiXiang=pandas.DataFrame()
            for a in sheet_Ji:
                BiaoDuiXiang=pandas.concat([BiaoDuiXiang,pandas.read_excel(Biao,sheet_name=a)])
        elif "站址" in Biao:
            ZhanZhiBiao=pandas.concat([ZhanZhiBiao,pandas.read_excel(Biao,header=1)])
            continue
        else:
            BiaoDuiXiang=pandas.read_excel(Biao)
        # 表格数据入库
        BiaoDuiXiang.to_sql(BiaoMing,ShuJvKu,index=False)
    ZhanZhiBiao.to_sql("站址",ShuJvKu,index=False)
        # print(time.time()-b)
# RuKu()