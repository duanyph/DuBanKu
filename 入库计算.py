import pandas,sqlite3,time
RiQi=time.strftime("%Y-%m-%d",time.localtime())
ShuJvKu=sqlite3.connect("数据库.db")
YouBiao=ShuJvKu.cursor()
def RuKu(DuBan,DiShi):
    ShuJv=pandas.read_sql("select * from "+DiShi+" where 运维监控系统ID IS NOT NULL",ShuJvKu)
    # ShuJv["运维监控系统ID"]=ShuJv["运维监控系统ID"].astype("int64")
    DuBan["处理日期"]=RiQi
    ShuJv=ShuJv.append(DuBan,sort=True)
    for JiLv in DuBan.itertuples():
        # DuBan.loc[JiLv[0],"处理日期"]=RiQi
        JiCi=len(ShuJv[ShuJv["运维监控系统ID"]==JiLv[5]])
        # print(JiLv[5],ShuJv["运维监控系统ID"])
        if JiCi==2:
            DuBan.loc[DuBan["运维监控系统ID"]==JiLv[5],"入库天数"]="4"
        elif JiCi==3:
            DuBan.loc[DuBan["运维监控系统ID"]==JiLv[5],"入库天数"]="7"
        elif JiCi>=4:
            DuBan.loc[DuBan["运维监控系统ID"]==JiLv[5],"入库天数"]="10天以上"
        else:
            DuBan.loc[DuBan["运维监控系统ID"]==JiLv[5],"入库天数"]="0"
    # 刷新入库表
    YouBiao.execute("DROP TABLE "+DiShi)
    ShuJv[["处理日期","运维监控系统ID"]].to_sql(DiShi,ShuJvKu,index=False)
    # ShuJv[["处理日期","运维监控系统ID"]].to_excel("c.xlsx",index=False)
    ShuJvKu.commit()
    # DuBan.to_excel("d.xlsx")
    return DuBan
# a=pandas.read_excel("b.xlsx")
# RuKu(a,"郑州")