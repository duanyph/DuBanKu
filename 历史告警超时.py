import sqlite3,pandas,datetime
ShuJvKu=sqlite3.connect("数据库.db")
def ChaoShi1():
    YouBiao=ShuJvKu.cursor()
    YouBiao.execute("select * from 历史告警 where (告警等级='一级告警' or 告警等级='二级告警') and (子设备名称 not like '%断电%')")
    # 一二级去除断电去除拉远站去除采集机清除
    LiShiGaoJing=YouBiao.fetchall()
    YouBiao.execute("select 站址运维ID from 拉远站")
    LaYuanZhan=YouBiao.fetchall()
    QvLaYuanZhan=[]
    for a in LiShiGaoJing:
        if(a[11],) not in LaYuanZhan:
            QvLaYuanZhan.append(a)
    BiaoTou=pandas.read_sql("select * from 历史告警 where 1!=1",ShuJvKu)
    ShuJv1=pandas.DataFrame(QvLaYuanZhan,columns=BiaoTou.columns)
    ShuJv2=ShuJv1
    ShuJv1=ShuJv1[ShuJv1["告警清除人"]!='采集机(工号:caijiji001)']
    # 一二级去除断电去除拉远站超8小时
    ShuJv2["告警发生时间"]=pandas.to_datetime(ShuJv2["告警发生时间"],format="%Y-%m-%d %H:%M:%S")
    ShuJv2["告警清除时间"]=pandas.to_datetime(ShuJv2["告警清除时间"],format="%Y-%m-%d %H:%M:%S")
    # 时间修正
    ShuJv2["告警发生时"]=ShuJv2["告警发生时间"].apply(lambda a:a.strftime("%H:%M:%S"))
    ShuJv2["告警清除时"]=ShuJv2["告警清除时间"].apply(lambda a:a.strftime("%H:%M:%S"))
    ShuJv2["告警发生时间2"]=ShuJv2["告警发生时间"]
    ShuJv2.loc[ShuJv2["告警发生时"]<"06:00:00","告警发生期"]=ShuJv2.loc[ShuJv2["告警发生时"]<"06:00:00","告警发生时间2"].apply(lambda a: a.strftime("%Y-%m-%d "))
    ShuJv2.loc[ShuJv2["告警发生时"]<"06:00:00","告警发生时间2"]=ShuJv2["告警发生期"]+"06:00:00"
    ShuJv2["告警清除时间2"]=ShuJv2["告警清除时间"]
    ShuJv2.loc[ShuJv2["告警清除时"]<"06:00:00","告警清除时间2"]=ShuJv2.loc[ShuJv2["告警清除时"]<"06:00:00","告警清除时间2"]-datetime.timedelta(days=1)
    ShuJv2["告警清除期"]=ShuJv2["告警清除时间2"].apply(lambda a:a.strftime("%Y-%m-%d "))
    ShuJv2.loc[ShuJv2["告警清除时"]<"06:00:00","告警清除时"]="23:59:59"
    ShuJv2["告警清除时间2"]=ShuJv2["告警清除期"]+ShuJv2["告警清除时"]
    ShuJv2["告警发生时间2"]=pandas.to_datetime(ShuJv2["告警发生时间2"],format="%Y-%m-%d %H:%M:%S")
    ShuJv2["告警清除时间2"]=pandas.to_datetime(ShuJv2["告警清除时间2"],format="%Y-%m-%d %H:%M:%S")
    # 时间差计算
    ShuJv2["天数"]=ShuJv2["告警清除时间2"].apply(lambda a:a.strftime("%d")).astype(int)-ShuJv2["告警发生时间2"].apply(lambda a:a.strftime("%d")).astype(int)
    ShuJv2.loc[ShuJv2["天数"]>=0,"时间差"]=ShuJv2["告警清除时间2"]-ShuJv2["告警发生时间2"]-datetime.timedelta(hours=6)*ShuJv2.loc[ShuJv2["天数"]>=0,"天数"]
    ShuJv=pandas.concat([ShuJv1,ShuJv2[ShuJv2["时间差"]>="8:00:00"]],sort=True)
    # print(ShuJv2.loc[ShuJv2["时间差"]>="24:00:00",["告警发生时间","告警清除时间","时间差"]])
    return ShuJv
def ChaoShi2():
    # YouBiao=ShuJvKu.cursor()
    # 三四级
    # YouBiao.execute("select 历史告警.* from 历史告警,告警名称 where (历史告警.告警等级='三级告警' or 历史告警.告警等级='四级告警') and (历史告警.告警名称=告警名称.'三四级超时+设备设施故障（所用16类）') and (历史告警.子设备名称 not like '断电') and (告警清除人!='采集机(工号:caijiji001)')")
    # LiShiGaoJing=YouBiao.fetchall()
    ShuJv1=pandas.read_sql("select 历史告警.* from 历史告警,告警名称 where (历史告警.告警等级='三级告警' or 历史告警.告警等级='四级告警') and (历史告警.告警名称=告警名称.'三四级超时+设备设施故障（所用16类）') and (历史告警.子设备名称 not like '%断电%') and (告警历时>=4320)",ShuJvKu)
    ShuJv2=pandas.read_sql("select 历史告警.* from 历史告警,告警名称 where (历史告警.告警等级='三级告警' or 历史告警.告警等级='四级告警') and (历史告警.告警名称=告警名称.'三四级超时+设备设施故障（所用16类）') and (历史告警.子设备名称 not like '%断电%') and (告警清除人!='采集机(工号:caijiji001)')",ShuJvKu)
    # print(LiShiGaoJing)
    return pandas.concat([ShuJv1,ShuJv2])
# a=time.time()
# print(time.time()-a)
def ChaoShi():
    return pandas.concat([ChaoShi1(),ChaoShi2()],sort=True)
# print(ChaoShi())