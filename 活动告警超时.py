import sqlite3,pandas,datetime
ShuJvKu=sqlite3.connect("数据库.db")
with open("活动告警截止时期.txt","r") as JieZhi:
    JieZhiShiQi=JieZhi.read()
def ChaoShi1():
    # 一二级去除断电去除拉远站超8小时
    ShuJv=pandas.read_sql("select * from 活动告警 where (告警等级='一级告警' or 告警等级='二级告警') and (子设备名称 not like '%断电%')",ShuJvKu)
    ShuJv["告警发生时间"]=pandas.to_datetime(ShuJv["告警发生时间"],format="%Y-%m-%d %H:%M:%S")
    ShuJv["告警清除时间"]=pandas.to_datetime(JieZhiShiQi,format="%Y-%m-%d %H:%M:%S")
    # 时间修正
    ShuJv["告警发生时"]=ShuJv["告警发生时间"].apply(lambda a:a.strftime("%H:%M:%S"))
    ShuJv["告警清除时"]=ShuJv["告警清除时间"].apply(lambda a:a.strftime("%H:%M:%S"))
    ShuJv["告警发生时间2"]=ShuJv["告警发生时间"]
    ShuJv.loc[ShuJv["告警发生时"]<"06:00:00","告警发生期"]=ShuJv.loc[ShuJv["告警发生时"]<"06:00:00","告警发生时间2"].apply(lambda a: a.strftime("%Y-%m-%d "))
    ShuJv.loc[ShuJv["告警发生时"]<"06:00:00","告警发生时间2"]=ShuJv["告警发生期"]+"06:00:00"
    ShuJv["告警清除时间2"]=ShuJv["告警清除时间"]
    ShuJv.loc[ShuJv["告警清除时"]<"06:00:00","告警清除时间2"]=ShuJv.loc[ShuJv["告警清除时"]<"06:00:00","告警清除时间2"]-datetime.timedelta(days=1)
    ShuJv["告警清除期"]=ShuJv["告警清除时间2"].apply(lambda a:a.strftime("%Y-%m-%d "))
    ShuJv.loc[ShuJv["告警清除时"]<"06:00:00","告警清除时"]="23:59:59"
    ShuJv["告警清除时间2"]=ShuJv["告警清除期"]+ShuJv["告警清除时"]
    ShuJv["告警发生时间2"]=pandas.to_datetime(ShuJv["告警发生时间2"],format="%Y-%m-%d %H:%M:%S")
    ShuJv["告警清除时间2"]=pandas.to_datetime(ShuJv["告警清除时间2"],format="%Y-%m-%d %H:%M:%S")
    # 时间差计算
    ShuJv["天数"]=ShuJv["告警清除时间2"].apply(lambda a:a.strftime("%d")).astype(int)-ShuJv["告警发生时间2"].apply(lambda a:a.strftime("%d")).astype(int)
    ShuJv.loc[ShuJv["天数"]>=0,"时间差"]=ShuJv["告警清除时间2"]-ShuJv["告警发生时间2"]-datetime.timedelta(hours=6)*ShuJv.loc[ShuJv["天数"]>=0,"天数"]
    ShuJv=ShuJv[ShuJv["时间差"]>="8:00:00"]
    # print(ShuJv)
    return ShuJv
def ChaoShi2():
    # YouBiao=ShuJvKu.cursor()
    # YouBiao.execute("select 活动告警.* from 活动告警,告警名称 where (活动告警.告警等级='三级告警' or 活动告警.告警等级='四级告警') and (活动告警.告警名称=告警名称.'三四级超时+设备设施故障（所用16类）') and (活动告警.子设备名称 not like '断电') and (告警清除人!='采集机(工号:caijiji001)')")
    # LiShiGaoJing=YouBiao.fetchall()
    # print(LiShiGaoJing)
    # 三四级
    ShuJv=pandas.read_sql("select 活动告警.* from 活动告警,告警名称 where (活动告警.告警等级='三级告警' or 活动告警.告警等级='四级告警') and (活动告警.告警名称=告警名称.'三四级超时+设备设施故障（所用16类）') and (活动告警.子设备名称 not like '%断电%') and ([告警历时(分钟)]>=4320)",ShuJvKu)
    return ShuJv
# a=time.time()
# print(time.time()-a)
def ChaoShi():
    return pandas.concat([ChaoShi1(),ChaoShi2()],sort=True)
# print(ChaoShi())