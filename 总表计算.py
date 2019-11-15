import sqlite3,pandas,历史告警,FSU接错,性能缺失,历史告警超时,活动告警超时
ShuJvKu=sqlite3.connect("数据库.db")
def ZongBiao():
    HuiZong=pandas.read_sql("select 地市,区县,站址名称,[机房(动环)维护人员] as 网格,运维ID as 运维监控系统ID from 站址",ShuJvKu)
    LiShiGaoJing=历史告警.LiShiGangJing()
    YiJiDiYa=pandas.read_sql("select * from 一级低压脱离",ShuJvKu)
    TingDianGaoJing=pandas.read_sql("select * from 停电告警不上传",ShuJvKu)
    FSU_JieCuo=FSU接错.FSU_JieCuo()
    XinNeng=性能缺失.XinNengQueShi()
    LiShiChaoShi=历史告警超时.ChaoShi()
    HuoDongChaoShi=活动告警超时.ChaoShi()
    try:
        ChaoPing=pandas.read_sql("select * from FSU超频离线站址明细_日",ShuJvKu)
        for a in HuiZong.itertuples():
            HuiZong.loc[a[0],"超频离线"]=ChaoPing[ChaoPing["站址运维ID"]==a[5]]["离线次数"].sum()
            b=HuiZong.loc[a[0],"超频离线"]
            if b>=1:
                HuiZong.loc[a[0],"超频离线扣分"]=10
            else:
                HuiZong.loc[a[0],"超频离线扣分"]=0
            # print(HuiZong.loc[a[0],["超频离线","超频离线扣分"]])
    except:
        HuiZong["超频离线"]=0
        HuiZong["超频离线扣分"]=0
    try:
        ChaoChang=pandas.read_sql("select * from FSU超长离线站址明细_日",ShuJvKu)
        for a in HuiZong.itertuples():
            HuiZong.loc[a[0],"超长离线"]=ChaoChang[ChaoChang["站址运维ID"]==a[5]]["离线次数"].sum()
            b=HuiZong.loc[a[0],"超长离线"]
            if b>=1:
                HuiZong.loc[a[0],"超长离线扣分"]=10
            else:
                HuiZong.loc[a[0],"超长离线扣分"]=0
            # print(HuiZong.loc[a[0],["超长离线","超长离线扣分"]])
    except:
        HuiZong["超长离线"]=0
        HuiZong["超长离线扣分"]=0
    for a in HuiZong.itertuples():
        # 告警次数及扣分计算
        HuiZong.loc[a[0],"告警次数"]=len(LiShiGaoJing[LiShiGaoJing["站址运维ID"]==a[5]])
        b=HuiZong.loc[a[0],"告警次数"]
        if b==0:
            HuiZong.loc[a[0],"告警扣分"]=0
        elif b<=15:
            HuiZong.loc[a[0],"告警扣分"]=2
        elif b>=30:
            HuiZong.loc[a[0],"告警扣分"]=20
        elif b>=25:
            HuiZong.loc[a[0],"告警扣分"]=15
        elif b>=20:
            HuiZong.loc[a[0],"告警扣分"]=10
        elif b>15:
            HuiZong.loc[a[0],"告警扣分"]=5
        # print(HuiZong.loc[a[0],"告警扣分"])
        # 一级低压脱离及扣分计算
        HuiZong.loc[a[0],"一级低压脱离不上传"]=len(YiJiDiYa[YiJiDiYa["站址运维ID"]==a[5]])
        b=HuiZong.loc[a[0],"一级低压脱离不上传"]
        if b>=1:
            HuiZong.loc[a[0],"一级低压脱离扣分"]=10
        else:
            HuiZong.loc[a[0],"一级低压脱离扣分"]=0
        # print(HuiZong.loc[a[0],["一级低压脱离不上传","一级低压脱离扣分"]])
        # 停电告警不上传及扣分计算
        HuiZong.loc[a[0],"停电告警不上传"]=len(TingDianGaoJing[TingDianGaoJing["站址运维ID"]==a[5]])
        b=HuiZong.loc[a[0],"停电告警不上传"]
        if b>=1:
            HuiZong.loc[a[0],"停电告警扣分"]=10
        else:
            HuiZong.loc[a[0],"停电告警扣分"]=0
        # print(HuiZong.loc[a[0],["停电告警不上传","停电告警扣分"]])
        # 疑似fsu接错及扣分计算
        HuiZong.loc[a[0],"疑似FSU接线错误"]=len(FSU_JieCuo[FSU_JieCuo["运维ID"]==a[5]])
        b=HuiZong.loc[a[0],"疑似FSU接线错误"]
        if b>=1:
            HuiZong.loc[a[0],"FSU_接错扣分"]=10
        else:
            HuiZong.loc[a[0],"FSU_接错扣分"]=0
        # print(HuiZong.loc[a[0],["疑似FSU接线错误","FSU_接错扣分"]])
        # 性能缺失及扣分计算
        HuiZong.loc[a[0],"性能缺失"]=len(XinNeng[XinNeng["站址运维ID"]==a[5]])
        b=HuiZong.loc[a[0],"性能缺失"]
        if b>=1:
            HuiZong.loc[a[0],"性能缺失扣分"]=10
        else:
            HuiZong.loc[a[0],"性能缺失扣分"]=0
        # print(HuiZong.loc[a[0],["性能缺失","性能缺失扣分"]])
        # 处理不及时数及扣分计算
        HuiZong.loc[a[0],"处理不及时告警数"]=len(LiShiChaoShi[LiShiChaoShi["站址运维ID"]==a[5]])+len(HuoDongChaoShi[HuoDongChaoShi["站址运维ID"]==a[5]])
        b=HuiZong.loc[a[0],"处理不及时告警数"]
        if b==0:
            HuiZong.loc[a[0],"处理不及时扣分"]=0
        elif b>8:
            HuiZong.loc[a[0],"处理不及时扣分"]=20
        elif b<=3:
            HuiZong.loc[a[0],"处理不及时扣分"]=10
        elif b<=8:
            HuiZong.loc[a[0],"处理不及时扣分"]=15
        # if b>0:
        #     print(HuiZong.loc[a[0],["处理不及时告警数","处理不及时扣分"]])
        # 总扣分计算
        # print(HuiZong.loc[a[0],"超频离线扣分"],HuiZong.loc[a[0],"超长离线扣分"],HuiZong.loc[a[0],"告警扣分"],HuiZong.loc[a[0],"一级低压脱离扣分"],HuiZong.loc[a[0],"停电告警扣分"],HuiZong.loc[a[0],"FSU_接错扣分"],HuiZong.loc[a[0],"性能缺失扣分"],HuiZong.loc[a[0],"处理不及时扣分"])
        HuiZong.loc[a[0],"总分"]=HuiZong.loc[a[0],"超频离线扣分"]+HuiZong.loc[a[0],"超长离线扣分"]+HuiZong.loc[a[0],"告警扣分"]+HuiZong.loc[a[0],"一级低压脱离扣分"]+HuiZong.loc[a[0],"停电告警扣分"]+HuiZong.loc[a[0],"FSU_接错扣分"]+HuiZong.loc[a[0],"性能缺失扣分"]+HuiZong.loc[a[0],"处理不及时扣分"]
    # HuiZong.to_excel("a.xlsx")
    # HuiZong.to_sql("总表",ShuJvKu,index=False)
    return HuiZong
# ZongBiao()