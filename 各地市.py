import 数据入库
import pandas,总表计算,sqlite3,入库计算,数据入库
数据入库.RuKu()
ShuJvKu=sqlite3.connect("数据库.db")
ZongBiao=总表计算.ZongBiao()
# ZongBiao=pandas.read_excel("a.xlsx")
JvTi=ZongBiao[ZongBiao["总分"]!=0]
DuBan=JvTi[JvTi["总分"]!=2]
RuKu=pandas.read_sql("select 区县,入库量 from 各区域入库站址数",ShuJvKu)
QvXianJiShu=pandas.DataFrame(DuBan["区县"].value_counts())
for a in QvXianJiShu.itertuples():
    # print(a[0],RuKu.loc[RuKu["区县"]==a[0],"入库量"].values[0])
    QvXianJiShu.at[a[0],"入库"]=RuKu.loc[RuKu["区县"]==a[0],"入库量"].values[0]
QvXianJiShu["删减数"]=QvXianJiShu["入库"]-QvXianJiShu["区县"]
a1=pandas.DataFrame()
for a in QvXianJiShu.itertuples():
    # print(len(DuBan[DuBan["区县"]==a[0]]))
    # print(len(DuBan[DuBan["区县"]==a[0]].sort_values(by="总分",ascending=True).head(int(a[3]))))
    if a[3]<0:
        c=DuBan[DuBan["区县"]==a[0]]
        for b in DuBan[DuBan["区县"]==a[0]].sort_values(by="总分",ascending=True).head(int(a[3]*-1)).itertuples():
            c=c.drop(b[0],axis=0)
        a1=a1.append(c)
    else:
        a1=a1.append(DuBan[DuBan["区县"]==a[0]])
    # DuBan.drop(DuBan[DuBan["区县"]==a[0]].sort_values(by="总分",ascending=True).head(int(a[3])))
    # print(len(DuBan[DuBan["区县"]==a[0]]))
QuanShen=pandas.DataFrame(columns=["地市","历史告警次数","超频离线","超长离线","一级低压脱离不上传","停电告警不上传","疑似FSU接线错误","性能缺失","超时数","入库数","超时4天","超时7天","超时10天"])
MingXi=pandas.DataFrame()
for a in pandas.DataFrame(a1["地市"].value_counts()).itertuples():
    DiShi=a1[a1["地市"]==a[0]]
    try:
        ShuJvKu.execute("DROP TABLE 缓存")
        ShuJvKu.commit()
    except:
        pass
    DiShi.to_sql("缓存",ShuJvKu,index=False)
    DiShi=pandas.read_sql("select * from 缓存",ShuJvKu)
    DiShi=入库计算.RuKu(DiShi,a[0][:-3])
    # DiShi.to_excel("e.xlsx",index=False)
    # 省汇总
    GaoJing=len(DiShi[DiShi["告警次数"]>0])
    ChaoPing=len(DiShi[DiShi["超频离线"]>0])
    ChaoChang=len(DiShi[DiShi["超长离线"]>0])
    YiJiDiYa=len(DiShi[DiShi["一级低压脱离不上传"]>0])
    TingDian=len(DiShi[DiShi["停电告警不上传"]>0])
    FSU_JieChuo=len(DiShi[DiShi["疑似FSU接线错误"]>0])
    XinNeng=len(DiShi[DiShi["性能缺失"]>0])
    ChaoShi=len(DiShi[DiShi["处理不及时告警数"]>0])
    KouFen=len(DiShi[DiShi["总分"]>0])
    RuKu4Tian=len(DiShi[DiShi["入库天数"]=="4"])
    RuKu7Tian=len(DiShi[DiShi["入库天数"]=="7"])
    RuKu10Tian=len(DiShi[DiShi["入库天数"]=="10天以上"])
    # 输出表
    b=pandas.DataFrame([[a[0],GaoJing,ChaoPing,ChaoChang,YiJiDiYa,TingDian,FSU_JieChuo,XinNeng,ChaoShi,KouFen,RuKu4Tian,RuKu7Tian,RuKu10Tian]],columns=QuanShen.columns)
    QuanShen=pandas.concat([QuanShen,b],ignore_index=True)
    DiShi=DiShi[["地市","区县","站址名称","网格","运维监控系统ID","超频离线","超长离线","告警次数","一级低压脱离不上传","停电告警不上传","疑似FSU接线错误","性能缺失","处理不及时告警数","总分","入库天数"]]
    MingXi=MingXi.append(DiShi)
    DiShi2=JvTi[JvTi["地市"]==a[0]]
    HuiZong=QuanShen[QuanShen["地市"]==a[0]]
    # print(a[0])
    # print(QuanShen)
    with pandas.ExcelWriter('输出数据/'+a[0]+'.xlsx') as DiShiBiao:
        HuiZong.to_excel(DiShiBiao,sheet_name="汇总",index=False)
        DiShi.to_excel(DiShiBiao,sheet_name="督办明细",index=False)
        DiShi2.to_excel(DiShiBiao,sheet_name="具体明细",index=False)
ZongJi=[]
for Lie in QuanShen:
    if Lie!="地市":
        ZongJi.append(QuanShen[Lie].sum())
b=pandas.DataFrame([["全省"]+ZongJi],columns=QuanShen.columns)
QuanShen=pandas.concat([QuanShen,b],ignore_index=True)
with pandas.ExcelWriter('输出数据/全省.xlsx') as QuanShenBiao:
    QuanShen.to_excel(QuanShenBiao,sheet_name="明细",index=False)
    MingXi.to_excel(QuanShenBiao,sheet_name="具体明细",index=False)