# -*- coding: utf-8 -*-
import sys,os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
sys.path.append(r'/home/it_user/pythonproject/zzairlines')
from sqlalchemy import text
import connect.oracleDatabase as connect
import init.oracle_cfg as ora
import sql.foc_to_ods.oracle_foc_ods_sql as foc

list_to_ods = ['t_pub_flight','t_bas_airport','t_grnd_payload_info','t_sch_roster','t_hr_crew_bak','t_flt_schedule','t_ac_inflt_engineer','t_hr_crew']
ods_to_jieyou = ['td_flight_cost_base_detail']
foc_db_info = ora.get_db(db='foc')           # 接收来自生产FOC的数据
efb_db_info = ora.get_db(db='efb')           # 接收来自生产EFB的数据
ods_test_db_info = ora.get_db(db='ods_test') # 经分系统
ods_db_info = ora.get_db(db='ods')           # ods交换平台
jieyou_db_info = ora.get_db(db='jieyou')     # 节油系统
jieyouceshi_db_info = ora.get_db(db='jieyouceshi')     # 节油测试系统

k = 0
b1 = connect.OracleDatabase(foc_db_info)      # FOC
b2 = connect.OracleDatabase(ods_db_info)      # ods
b3 = connect.OracleDatabase(jieyou_db_info)   # 节油
b4 = connect.OracleDatabase(ods_test_db_info) # 经分
b5 = connect.OracleDatabase(jieyouceshi_db_info)   # 节油测试系统
# 1、传输到ODS
for i in list_to_ods:
    # SQL查询语句
    # query = "SELECT DBMS_METADATA.GET_DDL('TABLE',u.table_name),u.table_name FROM USER_TABLES u where TABLE_NAME ='T_PUB_FLIGHT'"
    query = f"SELECT * FROM {i}"
    truncate = text(f"truncate table {i}")
    # 分块读取并处理查询结果
    k += 1
    b2.do_execute(truncate)
    for chunk in b1.get_pd_datas(query):
        try:
            b2.put_pd_datas(chunk,i)
            print(f'第{k}次',i,len(chunk))
        except:
            print(f'第{k}个了，之前{i}加载过的。')
# 2、处理ODS结果表
truncate = text(f"truncate table {ods_to_jieyou[0]}")
b2.do_execute(truncate)
b2.do_execute(foc.td_flight_cost_base_detail)

# 3、数据分发节油、经分、节油测试系统
for i in [b3,b4,b5]:
    for j in ods_to_jieyou:
        query = f"SELECT * FROM {j}"
        truncate = text(f"truncate table {j}")
        k += 1
        i.do_execute(truncate)
        for chunk in b2.get_pd_datas(query):
            try:
                i.put_pd_datas(chunk,j)
                print(f'第{k}次',j,len(chunk))
            except:
                print(f'第{k}个了，之前{i}加载过的。')

# 4、关闭数据库链接
for i in [b1,b2,b3,b4,b5]:
    i.db_closed()
