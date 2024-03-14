# encoding = utf8
import pandas as pd,csv,os,sys,datetime
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import init.oracle_cfg as oracfg,init.mysql_cfg as mysql
import JieYou.qar_sql as sql
import common.tosql_upsert_func as fun
import connect.mysqlDatabase as mysqldb,connect.oracleDatabase as oradb
sys.path.append(r'/home/it_user/pythonproject/zzairlines')
sys.path.append(r'/it/zzit_python/')
# auth:lvzongwei
# 1、改逻辑【油向上取之和与向下取之和比较，取大】-20240124
# 2、增加777机型-20240221
# 3、增加800机型取油逻辑(空中油量(取2-3万英尺的数据))-20240313
# 4、增加单发滑行逻辑-20240314

class Dealqardata:
    def __init__(self, result_file, filepath):
        self.__filepath = filepath
        self.__filename = None
        self.__top = 0
        self.__bottom = 0
        self.__result_file = result_file
        self.__listlog = []
    def __file_code(self):
        '''
        获取并返回解析文件编码格式
        :return:
        '''
        try:
            import codecs
            with codecs.open(self.__filename, 'r',errors='ignore') as file:
                aa1 = file.read(32)[1:]
                encoding = file.encoding
            return encoding
        except Exception as e:
            import cchardet
            with open(self.__filename,'rb') as f:
                res = cchardet.detect(f.read(512))['encoding']
                if res in ['ISO-8859-1','ASCII']:
                    return 'GB2312'
                if res == 'EUC-TW':
                    return 'GBK'
            return res

    def __readfile(self,columns,code,col,filenane): # on_bad_lines='skip',
        '''
        使用pandas数据分析包读取SCV格式文件，跳过前三行，忽略告警，使用python解析器
        :param columns:
        :param code:
        :param col:
        :param filenane:
        :return:
        '''
        self.__listlog.append(datetime.datetime.now().strftime('%Y%m%d%H%S'))
        try:
            self.__data = pd.read_csv(self.__filename,on_bad_lines='warn',skiprows=3,encoding=code, engine='python').iloc[:,:col]
            # self.__data.sort_values(by=self.__data.iloc[2],axis=0, ascending=False, inplace=True, na_position='last') 排序
            self.__data.columns = columns
            self.header_name = self.__data.iloc[0]
            self.__data['flag']='0'
            self.__data['coment']= '' # str(self.__filename)
            self.__data['deal_date']=datetime.datetime.now().strftime('%Y%m%d%H%S') # 加载日期到小时
            self.__listlog.append('文件正常编译')
        except Exception as e:
            list_taget = []
            list_taget.append((e,'文件异常，无法加载：'+self.__filename))
            self.__tocsv(filenane, list_taget)
            self.__listlog.append('文件异常，无法编译')

    def __getfiledata_gndfile(self, columns,gnd6,res_file,filenane):
        '''
        获取文件信息，取出判断离地接地标志，并取得离地、接地行数，于该行的标志位(flag)打上标志
        标志：离地、接地行数，否则标志位置 0
        :param columns:文件列重命名
        :param gnd6:选取执行通道
        :param res_file:存储文件执行信息
        :param filenane:正执行文件名
        :return:标志位行上：写上油量、行数、总重，总油
        '''
        col,r,c = 0,0,0
        if gnd6 == 'gnd_6':
            # 1、800 文件解析
            self.__readfile(columns,'utf8',68,filenane)
            # 1.1、获取起落信息
            self.df_value = self.__data.iloc[:, 16:28]
            # 1.2、起落信息开始行数
            col = 44
            r,c = self.df_value.shape
        elif gnd6 == 'gnd_7':
            # 2、777 文件解析
            self.__readfile(columns,self.__file_code(),40,filenane)
            # 2.1、获取起落信息 第一部分
            self.df_value = self.__data.iloc[:, 20:35]
            # 2.2、获取起落信息 第二部分
            self.df_value['L_AIR_GROUND'] = self.__data['L_AIR_GROUND']
            print(self.df_value.head(5),self.df_value.shape,)
            col = 35
            r,c = self.df_value.shape
        elif gnd6 == 'gnd_3':
            # 3、300 文件解析
            self.__readfile(columns,self.__file_code(),24,filenane)
            # 3.1、获取机号
            reg_code = self.__getregcode(gnd6)
            col = 9 # B-2963 两个轮 总油 左右 油的单位：LBS 不用加
            if reg_code in ['B-2945','B-2119','B-2908']:
                # B2945、B2119、B2908 三个轮 总油 左右 油的单位：KG
                col += 1
                # print('未取到全AIR行:',reg_code,self.__filename)
            # 3.2、获取起落信息
            self.df_value = self.__data.iloc[:,7:col]
            r,c = self.df_value.shape
            col += 1
        # 通过起落信息获取具体的离地、接地行
        val = self.__gettophang(r,c,col,gnd6,res_file,filenane)
        print('上油-------------',val)
        # 4、800飞机有离地油逻辑有求和取大的比较需求，获取比较结果后回写数据
        if gnd6 in['gnd_6',]:
            # 4.1、返回数据处理：二维数据类型
            df = pd.DataFrame(data=val[0],)   # 离地油
            df1 = pd.DataFrame(data=val[-1],) # 空中油
            # 4.2、0 代替 空数据
            df = df.fillna(value=0)
            df1 = df1.fillna(value=0) # 空中油处理空值 0替
            # 4.3、转浮点数类型
            one,two,thr = df.iloc[:,0].astype('float'),df.iloc[:,1].astype('float'),df.iloc[:,-1] # 离地油
            yi,er = df1.iloc[1:,0].astype('float'),df1.iloc[1:,-1].astype('float') # 空中油
            x,y = map(int,val[-1][0])
            print(f'大吕神腰展嗷Aw(ﾟДﾟ)',df1,'\n离地油:',df,'\n第一个值:',yi,yi.sum(axis=0),er,er.sum(axis=0),'\n',val[-1][0],'\n',x,y)
            self.__data.iloc[x,y] = str(yi.sum(axis=0)) if yi.sum(axis=0) > er.sum(axis=0) else str(er.sum(axis=0))
            # 4.4、数据比较，上面数据大标志 0 else 1
            k = 0 if one.sum(axis=0)>=two.sum(axis=0) else 1
            for i in range(len(df)):
                # 4.5、需要修改的坐标拆解
                c,r = map(int,thr[i])
                print(f'第{i}次，坐标：{c,r}，赋值：',one[i],two[i],'原来的值：',self.__data.iloc[c,r])
                # 4.6、确定回填的结果
                res = one[i] if k== 0 else two[i]
                # 4.7、回填 赋值
                self.__data.iloc[c,r] = str(res)
                print(f'取上油{k,i}，坐标：{c,r}，新赋值：',self.__data.iloc[c,r])#df.iloc[:,0].sum(axis=1),df.iloc[:,1].sum(axis=1))
                # 4.8、记录回填日志
                self.__listlog.append(f'取上油赋值：{self.__data.iloc[c,r]}，坐标：（{c},{r}）')

    def __sortnum(self,list_,column,b,c):
        '''
        返回相对坐标值：k
        :param list_:
        :param column:
        :param b:结束标志
        :param c:步长
        :return:
        '''
        for i in range(column,b,c):
            if float(list_[i])>0:
                k = i - column
                print('K:',k,'第几行：',i,'油：',list_[i])
                return k
        else:return 0

    def __getairoil800(self,column,row):
        df = self.__data.where(self.__data.notnull(),0)
        # 1、回退1行，转列表
        row -= 1
        list02 = df.iloc[:, row].tolist()
        # 2、去掉异常值干扰
        list03 = [float(0 if _ in ["",'     FFL','','..','CENTER FUEL WEIGHT','FUEL QTY CENTER MAIN','FUEL QTY LEFT MAIN','FUEL QTY RIGHT MAIN','GROSS WEIGHT OF AIRCRAFT','KGS/LBS','LBS','LBS/HR','MAIN FUEL WEIGHT L','MAIN FUEL WEIGHT R','TONS','TOTAL FUEL WEIGHT','X','kg','全重','右燃油流量','总油量','左燃油流量'] else _) for _ in list02]
        # 3、向下移动20行开始处理-(全AIR之后，向下数20行取值)
        res = []
        column = column+20 # 向下数20行
        if list03[column] != 0: # self.__data.iloc[column,row-1]=str(list02[column+20]) # 按位置回退一行
            # 4、当前行不等于0，返回
            res.append((list03[column],list03[column]))
            print(f'wa：{list02[column]}、{list03[column]}')
        else:
            if max(list03) == 0: # self.__data.iloc[column,row-1]='0'
                # 5、当前列表最大值为0，返回字符0
                res.append((0,0))
                print(f'大吕神腰展b：max-0')
            else:
                # 6、当前行空，上下有值，处理方式如下
                col = self.__getval(list03,column,'top')
                col0 = int(col[0]) + column # 下行数坐标值
                col1 = int(col[1]) + column # 上行数坐标值
                print(f'大吕神腰展-下油行数{col0},下油量:',self.__data.iloc[col0,row],col,f'上油行数{col1}上油量：',self.__data.iloc[col1,row])
                res.append((self.__data.iloc[col0,row],self.__data.iloc[col1,row]))
        return res
    def __getFullweight(self,column,row):
        df = self.__data.where(self.__data.notnull(),0)
        # 1、回退1行，转列表
        list02 = df.iloc[:, row-1].tolist()
        # 2、去掉异常值干扰
        list03 = [float(0 if _ in ['GROSS WEIGHT OF AIRCRAFT','TONS','TOTAL FUEL WEIGHT','KGS/LBS','kg', '全重','',"", 'X','总油量','FUEL QTY CENTER MAIN', 'LBS','..','GROSS WEIGHT OF AIRCRAFT','TONS'] else _) for _ in list02]
        # 3、向下移动20行开始处理-(全AIR之后，向下数20行取值)
        res = 0
        if list03[column+20] != 0: # self.__data.iloc[column,row-1]=str(list02[column+20]) # 按位置回退一行
            # 4、当前行不等于0，返回
            res = str(list02[column+20])
            print(f'大吕神腰展a：{list02[column+20]}')
        else:
            if max(list03) == 0: # self.__data.iloc[column,row-1]='0'
                # 5、当前列表最大值为0，返回字符0
                res = '0'
                print(f'大吕神腰展b：max-0')
            else:
                # 6、当前行空，上下有值，处理方式如下
                k = self.__getnum(list03,column+20)
                # self.__data.iloc[column,row-1]=self.__data.iloc[column+k+20, row - 1] # 按位置回退一行
                res = self.__data.iloc[column+k+20, row - 1]
                print(f'大吕神腰展c：{self.__data.iloc[column+k+20, row - 1]}')
        return res
    def __getnum(self,list01,column):
        # 1、下部坐标值
        k1 = self.__sortnum(list01,column,len(list01),1)
        # 2、上部坐标值
        k2 = self.__sortnum(list01,column,0,-1)
        # 无航油数据时，找上下最近的数据取值；如果距离相同，向下取值 1.0版本
        if k2 ==0:
            return k1
        else:
            return k1 if abs(k2) > abs(k1) & k1!=0 else k2
    def __getval(self,list01,column,st_):
        '''
        返回两个坐标值
        :param list01:
        :param column:
        :param st_:
        :return:返回离地时(上下两个值)、接地油量值(向下一个值)
        '''
        # 1、下部坐标值
        k1 = self.__sortnum(list01,column,len(list01),1)
        # 2、上部坐标值
        k2 = self.__sortnum(list01,column,0,-1)
        # 3、无航油数据时，起飞总油量的值，全AIR的，上下取大；落地总油量的值,全GND的，向下取值。1.1版本
        print(f'接地油 k1:{k1},K1对应的值{list01[column+k1]},k2:{k2},K2对应的值{list01[column+k2]}:',k1 if abs(k2) > abs(k1) & k1!=0 & k2!=0 else k2)
        # 4、
        return k1,k2 if st_ == 'top' else k1 # 【下油、上油坐标】、【下油坐标】
        # if st_ == 'top':return k1,k2 # 下油、上油坐标
            # if k2 == 0:
            #     return k1
            # else:
            #     return k2 if abs(int(list01[column+k2])) > abs(int(list01[column+k1])) & int(list01[column+k1])!=0 else k1
        # else:return k1
    def __getoil(self,column,row,st_,gn3=None):
        df = self.__data.where(self.__data.notnull(),0)
        list01 = df.iloc[:, row].tolist()
        list01 = [float(0 if _ in ['MAIN FUEL WEIGHT R','MAIN FUEL WEIGHT L','CENTER FUEL WEIGHT','KGS/LBS','TOTAL FUEL WEIGHT','FUEL QTY RIGHT MAIN','FUEL QTY LEFT MAIN','LBS/HR','右燃油流量','总油量','X','左燃油流量', 'LBS/HR','LBS','全重','左燃油流量','','     FFL','FUEL QTY CENTER MAIN'] else _) for _ in list01]
        self.__data.at[column,'flag'] = f'{column}'
        if df.iloc[column,row] == 0:
            col = self.__getval(list01,column,st_)
            if st_ == 'top':
                col0 = int(col[0])
                col0 += column # 行数下
                col1 = int(col[1])
                col1 += column # 行数上
                if gn3 in ['gnd_6','gnd_7']:
                    print(f'上油行数{col0},下油:',self.__data.iloc[col0,row],col,'上油：',self.__data.iloc[col1,row])
                    return (self.__data.iloc[col0,row],self.__data.iloc[col1,row],(column,row))
                else:
                    val1 = self.__data.iloc[col0, row] if not pd.isna(self.__data.iloc[col0, row]) else '0'
                    val2 = self.__data.iloc[col1, row] if not pd.isna(self.__data.iloc[col1, row]) else '0'
                    print(f'777调试{col0},{col1}-lvzw{self.__data.iloc[col0, row] if not pd.isna(self.__data.iloc[col0, row]) else 0}--值：{val1},{val2}，类型：{type(val1)}，{type(val2)}')
                    print(f'777调试，转类型：{float(val1)}，{float(val2)},{"float" if ".00" in val1 else "int"}')
                    valtype = eval("float" if ".00" in val1 else "int") # 777增加类型判断
                    print(f'777调试，{valtype}，选值：{val1 if valtype(val1)>=valtype(val2) else val2}')
                    # self.__data.iloc[column, row] = val1 if int(val1)>=int(val2) else val2 # 原来逻辑
                    self.__data.iloc[column, row] = val1 if valtype(val1)>=valtype(val2) else val2 # 777修改逻辑
                    print(f'777-取上油赋值：{self.__data.iloc[column, row]}，坐标：（{column},{row}）')
                    self.__listlog.append(f'取上油赋值：{self.__data.iloc[column, row]}，坐标：（{column},{row}）')
            else:
                col = int(col[0])
                col += column
                print('lvzw:-----',col)
                self.__data.iloc[column,row] = self.__data.iloc[col,row]
                print(f'下油行数{col},返回行数:',self.__getnum(list01,column),'具体行:',col,'油：',self.__data.iloc[column,row])
                self.__listlog.append(f'取下油行数：{column}，赋值：{self.__data.iloc[column,row]}')
        else:
            self.__listlog.append(f'取油：{df.iloc[column,row]}，坐标：（{column},{row}）')
            return (self.__data.iloc[column,row], self.__data.iloc[column,row], (column,row))

    def __gettophang(self,r,c,colgnd_3,gnd6,res_file,filenane):
        # 1、调用接地方法，先取接地油
        self.__getbottomhang(r,c,colgnd_3,gnd6,res_file,filenane)
        # 2、离地获取，方法如下
        # 3、找到离地标志，再向下扫描100次离地行，
        lvzw,mm,topoil = [],100,[]
        # 3.1、增加800机型空中节油处理列表
        airOil = []
        for j in range(r):
            m = 0
            for i in range(c):
                # if self.df_value.iloc[j,i] == 'AIR':
                if self.df_value.iloc[j,i] in ['AIR','0']:
                    m += 1
                    if m == c:
                        # print('大吕苍鹭飞1',j,m)
                        if j == r:
                            self.__error += '文件头异常，第一条就是AIR'
                            self.__data[j,'coment'] = self.__error
                        lvzw.append(j)
            if len(lvzw) >= mm:
                break
        if not lvzw:
            self.__listlog.append('未取到全AIR行')
        # 4、取最大连续数的最小值
        self.__top = min(self.__justnumber(lvzw))
        # 5、对取值的判断
        if self.__top == 2:
            # self.__data['coment'][self.__top] += '第一条就是AIR'
            self.__data.loc[self.__data['flag']==self.__top,'coment'] += '第一条就是AIR'
            self.__listlog.append('文件头异常，开始就是AIR')
        elif self.__top == r:
            self.__listlog.append('文件异常，从头到尾无全AIR行')
            # print('阿珍！',self.__data['TAT'][0],'阿强：',self.__data['TAT'][self.__top],self.__data['TAT'][:self.__top+3])
        # 6、存储离地行到文件中
        self.__pdtofile(self.__data.iloc[self.__top],res_file)
        maxgn3 = colgnd_3+3
        print(f'取上油前-lvzw：{self.__top}，{colgnd_3}','老吕大大1：\n',self.__data.iloc[self.__top,:],'老吕大大2：\n',self.__data.iloc[self.__top,colgnd_3])
        # 7、离地油、全重、总油获取算法，文件结构因机型而异，取法有差别，gnd_3：300机型、gnd_6：800机型、gnd_7：777机型，具体如下：
        if gnd6 == 'gnd_3':
            # 8、300机型，有总油，只取总油就可以
            for i in range(colgnd_3,maxgn3):
                print('GND3：',self.__data.iloc[self.__top,i])
                # 9、第一条全离地时间的数据：起飞油量(左)、起飞油量(中)、起飞油量(右)、起飞总油量
                val=self.__getoil(self.__top, i,'top')
                topoil.append(val)
                if i == colgnd_3:
                    # 10、空中总重(取2-3万英尺的数据)-全AIR之后，向下数20行取值
                    self.__data.iloc[self.__top, i-1] = self.__getFullweight(self.__top, i)
                    self.__listlog.append(f'全重：{self.__data.iloc[self.__top, i-1]}')
                    print('大吕苍鹭飞2',self.__data.iloc[self.__top, i-1])
                elif i == colgnd_3+1: # 总油
                    # 11、空中油量(取2-3万英尺的数据)-全AIR之后，向下数20行取值
                    self.__data.iloc[self.__top, i-3] = self.__getFullweight(self.__top, i)
                    self.__listlog.append(f'总油：{self.__data.iloc[self.__top, i-3]}')
        elif gnd6 == 'gnd_7':
            # 777机型，有总油、左、右、中，逻辑上取总油就可以，但其余数据也需要正确
            maxgn3 += 1
            lv = 0
            for i in range(colgnd_3,maxgn3):
                if i == colgnd_3:
                    # 12、空中总重(取2-3万英尺的数据)-全AIR之后，向下数20行取值
                    print(f'全重哈哈哈：{i}',self.__data.iloc[self.__top, i-3:])
                    self.__data.iloc[self.__top, i-3] = self.__getFullweight(self.__top, i-23) # 全重
                    self.__listlog.append(f'全重：{self.__data.iloc[self.__top, i-3]}')
                    print('777-lvzw-1-全重：',self.__data.iloc[self.__top, i-3])
                elif i == colgnd_3+1: # 总油
                    # 13、空中油量(取2-3万英尺的数据)-全AIR之后，向下数20行取值
                    self.__data.iloc[self.__top, i-3] = self.__getFullweight(self.__top, i) # 空中油量
                    self.__listlog.append(f'总油：{self.__data.iloc[self.__top, i-3]}')
                    print('777-lvzw-2-总油：',self.__data.iloc[self.__top, i-3])
                print(f'第{lv}次执行-lvzw:{i}')
                # 14、第一条全离地时间的数据：起飞油量(左)、起飞油量(中)、起飞油量(右)、起飞总油量
                val=self.__getoil(self.__top, i,'top')
                print('777-lvzw-3-油',val)
                topoil.append(val)
                lv += 1
        elif gnd6 == 'gnd_6':
            # 15、800机型，无总油，需要左、右、中三个油箱相加，其大值=空中油量
            k = 0
            for j in range(colgnd_3,maxgn3):#57,60
                k += 1
                if j == colgnd_3:
                    i = colgnd_3+1
                    # 16、空中油量(取2-3万英尺的数据)-全AIR之后，向下数20行取值 #
                    self.__data.iloc[self.__top, i-2] = self.__getFullweight(self.__top, i) # 空中油量 20240313
                    airOil.append((self.__top, i-2))
                    self.__listlog.append(f'总油：{self.__data.iloc[self.__top, i-2]}') # 空中油量
                    # 17、空中总重(取2-3万英尺的数据)-全AIR之后，向下数20行取值
                    self.__data.iloc[self.__top, i-3] = self.__getFullweight(self.__top, i-29) # 全重
                    self.__listlog.append(f'全重：{self.__data.iloc[self.__top, i-3]}') # 全重
                    print(f'大吕耀云天-老吕{self.__data.iloc[self.__top, i-1]}' # 主油箱
                          f'-{self.__data.iloc[self.__top, i-2]}' # AIR
                          f'-{self.__data.iloc[self.__top, i-3]}' # AIR
                          f':::{self.__top}:{i}下油lvzw---》\n哈了一只狗：{self.__data.iloc[self.__top, i-2]},\n再哈一只狗狗：{self.__getFullweight(self.__top, i)}'
                          f'\n-{self.__data.iloc[self.__top, i-41]}：' # AIR 左起落架
                          f'{self.__data.iloc[self.__top, i-42]}：' # 总重 f'-----{self.__data.iloc[:, i-2]}：' # GROUND f'-----{self.__data.iloc[:, i-3]}：' # GROUND
                          f'{self.__data.iloc[self.__top, i-43]}') # 经度
                    print(f'{self.__data.iloc[self.__top, i-4]}大吕耀云天GND6:' # AIR
                          f'{self.__data.iloc[self.__top, i-3]}-' # AIR
                          f'{self.__top}:{i}下油：'
                          f'{self.__data.iloc[self.__top, i-42]}，\n:KKKK:',i) # 56.6 # 全重 f'{self.__data.iloc[:, i-43:i-39]}：')
                # 18、第一条全离地时间的数据：起飞油量(左)、起飞油量(中)、起飞油量(右)、起飞总油量
                val = self.__getoil(self.__top, j,'top',gnd6)
                airOil.append(*self.__getairoil800(self.__top, j+1)) # 空中油量与离地油一样逻辑取值-lvzw
                print('大吕神腰展-值与坐标:',val,self.__top, j,type(val),'\nlvzw:',airOil,f'\nJJJJ-{i}-JJJJJJ-{j}-JJJ{j+k}:',j,colgnd_3)
                if not val:
                    val = ('0','0',(self.__top, j))
                topoil.append(val)
            # print('取上油:',self.__top,j,self.__data.iloc[self.__top,j],colgnd_3)
        return topoil,airOil

    def __getbottomhang(self,r,c,colgnd_3,gnd6,res_file,filenane):
        print('接地油取法：',colgnd_3)
        # 1、找到接地标志，再向下扫描100次离地行，
        lvzw,mm = [],100
        for i in range(r-1,1,-1):
            m = 0
            for j in range(c):
                # if self.df_value.iloc[i,j] == 'AIR':
                if self.df_value.iloc[i,j] in ['AIR','0']:
                    m += 1
                    # print('改变值了哈：',type(self.df_value.iloc[i,j]),'\n',self.df_value.iloc[i,:],r,i,j,c,self.df_value.columns)
                    if m > 0:
                        if i not in lvzw:
                            lvzw.append(i)
            if len(lvzw) >= mm:
                break
        if not lvzw:
            list_taget=[]
            list_taget.append(('文件头异常:',self.__filename))
            self.__tocsv(filenane, list_taget)
            self.__listlog.append(f'文件头异常{self.__filename}')
            return
        if len(lvzw) == 1:
            k = lvzw[0]
        # 2、取最小连续数的最大值
        else:k = max(self.__justnumber(lvzw)) + 1
        if k == 0:
            self.__error = '文件头异常，第一条既是AIR'
            self.__listlog.append('文件头异常，第一条既是AIR')
        elif k == r:
            k -= 1
            self.__data['coment'] += '最后一条还是AIR：'
            self.__listlog.append('最后一条还是AIR')
        self.__pdtofile(self.__data.iloc[k],res_file)
        maxgn3 = colgnd_3+3
        # 3、300、800机型 3个油箱，向下取值
        if gnd6 in ['gnd_3','gnd_6']:
            for i in range(colgnd_3,maxgn3): #gnd_3 10,13 gnd_6 57,60
                self.__getoil(k, i,'bott')
        elif gnd6 == 'gnd_7':
            # 4、777机型四个油箱，向下取值
            maxgn3 += 1 # print('取下油前：',self.__data.iloc[k,:],'\n',maxgn3)
            for i in range(colgnd_3,maxgn3): #57,60
                self.__getoil(k, i,'bott') # print('取下油后：',self.__data.iloc[k,:],'\n',maxgn3)
    def __toracle(self,db,cfg,tablename):
        dba = db(cfg)
        cnt = dba.put_pd_datas(self.__data,table_name=tablename)
        self.__listlog.append(f'文件正常加载入库，表：{tablename}，数据量：{cnt}',)

    def __tomysql(self,db,cfg,sql):
        dba = db(cfg)
        data = self.__data.where(self.__data.notnull(), 'nan')
        df1 = [tuple(_) for _ in data.values]
        dba.create_cursor()
        # self.__db.updata_db(sql.sql0)
        dba.execute_many_data(sql, df1)
        dba.db_close()
    def __tocsv(self,filenane,list_taget):
        if not os.path.exists(filenane):
            open(filenane,'w').close()
        with open(filenane,'a+',encoding='utf8',newline='') as f:
            write = csv.writer(f,dialect='excel')
            write.writerow(list_taget)

    def __pdtofile(self, df,res_file):
        with open(res_file,'w+',encoding='utf8',newline='') as f:
            df.to_csv(f, sep='\t', index=False)

    def __getfile(self,columns,db,ora,cfg,hist,tablename,gnd6,res_file,filenane,backpath,tab_log):
        '''
        文件开始处理，参数意义与主程序一致，获取待解析文件
        :param columns:为解析文件添加列名
        :param db:数据库接口
        :param ora:0之外的数字 Oracle  0 mysql
        :param cfg:数据库连接串
        :param hist:文件解析后存储目录
        :param tablename:结果表
        :param gnd6:选择300、800、777执行入口
        :param res_file:存储结果文件
        :param filenane:待解析文件名称
        :param backpath:文件解析后存储目录路径
        :param tab_log:文件解析加载后存储日志
        :return:
        '''
        donelist,newlist = [],[]
        fullpath = os.path.join(self.__filepath,gnd6)
        fullpath1 = os.path.join(fullpath,hist)
        currenttime = 'Donefile_'+pd.to_datetime('now').strftime('%Y%m%d')
        filenane1 = os.path.join(fullpath1,currenttime)
        # if filenane:
        #     with open(filenane,'r',newline='') as f:
        #         h = csv.reader(f,dialect='excel',)
        # 0、穷尽式遍历文件夹，读取文件
        for root, dirs, files in os.walk(fullpath):
            for file in files:
                self.__filename = os.path.join(fullpath,file)
                self.__listlog.append(file)
                print('处理的文件名为:',file,'\n编码格式：',self.__file_code())
                try:
                    # 1、解析文件，获取离地、接地油，全重，总油
                    self.__getfiledata_gndfile(columns,gnd6,res_file,filenane)
                    # 2、解析文件，获取机号、航班号、起落机场，起始时间等
                    self.__getfilenameheader(gnd6)
                    # 3、comment 字段增加解析的文件名字
                    if 'coment' in self.__data.columns:self.__data['coment'] += str(file)
                    else:self.__data['coment'] = str(file)
                    self.__pdtofile(self.__data,res_file)
                    # 4、成功解析后的文件存库
                    # 5、传输参数 0 选择mysql数据库
                    if ora == 0:
                        # 5.1、mysql数据库
                        self.__tomysql(db,cfg,tablename)
                    else:
                        # 5.2、传输 0 之外的参数选择 oracle
                        self.__toracle(db,cfg,tablename)
                    # 6、文件名存储到列表中
                    donelist.append(self.__filename)
                except Exception as e:
                    # 7、异常处理后文件移入异常文件夹
                    list_taget = []
                    list_taget.append((e, '文件异常:', self.__filename))
                    path1 = os.path.join(backpath, 'bad_file')
                    path_1 = os.path.join(path1, gnd6)
                    # path2 = os.path.join(path_1, datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + file)
                    path2 = os.path.join(path_1, file)
                    try:
                        # 7.1、文件移动到备份文件夹
                        os.rename(self.__filename, path2)
                    except Exception as e:
                        print('不移动就留着罢，反正不影响我后续')
                    self.__tocsv(filenane, list_taget)
                else:
                    # 8、正常处理后文件移入备份目录
                    path1 = os.path.join(backpath, gnd6)
                    # path2 = os.path.join(path1, datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + file)
                    path2 = os.path.join(path1, file)
                    try:
                        os.rename(self.__filename, path2)  # 文件移动到备份文件夹
                    except Exception as e:
                        print('不移动就留着罢，反正不影响我后续')
                finally:
                    print(self.__listlog)
                    # 9、不管是否能够解析，结束后写日志
                    log_dict = {}
                    log_dict['FILE_NAME'] = self.__listlog[0]
                    log_dict['DEAL_DATE'] = self.__listlog[1]
                    log_dict['FILE_STATUS'] = self.__listlog[2]
                    log_dict['REASON'] = ''
                    for i in self.__listlog[3:]:
                        if 'AIR' in i or '加载入库' in i:
                            log_dict['REASON'] += i
                    log_dict['REMARKS'] = ','.join(self.__listlog[3:])
                    print('字典:',log_dict)
                    df = pd.DataFrame(log_dict,index=[0])
                    print('表格:',df)
                    for table_log,extra_update_fields in tab_log.items():
                        primary_key_column = extra_update_fields[0]
                        dba = db(cfg)
                        # 日志入库
                        dba.put_pd_datas(df,table_name=table_log)
                    self.__listlog = []
                    self.__top = 0
                    self.__bottom = 0
                # exit('777 调试')
        # self.__tocsv(filenane,donelist)
    def __getheaderline(self):
        '''
        文件头解析,只解析前两行
        :return:列表形式返回文件前两行
        '''
        row = []
        with open(self.__filename,'r',) as f:
            r = csv.reader(f)
            for i,rw in enumerate(r):
                if i < 3:
                    row.append(rw)
                else:
                    break
        return row
    def __justnumber(self,list_target):
        '''
        反转后取连续的数，返回第一次连续数的值
        :param list_target:
        :return:
        '''
        k1,k2 = [],[]
        lst = map(int,list_target[::-1])
        for i in lst:
            if not k1:
                k1.append(i)
            else:
                if abs(k1[-1]-i)== 1:
                    k1.append(i)
                else:
                    k2.append(i)
        return k1

    def __timeutc(self,st='03/01/2020 06:09 PM'):
        '''
        参数 st:带转换时间 str
        return new :转换后的时间 str
        '''
        tl = st.split(' ')
        if tl[-1] == 'PM':
            hm = tl[-2].split(':')
            h = int(hm[0]) + 12
            m = hm[1]
            s = hm[-1]
        else:
            hm = tl[-2].split(':')
            h = hm[0]
            m = hm[1]
            s = hm[-1]
        y = tl[0].split('/')[2]
        mo = tl[0].split('/')[0]
        d = tl[0].split('/')[1]
        old = datetime.datetime(int(y), int(mo), int(d), int(h), int(m),int(s))  # 转换为datetime格式
        new = old + datetime.timedelta(hours=8) #时差5
        new = new.strftime('%Y-%m-%d %H:%M:%S') # 格式化输出
        # print(new)
        return new
    def __getfilenameheader(self,gnd6):
        '''
        返回文件头信息，并解析到对应字段，存储数据库
        :param gnd6:
        :return:
        '''
        # 1、获取文件前两行
        row = self.__getheaderline()
        temp1=temp2=[]
        REMARKS=ARRIVAL_ABBR=''
        DEPARTURE_AIRPORT=REG_CODE=''
        FLIGHT_NO=ACT_DATE=''
        if gnd6 in ['gnd_6','gnd_7']:
            temp1 = ''.join(row[0]).split()[:]
            temp2 = ''.join(row[1]).split()[:]
            # 2、使用时间处理函数，入库前自动加8小时
            BEGIN_TIME = self.__timeutc(temp1[1] + ' ' + ' '.join(temp2[4:6]))
            END_TIME = self.__timeutc(temp1[1] + ' ' + ' '.join(temp2[8:10]))
            print('END_TIME:',END_TIME)
            REMARKS = ''.join(row[0])+''.join(row[1])
            ARRIVAL_ABBR = temp2[2].split('-')[-1]
            DEPARTURE_AIRPORT = temp2[2].split('-')[0]
            REG_CODE = temp1[0]
            FLIGHT_NO= 'I9'+''.join(list(temp2[1].split('-')[0])[3:])
            self.__data.insert(loc=0, column='REMARKS', value=REMARKS)
            self.__data.insert(loc=0, column='ARRIVAL_ABBR', value=ARRIVAL_ABBR)
            self.__data.insert(loc=0, column='DEPARTURE_AIRPORT', value=DEPARTURE_AIRPORT)
            self.__data.insert(loc=0, column='REG_CODE', value=REG_CODE)
            self.__data.insert(loc=0, column='FLIGHT_NO', value=FLIGHT_NO)
            self.__data.insert(loc=0, column='END_TIME', value=END_TIME)
            self.__data.insert(loc=0, column='BEGIN_TIME', value=BEGIN_TIME)
            print('BEGIN_TIME：', BEGIN_TIME)
            print('END_TIME：', END_TIME)
        elif gnd6 in ['gnd_3']:
            temp1 = row[0]
            temp2 = ''.join(row[1]).split()[:]
            REMARKS = ''.join(row[0]) + ''.join(row[1])
            ARRIVAL_ABBR = temp1[2].split(':')[-1].split('-')[-1]
            DEPARTURE_AIRPORT = temp1[2].split(':')[-1].split('-')[0]
            REG_CODE = temp1[0].split(':')[-1]
            ACT_DATE = temp1[1].split(':')[-1]
            FLIGHT_NO = ':'.join(temp1[3].split(':')[1:])
            print('航班号1：', FLIGHT_NO)
            self.__data.insert(loc=0, column='REMARKS', value=REMARKS)
            self.__data.insert(loc=0, column='ARRIVAL_ABBR', value=ARRIVAL_ABBR)
            self.__data.insert(loc=0, column='DEPARTURE_AIRPORT', value=DEPARTURE_AIRPORT)
            self.__data.insert(loc=0, column='REG_CODE', value=REG_CODE)
            self.__data.insert(loc=0, column='FLIGHT_NO', value=FLIGHT_NO)
            self.__data.insert(loc=0, column='ACT_DATE', value=ACT_DATE)
        # aa = pd.to_datetime(self.__data['ACT_DATE'])
        print('机号：', REG_CODE)
        print('实飞日期：',ACT_DATE if ACT_DATE else BEGIN_TIME)
        print('航班号：', FLIGHT_NO)
        print('起飞机场三码：', DEPARTURE_AIRPORT)
        print('落地机场三码：', ARRIVAL_ABBR)
        print('备注：', REMARKS)
    def __getregcode(self,gnd6):
        '''
        解析并返回机号
        :param gnd6:
        :return:
        '''
        row = self.__getheaderline()
        reg_code=temp1=''
        if gnd6 == 'gnd_6':
            temp1 = ''.join(row[0]).split()[:]
            reg_code = temp1[0]
        elif gnd6 == 'gnd_3':
            temp1 = row[0]
            reg_code = temp1[0].split(':')[-1]
        return reg_code
    def main(self,columns,db,ora,cfg,hist,tablename,gnd6,res_file,filenane,backpath,tab_log):
        '''
        程序入口
        :param columns: 文件列名
        :param db: 数据库连接
        :param ora: 连接串
        :param cfg: 选择Oracle、mysql
        :param hist: 执行文件存储路径
        :param tablename: 加载文件存储表
        :param gnd6: 文件解析类型
        :param res_file: 记录文件
        :param filenane: 错误记录文件
        :param backpath: 备份目录
        :param tab_log: 文件加载日志表
        :return:
        '''
        self.__getfile(columns,db,ora,cfg,hist,tablename,gnd6,res_file,filenane,backpath,tab_log)

if __name__=='__main__':
    gnds = ['gnd_3','gnd_6','gnd_7']
    backpath = r'D:\工作日记\飞机节油项目\QAR数据处理\hist'
    filepath = r'D:\工作日记\飞机节油项目\QAR数据处理' # 'D:\工作日记\QAR数据处理\gnd_6\TEST.csv'
    # columns6 = ['aframesf','asfcount','atime','a_altitude','a_cas','a_gs','a_n1_1','a_n1_2','a_mach','aatat','a_wind_spd','a_windir','a_windlong','a_latitude','a_longitude','a_gross_weight','a_ldg_lh','a_ldg_lh1','a_ldg_lh2','a_ldg_lh3','a_ldg_rh','a_ldg_rh1','a_ldg_rh2','a_ldg_rh3','a_ldg_nose','a_ldg_nose1','a_ldg_nose2','a_ldg_nose3','aflthlpa','aflthlpb','ahoqa1','ahoqb1','ahsa1','ahsb','ahsb2','ahsea','ahssy','asbhydopr','aavcn11','aavcn12','aan21','aan22','alsvo1','arsvo1','acfq1','alfq1','arfq1','aavcn21','aavcn22','aavtn11','aavtn12','aavtn21','aavtn22']
    columns6 = ['AFRAMESF','ASFCOUNT','ATIME','A_ALTITUDE','A_CAS','A_GS','A_N1_1','A_N1_2','A_MACH','AATAT','A_WIND_SPD','A_WINDIR','A_WINDLONG','A_LATITUDE','A_LONGITUDE','A_GROSS_WEIGHT','A_LDG_LH','A_LDG_LH1','A_LDG_LH2','A_LDG_LH3','A_LDG_RH','A_LDG_RH1','A_LDG_RH2','A_LDG_RH3','A_LDG_NOSE','A_LDG_NOSE1','A_LDG_NOSE2','A_LDG_NOSE3','AFLTHLPA','AFLTHLPB','AHOQA1','AHOQB1','AHSA1','AHSB','AHSB2','AHSEA','AHSSY','ASBHYDOPR','AAVCN11','AAVCN12','AAN21','AAN22','ALSVO1','ARSVO1','ACFQ1','ALFQ1','ARFQ1','AAVCN21','AAVCN22','AAVTN11','AAVTN12','AAVTN21','AAVTN22']
    columns3 = ['ID','FDAY','AID','FFNO','FNO','FDEP','FDEST','AGND','AGNDN','GWT','FQT','FFL','FFR','ALT','TAT','WS','WDTIRU','N1L','N1R','CAS','MACH','ATN1','LON','LAT']
    columns7 = ['FRAME_SF', 'SFCOUNT', 'LEAD_TIME', 'L_ALTITUDE', 'L_CAS', 'L_PITCH1', 'L_PITCH2', 'L_PITCH3',
                'L_PITCH4', 'L_PITCH5', 'L_TAT', 'L_GROSS_WEIGHT', 'L_WIND_SPD', 'L_WINDIR', 'L_WINDIR_LINEAR',
                'L_LATITUDE', 'L_LONGITUDE', 'L_ALT_RADIO', 'L_N1_1', 'L_N1_2', 'L_LDG_NOSE1', 'L_LDG_NOSE2',
                'L_LDG_NOSE3', 'L_LDG_NOSE4', 'L_LDG_NOSE5', 'L_LDG_RH1', 'L_LDG_RH2', 'L_LDG_RH3', 'L_LDG_RH4',
                'L_LDG_RH5', 'L_LDG_LH1', 'L_LDG_LH2', 'L_LDG_LH3', 'L_LDG_LH4', 'L_LDG_LH5', 'TFQ', 'FUWTC', 'FUWTML',
                'FUWTMR', 'L_AIR_GROUND']
    res_file = r'excel2txt.txt'
    db_list = ['mysqldb.Database','myoradb.OracleDatabase']
    filenane=r'errfile.txt'
    mysql_info = mysql.get_db(db='local')
    hist = 'hist'
    for gnd6 in gnds:
        db_info = oracfg.get_db(db='jieyou')
        b1 = Dealqardata(res_file, filepath)
        if gnd6 == 'gnd_6':
            b1.main(columns6, oradb.OracleDatabase,1, db_info,hist,sql.td_flight_signals_gn800,gnd6,res_file,filenane,backpath,sql.td_qar_oil_log) # 0 mysql 1 oracle
        elif gnd6 == 'gnd_7':
            b1.main(columns7, oradb.OracleDatabase,1, db_info,hist,sql.td_flight_signals_gn7,gnd6,res_file,filenane,backpath,sql.td_qar_oil_log) # 0 mysql 1 oracle
            # b1.main(columns6, oradb.OracleDatabase,1, db_info,hist,sql.table_signalgn6,gnd6,res_file,filenane,backpath) # 0 mysql 1 oracle
            # b1.main(columns6,mysqldb.Database,0,mysql_info,hist,sql.insert_signalgn6)
        elif gnd6 == 'gnd_3':
            b1.main(columns3, oradb.OracleDatabase,1, db_info,hist,sql.table_signalgn3,gnd6,res_file,filenane,backpath,sql.td_qar_oil_log) # 0 mysql 1 oracle
