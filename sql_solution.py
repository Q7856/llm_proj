import data_paths
import pandas as pd
import difflib
from langchain_community.utilities import SQLDatabase
import sqlite3
import re
import threading
import sql_metadata


db_info = {
    'A股公司行业划分表': 
'''
字段 类型
股票代码 TEXT 
交易日期 TEXT
行业划分标准 TEXT
一级行业名称 TEXT
二级行业名称 TEXT
''',
'A股票日行情表': 
'''
字段 类型
股票代码 TEXT
交易日 TEXT
[昨收盘(元)] REAL
[今开盘(元)] REAL
[最高价(元)] REAL
[最低价(元)] REAL
[收盘价(元)] REAL
[成交量(股)] REAL
[成交金额(元)] REAL
''',
'基金份额持有人结构':
'''
字段 类型
基金代码 TEXT
基金简称 TEXT
公告日期 TIMESTAMP
截止日期 TIMESTAMP
机构投资者持有的基金份额 REAL
机构投资者持有的基金份额占总份额比例 REAL
个人投资者持有的基金份额 REAL
个人投资者持有的基金份额占总份额比例 REAL
定期报告所属年度 INTEGER
报告类型 TEXT
''',
'基金债券持仓明细':
'''
字段 类型
基金代码 TEXT
基金简称 TEXT
持仓日期 TEXT
债券类型 TEXT
债券名称 TEXT
持债数量 REAL
持债市值 REAL
持债市值占基金资产净值比 REAL
第N大重仓股 INTEGER
所在证券市场 TEXT
[所属国家(地区)] TEXT
报告类型TEXT TEXT
''',
'基金可转债持仓明细':
'''
字段 类型
基金代码 TEXT
基金简称 TEXT
持仓日期 TEXT
对应股票代码 TEXT
债券名称 TEXT
数量 REAL
市值 REAL
市值占基金资产净值比 REAL
第N大重仓股 INTEGER
所在证券市场 TEXT
[所属国家(地区)] TEXT
报告类型 TEXT
''',
'基金基本信息':
'''
字段 类型
基金代码 TEXT
基金全称 TEXT
基金简称 TEXT
管理人 TEXT
托管人 TEXT
基金类型 TEXT
成立日期 TEXT
到期日期 TEXT
管理费率 TEXT
托管费率 TEXT
''',
'基金日行情表':
'''
字段 类型
基金代码 TEXT
交易日期 TEXT
单位净值 REAL
复权单位净值 REAL
累计单位净值 REAL
资产净值 REAL
''',
'基金股票持仓明细':
'''
字段 类型
基金代码 TEXT
基金简称 TEXT
持仓日期 TEXT
股票代码 TEXT
股票名称 TEXT
数量 REAL
市值 REAL
市值占基金资产净值比 REAL
第N大重仓股 INTEGER
所在证券市场 TEXT
[所属国家(地区)] TEXT
报告类型 TEXT
''',
'基金规模变动表':
'''
字段 类型
基金代码 TEXT
基金简称 TEXT
公告日期 TIMESTAMP
截止日期 TIMESTAMP
报告期期初基金总份额 REAL
报告期基金总申购份额 REAL
报告期基金总赎回份额 REAL
报告期期末基金总份额 REAL
定期报告所属年度 INTEGER
报告类型 TEXT
''',
'港股票日行情表':
'''
字段 类型
股票代码 TEXT
交易日 TEXT
[昨收盘(元)] REAL
[今开盘(元)] REAL
[最高价(元)] REAL
[最低价(元)] REAL
[收盘价(元)] REAL
[成交量(股)] REAL
[成交金额(元)] REAL
'''
}

db = SQLDatabase.from_uri("sqlite:///"+data_paths.bojin_paths)
conn = sqlite3.connect


def handle_err(e,last_response,question,sample,num):
    if num == 1:
        return '''你是一名高级SQL工程师，请你根据我提供的用户问题，生成sql语句，数据库为sqlite，你生成的sql语句格式必须符合sqlite格式。
        请只使用以下表格的表格名和列名生成SQL语句
        SQL表格信息是：
        {}
        请直接生成能在以下数据库中执行成功的SQL代码，不要有其他解释，
        问题：`{}`，SQL语句:'''.format(db_info,question)

    err_prompt = '''你是一个擅长将人类提问的问题转成SQL语句的AI，根据下面的错误提示和人类提问的问题修改一个SQL语句,
    错误是：`{}`，
    人类提问的问题是:`{}`。
    需要修改的SQL语句是:`{}`。
    请只使用以下表格的表格名和列名生成SQL语句,SQL表格信息是：`{}`.
    请直接生成能在以下数据库中执行成功的SQL代码，不要有其他解释，SQL语句:'''.format(e,question,last_response,db_info)

    try:
        sql = sql_metadata.Parser(last_response)
        table_info = db.get_table_info(sql.tables)
    except:
        return err_prompt

    if 'syntax error' in e:
        err_prompt = '''你回答的sql语法中有错误，你的上一次回答是：`{}`，错误是:`{}`。
        注意检查SQL语法，请你修改好后再次回答。
        请直接生成能在以下数据库中执行成功的SQL代码，现在的问题是：`{}`，请编写SQL语句：'''.format(last_response,e,question)
    
    if 'no such column' in e:
        err_prompt = '''你的上一次回答 `{}` 有错误，错误是：`{}`。
        注意检查SQL语法，只能用下面的表名和列名生成SQL语句：`{}`,
        请直接生成能在以下数据库中执行成功的SQL代码，现在的问题是：`{}`，
        请编写SQL语句：  '''.format(last_response,e,table_info,question)

    return err_prompt

def get_ans_db(question,sample:dict):

    def query_database(sql,event):
        try:
            conn = sqlite3.connect(data_paths.bojin_paths)
            cursor = conn.cursor()
            cursor.execute(sql)

            columns = [col[0] for col in cursor.description] #字段名
            results = cursor.fetchall()

            result_list = []
            for result in results:
                result_dict = {}
                for index,val in enumerate(result):
                    result_dict[columns[index]] = val
                result_list.append(result_dict)
            
            final_result['results'] = result_list # {"results":[{...},{...},...]}
        
        except sqlite3.OperationalError as e:
            final_result['err'] = 'SQLite err: {}'.format(e) #{"err":'SQLi...'}

        finally:
            event.set()
            conn.close()

    def execute_sql(sql):
        '''sql = "select...."'''
        event = threading.Event()

        thread = threading.Thread(target=query_database,args=(sql,event))
        thread.start()

        thread.join(120)
        if thread.is_alive():
            raise ValueError('SQL execute timeout')
        else:
            if "err" in final_result:
                results = final_result.get("err",[])
                raise ValueError(results)
            else:
                results = final_result.get("results",[])
            return results


    same = difflib.get_close_matches(question['question'],sample.keys(),n = 2,cutoff = 0.2)  #从sample中找出和question最相似的两个问题
    sample_ques = '\n'.join([f'{i}.问题:\'{v}\',SQL语句:\'{sample[v]}\'。' for i,v in enumerate(same)])

    if sample_ques == '':
        prompt = '''你是一名高级SQL工程师，请你根据我提供的用户问题，生成sql语句，数据库为sqlite，你生成的sql语句格式必须符合sqlite格式。
        请只使用以下表格的表格名和列名生成SQL语句
        SQL表格信息是：
        {}
        请直接生成能在以下数据库中执行成功的SQL代码，不要有其他解释，
        问题：`{}`，SQL语句:'''.format(db_info,question['question'])
        
        response = 'haven\'t'
    
    else:
        prompt = '''你是一名高级SQL工程师，请你根据我提供的用户问题，从给定的2个问题转SQL例子中选择最相似的一个，然后模仿SQl语句修改，生成sql语句，数据库为sqlite，你生成的sql语句格式必须符合sqlite格式。
        请只使用以下表格的表格名和列名生成SQL语句，
        SQL表格信息是：
        {}
        给定的问题转SQL例子：
        {}
        请直接生成能在以下数据库中执行成功的SQL代码，不要有其他解释，
        问题：`{}`，SQL语句:'''.format(db_info,sample_ques,question['question'])
        
        response = 'haven\'t'
    
    question['model_return_sql'] = response #question = {'question':"....",'model_return_sql':SQL语句}
        
    sql_err = None
    times = 0
    while True:
        if times > 3:
            question['sql_err'] = sql_err
            break
        try:
            if sql_err:
                err_prompt = handle_err(sql_err,response,question['question'],sample_ques,times)
                if times == 1:
                    response = 'haven\'t'
                else:
                    response = 'haven\'t'
                question['model_return_sql'] = response
            sql = re.findall(r'.*?(SELECT .*?)(?:`|$|。|;)', response, re.DOTALL) #sql = ['SELECT ....']
            final_result = {}
            result = execute_sql(sql[0])
            question['sql_return'] = result
            prompt = '''请把问题和答案组成一个完整的答案，回答要简洁和完整，
                例如："景顺长城中短债债券C基金在20210331的季报里，前三大持仓占比的债券名称是什么?"，需要回答："景顺长城中短债债券C在20210331的季报中，前三大持仓占比的债券名称分别是21国开01、20农发清发01、20国信03。"。
                现在问题是<{}>,答案是<{}>,请回复:
            '''
            response = 'haven\'t'
            question['answer'] = response
        except Exception as e:
            sql_err = str(e)
            times += 1

    return question
    '''
    1)#question = {'question':"....",'model_return_sql':SQL语句,'sql_return':SQL语句找回来的答案,'answer'：经过整理的答案}
    2)#question = {'question':"....",'model_return_sql':SQL语句,'sql_err':....}
    '''
    
def s1(question):
    '''
    init question = {'question':".........."}
    '''
    train_sql = pd.read_excel('train_sql.xlsx')
    train_sql_dict = train_sql.to_dict('records')
    train_data = {} #{'question':'sql',......}
    for tq in train_sql_dict:
        train_data[tq['question']]  = tq['sql']
    ans = get_ans_db(question,train_data)

