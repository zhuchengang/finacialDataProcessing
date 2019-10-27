#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
@author:ZCG
@file:FinacialDataProcess.py
@time:2019/10/16
"""
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy

conn_string='oracle+cx_oracle://rep:rep@172.30.170.4:1521/jsbi1'
engine = create_engine(conn_string, echo=False)

#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',200)


#dir = 'C:\\财务报表\\201901发展公司'
dir = 'C:\\财务报表'

for root, dirs , files in os.walk(dir):
    for name in files:
        path =  os.path.join(root,name)

#list = os.listdir(dir)

#for i in range(0,len(list)):
#    path = os.path.join(dir,list[i])

        print('current file is %s',path)

        if os.path.isfile(path) and os.path.basename(path).endswith('.xls'):
        #print('worksheet is %s', os.path.basename(path))
        #book = xlrd.open_workbook(path)
        #incomeSheet = book.sheet_by_name('收入明细表')
            s = os.path.basename(path).split('.')[0].split('-')

            company_name = s[1]
            data_cycle = s[2]


            print('正在处理 %s  %s 的文件'%(company_name,data_cycle))
        try:
            #导入收入明细表
            incomeDF = pd.read_excel(path,sheet_name='收入明细表',header=2)

            #增加公司，时间
            incomeDF.insert(0,'company_name',company_name)
            incomeDF.insert(1,'data_cycle',data_cycle)

            #修改列名
            incomeDF.columns = ['company_name','data_cycle','code','code_name','current_amount','total_amount','budget_amount','percentage']
            #incomeDF.apply(pd.to_numeric, errors='ignore')
            #print(incomeDF.dtypes)

            #删除不需要的列
            incomeDF.drop(['percentage'],axis=1)

            #去掉项目中的空格
            incomeDF['code_name'] = incomeDF['code_name'].str.strip()

            #print(incomeDF)
            #保存到oracle中
            incomeDF.to_sql(name='rep_income_detail',
                            con=engine,
                            if_exists='append',
                            index=False,
                            dtype={'company_name':sqlalchemy.types.NVARCHAR(length=255),
                                   'data_cycle': sqlalchemy.types.NVARCHAR(length=12),
                                   'code': sqlalchemy.types.NVARCHAR(length=20),
                                   'code_name': sqlalchemy.types.NVARCHAR(length=255),
                                   'current_amount': sqlalchemy.types.FLOAT,
                                   'total_amount': sqlalchemy.types.FLOAT,
                                   'budget_amount': sqlalchemy.types.FLOAT
                                   #'本年累计数占预算数比例': sqlalchemy.types.FLOAT
                                   })
        except BaseException as e:
            #print(e.msg)
            #print()
            print(' %s %s 的文件缺少“收入明细表”' % (company_name, data_cycle))


        try:
            #导入成本明细表
            costDF = pd.read_excel(path, sheet_name='成本明细表', header=1)
            costDF.insert(0, 'company_name', company_name)
            costDF.insert(1, 'data_cycle', data_cycle)

            # 修改列名
            costDF.columns = ['company_name', 'data_cycle', 'code', 'code_name', 'current_amount', 'total_amount',
                                'budget_amount', 'percentage']
            # incomeDF.apply(pd.to_numeric, errors='ignore')

            costDF.drop(['percentage'], axis=1)
            costDF['code_name'] = costDF['code_name'].str.strip()
            #print(costDF)

            costDF.to_sql(name='rep_cost_detail',
                            con=engine,
                            if_exists='append',
                            index=False,
                            dtype={'company_name': sqlalchemy.types.NVARCHAR(length=255),
                                   'data_cycle': sqlalchemy.types.NVARCHAR(length=12),
                                   'code': sqlalchemy.types.NVARCHAR(length=20),
                                   'code_name': sqlalchemy.types.NVARCHAR(length=255),
                                   'current_amount': sqlalchemy.types.FLOAT,
                                   'total_amount': sqlalchemy.types.FLOAT,
                                   'budget_amount': sqlalchemy.types.FLOAT
                                   # '本年累计数占预算数比例': sqlalchemy.types.FLOAT
                                   })
        except BaseException as e:
            print(' %s %s 的文件缺少“成本明细表”' % (company_name, data_cycle))

        #导入"经营指标分析"
        try:
            indexDF = pd.read_excel(path,sheet_name='经营指标分析',header=0)

            indexDF.insert(0, 'company_name', company_name)
            indexDF.insert(1, 'data_cycle', data_cycle)

            indexDF.columns = ['company_name','data_cycle','employee_number',
                               'pay_user_number','income','income_budget',
                               'income_ratio','main_income_ratio','income_per_employee',
                               'cost', 'cost_per_user', 'sales_cost','manage_cost',
                               'total_cost_per_user','profit' ,'profit_budget' ,'profit_ratio',
                               'profit_per_employee', 'profit_income_ratio','cost_profit_ratio',
                               'net_assets','profit_assets_ratio','debt','debt_asset_ratio']

            #print(indexDF)

            indexDF.to_sql(name='rep_business_indicators',
                            con=engine,
                            if_exists='append',
                            index=False,
                            dtype={'company_name': sqlalchemy.types.NVARCHAR(length=255),
                                   'data_cycle': sqlalchemy.types.NVARCHAR(length=12),
                                   'employee_number': sqlalchemy.types.INTEGER,
                                   'pay_user_number': sqlalchemy.types.FLOAT,
                                   'income': sqlalchemy.types.FLOAT,
                                   'income_budget': sqlalchemy.types.FLOAT,
                                   'income_ratio': sqlalchemy.types.FLOAT,
                                   'main_income_ratio': sqlalchemy.types.FLOAT,
                                   'income_per_employee': sqlalchemy.types.FLOAT,
                                   'cost': sqlalchemy.types.FLOAT,
                                   'cost_per_user': sqlalchemy.types.FLOAT,
                                   'sales_cost': sqlalchemy.types.FLOAT,
                                   'manage_cost': sqlalchemy.types.FLOAT,
                                   'total_cost_per_user': sqlalchemy.types.FLOAT,
                                   'profit': sqlalchemy.types.FLOAT,
                                   'profit_budget': sqlalchemy.types.FLOAT,
                                   'profit_ratio': sqlalchemy.types.FLOAT,
                                   'profit_per_employee': sqlalchemy.types.FLOAT,
                                   'profit_income_ratio': sqlalchemy.types.FLOAT,
                                   'cost_profit_ratio': sqlalchemy.types.FLOAT,
                                   'net_assets': sqlalchemy.types.FLOAT,
                                   'profit_assets_ratio': sqlalchemy.types.FLOAT,
                                   'debt': sqlalchemy.types.FLOAT,
                                   'debt_asset_ratio': sqlalchemy.types.FLOAT
                                   })
        except BaseException as e:
            print(' %s %s 的文件缺少“经营指标分析”' % (company_name, data_cycle))


if __name__ == "__main__":
    pass