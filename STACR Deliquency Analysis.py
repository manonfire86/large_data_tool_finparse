# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 12:11:29 2021

@author: santana
"""

import pandas as pd
import numpy as np
import os
import datetime as dt
import zipfile
import psutil


os.chdir(r'C:\Users\santana\Tolis Advisors LP\Technology - Shared\Python Tools - SOP, Scripts, Data\Large Data Tool Stacr')
colheaders = pd.read_excel('columncreatorfile.xlsx',sheet_name='col86')

svmem = psutil.virtual_memory()



compressionfiles = []
for i in sorted(os.listdir(os.getcwd()),reverse=True):
    if 'ru' in i:
        compressionfiles.append(i)

df_sample_mem= []
for i in compressionfiles:
    df_sample_mem.append(pd.read_csv(zipfile.ZipFile(i).open(zipfile.ZipFile(i).infolist()[0].filename),sep='|',header=None,skiprows= [0],nrows=10,index_col=False).memory_usage(index=True).sum())
    
#df_sample_mem.append(pd.read_csv(zipfile.ZipFile(i).open(zipfile.ZipFile(i).infolist()[0].filename),sep='|',header=None,skiprows= [0],nrows=10,names=colheaders,index_col=False).memory_usage(index=True).sum())
# =============================================================================
#excelwriter = pd.ExcelWriter('mapping.xlsx',engine='xlsxwriter')
#pd.read_csv(zipfile.ZipFile(i).open(zipfile.ZipFile(i).infolist()[0].filename),sep='|',header=None,skiprows= [0],nrows=1,names=colheaders,index_col=False).to_excel(excelwriter,'mapping',index=False)
#excelwriter.save()
#excelwriter.close()
# 
# =============================================================================
my_chunk = (1000000000 / sum(df_sample_mem))/10
my_chunk = int(my_chunk//1) # we get the integer part


raw_data = pd.read_csv(zipfile.ZipFile(compressionfiles[0]).open(zipfile.ZipFile(compressionfiles[0]).infolist()[0].filename),sep='|',header=None,skiprows= [0],names=colheaders.columns,index_col=False,iterator=True,chunksize=my_chunk)


df_result = pd.concat([chunk[(pd.to_numeric(chunk['Current Loan Delinquency Status'],errors='coerce',downcast='float') != 0)] for chunk in raw_data]) 
df_result['Current Loan Delinquency Status'] = df_result['Current Loan Delinquency Status'].astype('str')
df_result['Current Loan Delinquency Status'] = np.where(df_result['Current Loan Delinquency Status'].str.isdigit(),pd.to_numeric(df_result['Current Loan Delinquency Status'],errors='coerce',downcast='float'),df_result['Current Loan Delinquency Status'])
df_result = df_result[df_result['Current Actual UPB'] != 0]
# =============================================================================
# filter_criteria = ['13DN01',
# '13DN02',
# '14DN01',
# '14DN02',
# '14DN03',
# '14DN04',
# '14HQ01',
# '14HQ02',
# '14HQ03',
# '15DN01',
# '15DNA1',
# '15DNA2',
# '15DNA3',
# '15HQ01',
# '15HQ02',
# '15HQA1',
# '15HQA2',
# '16DNA1',
# '16DNA2',
# '16DNA3',
# '16DNA4',
# '16HQA1',
# '16HQA2',
# '16HQA3',
# '16HQA4',
# '17DNA1',
# '17DNA2',
# '17DNA3',
# '17HQA1',
# '17HQA2',
# '17HQA3',
# '18DNA1',
# '18DNA2']
# 
# df_result_trim = df_result[~df_result['Reference Pool Number'].isin(filter_criteria)]
# 
# =============================================================================

# =============================================================================
# df_result.head()
# df_result.shape
# df_result_trim.shape
# len(df_result['Loan Identifier'].unique())
# len(set(df_result['Reference Pool Number']))
# 
# =============================================================================

excelwriter = pd.ExcelWriter('STACR Deliquency DB (RU) '+dt.datetime.today().strftime('%m-%d-%Y')+'.xlsx',engine='xlsxwriter')
df_result.to_excel(excelwriter,'STACR DB',index=False)
excelwriter.save()
excelwriter.close()