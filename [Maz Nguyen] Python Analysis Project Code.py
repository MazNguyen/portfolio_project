#!/usr/bin/env python
# coding: utf-8

# # Configuration

# In[4]:


from matplotlib.ticker import PercentFormatter
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
import plotly.graph_objs as go
from plotly.offline import iplot, init_notebook_mode
init_notebook_mode(connected=True)
from pyspark.sql.types import *
import pyspark.sql.functions as f
import datetime
import numpy as np
import pandas as pd
import glob
import time
from datetime import date
import matplotlib.pyplot as plt
import json


# # Ex1

# In[5]:


a = """*
**
***
****
*****"""
# show_stars = int(input("Input number of rows: "))

def show_stars(rows):
    a = ''
    for i in range(0,rows):
        a += '*'
        print(a)


# In[6]:


show_stars(5)


# # Ex2

# In[7]:


states = [0.0]*100

# Gọi xác suất đến được tầng n là: state[n]

# Xác suất đến tầng 0 là: 
states[0] = 1.0


# In[8]:


# Giải theo Markov Chain

for i in range(100):
    tmp_states = [0]*100
    for i in range(100):
        tmp_states[i+1 if i < 99 else 99] += states[i]*19/36
        tmp_states[i+2 if i < 98 else 99] += states[i]*1/36
        tmp_states[i+3 if i < 97 else 99] += states[i]*1/36
        tmp_states[i+4 if i < 96 else 99] += states[i]*1/36
        tmp_states[i+5 if i < 95 else 99] += states[i]*1/36
        tmp_states[i+6 if i < 94 else 99] += states[i]*1/36
        tmp_states[i-1 if i > 0 else 0] += states[i]*1/3
    for i in range(100):
        states[i] = tmp_states[i]


# In[9]:


# Nếu đến được tầng 60 trở lên sẽ thắng nên xác suất thắng cuộc sẽ là:

sum = 0
for i in range(0, len(states[60:])):
    sum = sum + states[60:][i]
    
print("Xác suất thắng cuộc: ", sum)


# # Ex3

# ## Tổng hợp dữ liệu

# In[10]:


df_mci = sqlContext.createDataFrame(pd.read_csv("/home/hieunt14/Risk/Hayden/name/yob1880.txt", names=["name","sex", "number"])).withColumn("year", f.lit(1880))
for i in range(1881,2019):
    location_file = "";
    location_file += location_file + ("/home/hieunt14/Risk/Hayden/name/yob" + str(i)+ ".txt");
    df_mci = df_mci.union(sqlContext.createDataFrame(pd.read_csv(location_file, names=["name","sex", "number"])).withColumn("year", f.lit(i)))


# In[11]:


df_mci_pd = df_mci.toPandas()


# In[12]:


df_mci_pd.head(5)


# ## Total births by sex and year

# In[13]:


# Tính số lượng trẻ sinh ra theo giới tính và năm 
df_births = df_mci_pd.groupby(["year", "sex"], as_index=False)["number"].sum()


# In[14]:


fig, ax = plt.subplots(figsize=(14, 6))
ax.xaxis.set_major_locator(MultipleLocator(10))
ax.grid(which='major', color='#CCCCCC', linestyle='--')
ax.grid(which='minor', color='#CCCCCC', linestyle=':')

plt.plot( 'year', 'number', data = df_births[(df_births["sex"] == "M")], marker='o', markerfacecolor='blue',
         markersize=3, color='skyblue', linewidth=3, label= "Male")
plt.plot( 'year', 'number',data = df_births[(df_births["sex"] == "F")], marker='o', markerfacecolor='red',
         markersize=3, color='tomato', linewidth=3, label= "Female")
plt.legend()
plt.title('Total births by sex and year', fontsize = 14)
# plt.xticks(rotation=90)

Tỷ lệ Nam và Nữ có sự biến động theo từng thời kì, có thể chịu tác động của nhiều yếu tố (chiến tranh, sự bình đẳng trong giới tính, ...)
    - Trước 1930, số lượng Nữ giới luôn cao hơn Nam
    - Từ 1930 -> 1947, Nam và Nữ khá cân bằng nhau qua các năm
    - Từ 1947 trở đi thì xuất hiện tình trạng mất cân bằng khi Nam giới luôn nhiều hơn Nữ giới.
# ## Create subset include top 1000 popular names by year

# In[15]:


def percentage(group_by):
    number = group_by.number.astype(float)
    group_by['percen'] = number/number.sum()
    return group_by


# In[16]:


df_names = df_mci_pd.groupby(['year','sex']).apply(percentage)


# In[17]:


def get_top1000(group_by):
    return group_by.sort_index(by='number', ascending=False)[:1000]


# In[18]:


df_name_grouped = df_names.groupby(['year','sex'])


# In[19]:


df_name_grouped.head(5)


# In[20]:


df_name_grouped_top_1000 = df_name_grouped.apply(get_top1000).reset_index(drop=True).sort_values(["year", "percen"], ascending = [True, False]).reset_index()


# In[21]:


df_top_1000_male = df_name_grouped_top_1000[df_name_grouped_top_1000["sex"] == "M"]

df_top_1000_female = df_name_grouped_top_1000[df_name_grouped_top_1000["sex"] == "F"]


# In[22]:


df_top_1000_male.head(6)


# ## Total births by names: Philip, Harry, Elizabeth, Marilyn

# In[30]:


df_subset_name = df_mci_pd.pivot_table(values = 'number', index='year', columns = 'name', aggfunc="sum")[['John','Harry','Mary','Marilyn']]


# In[31]:


df_subset_name.head(5)


# In[32]:


# Vẽ chart
df_subset_name.plot(figsize=(12,10),grid=False, subplots=True, 
                 marker='o', markerfacecolor='blue', markersize=2, linewidth=2, title='Number of births per year')

Hầu hết 4 tên này chỉ phổ biến trong một giai đoạn (từ 1910 -> 1960), về sau thì số lượng trẻ em được đặt tên này không còn nhiều. 
    - Đối với tên Marilyn, biểu đồ cho thấy hình dạng của normal distribution, chỉ tập trung vào giai đoạn 1930 -> 1960
    - John, Harry, mary có đạt được 2 đỉnh vào khoảng 1920 và 1950
# In[35]:


df_name_compare = df_mci_pd.groupby(["year","name"], as_index=False)["number"].sum()


# In[37]:


# So sáng số lượng giữa các nhóm tên

fig, ax = plt.subplots(figsize=(14, 6))
ax.xaxis.set_major_locator(MultipleLocator(10))
ax.grid(which='major', color='#CCCCCC', linestyle='--')
ax.grid(which='minor', color='#CCCCCC', linestyle=':')

plt.plot( 'year', 'number', data = df_name_compare[df_name_compare["name"] == "John"], marker='o', markerfacecolor='blue',
         markersize=3, color='skyblue', linewidth=3, label= "John")
plt.plot( 'year', 'number', data = df_name_compare[df_name_compare["name"] == "Mary"], marker='o', markerfacecolor='green',
         markersize=3, color='lightgreen', linewidth=3, label= "Mary")
plt.plot( 'year', 'number', data = df_name_compare[df_name_compare["name"] == "Marilyn"], marker='o', markerfacecolor='grey',
         markersize=3, color='grey', linewidth=3, label= "Marilyn")
plt.plot( 'year', 'number', data = df_name_compare[df_name_compare["name"] == "Harry"], marker='o', markerfacecolor='red',
         markersize=3, color='tomato', linewidth=3, label= "Harry")
plt.legend()
plt.title('Total births by name and year', fontsize = 14)


# In[ ]:


Trong 4 loại tên này thì John và Mary là 2 tên chiếm tỷ trọng nhiều nhất và bùng nổ kéo dài một giai đoạn 1920 -> 1970


# ## Name diversity

# In[38]:


df_name_diversity = df_name_grouped_top_1000.pivot_table(values = 'percen', index='year', columns = 'sex', aggfunc="sum").reset_index()


# In[39]:


fig, ax = plt.subplots(figsize=(14, 6))
ax.set_ylim(0, 1.2)
ax.xaxis.set_major_locator(MultipleLocator(10))
ax.grid(which='major', color='#CCCCCC', linestyle='--')
ax.grid(which='minor', color='#CCCCCC', linestyle=':')

plt.plot( 'year', 'F', data = df_name_diversity[["year", "F"]], marker='o', markerfacecolor='blue',
         markersize=3, color='skyblue', linewidth=3, label= "Female")
plt.plot( 'year', 'M', data = df_name_diversity[["year", "M"]], marker='o', markerfacecolor='red',
         markersize=3, color='tomato', linewidth=3, label= "Male")
plt.legend()
plt.title('Name diversity trend', fontsize = 14)


# In[ ]:


Chọn mốc 1880 đạt 100%, ta thấy có sự giảm dần tỷ trọng đa dạng tên theo giới tính của top 1000 cái tên phổ biến. 
    - Đối với Nữ, có sự giảm nhẹ kể từ năm 1960 và Nam là từ 1970 trở đi. 
    - Từ 1980 cả Nam và Nữ đều giảm dần đều theo thời gian, đến 2018 rơi vào khoảng 82% cho Nam và 75% cho Nữ. Điều này cho thấy càng về sau,
    càng xuất hiện nhiều tên mới cho trẻ em hơn. 


# ## First letter of name

# In[40]:


# Extract first letter
df_first_letter = df_mci_pd[df_mci_pd["year"].isin([1900, 1960, 2018])]
df_first_letter["first_letter"] = df_first_letter["name"].astype(str).str[0]


# In[41]:


df_first_letter_pivot = df_first_letter.pivot_table(values = "number", index = "first_letter", columns = ["sex", "year"], aggfunc = "sum").reindex(columns=[1900,1960,2018], level='year')


# In[42]:


df_first_letter_percen = df_first_letter_pivot/df_first_letter_pivot.sum().astype(float)


# In[43]:


fig, axes = plt.subplots(2, 1, figsize=(16,11))
axes[0].set_ylim(0, 0.2)

df_first_letter_percen['M'].plot(kind='bar', rot=0, ax=axes[0], title='Male')
df_first_letter_percen['F'].plot(kind='bar', rot=0, ax=axes[1], title='Female')

axes[0].grid(which='major', color='#CCCCCC', linestyle='--')
axes[1].grid(which='major', color='#CCCCCC', linestyle='--')

Nhóm chữ cái đầu tiên của tên vẫn chiếm tỷ trọng cao theo thời gian là: 
    - Nam: J, C, G
    - Nữ: L, B, R
Nhóm chữ cái có sự tăng cao tính phổ biến ở 2018 so với quá khứ:
    - Nam: A, D, M, K
    - Nữ: A, S, J, K
Nhóm chữ cái không còn phổ biến ở hiện tại:
    - Nam: H, W
    - Nữ: F, M