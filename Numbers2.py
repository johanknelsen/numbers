
# coding: utf-8

# In[1]:


#used for finding the file location
import tkinter as tk
from tkinter.filedialog import askopenfilename


#gets the files location and name
def import_csv_data():
    global v
    csv_file_path = askopenfilename()
    print(csv_file_path)
    v.set(csv_file_path)
    #df = pd.read_csv(csv_file_path)

root = tk.Tk()
tk.Label(root, text='File Path').grid(row=0, column=0)
v = tk.StringVar()
entry = tk.Entry(root, textvariable=v).grid(row=0, column=1)
tk.Button(root, text='Browse Data Set',command=import_csv_data).grid(row=1, column=0)
tk.Button(root, text='Close',command=root.destroy).grid(row=1, column=1)
root.mainloop()


# In[1]:


import pandas as pd
import numpy as np
import datetime as dt
import time
import os
import glob


import os

parse_dates = ['AdjustmentSubmittedTime','TestSubmittedTime1','TestStartedTime','TestCompletedTime','AdjustmentStartedTime','AdjustmentCompletedTime']

readpath = os.path.join('G:\\','Locations_NA', 'LON', 'QU', '02_Lead_Technicians','Batch_Tracking', '2018')


path = readpath
allFiles = glob.glob(os.path.join(path,"*.csv"))

df = pd.DataFrame()

for file_ in allFiles:
    df = df.append(pd.read_csv(open(file_), usecols=range(27),index_col=False,
                               parse_dates=parse_dates))  


    df['WeekNo'] = df['TestSubmittedTime1'].apply(lambda x: (x + dt.timedelta(days=1)).week)
    #df['WeekNo'] = df['TestSubmittedTime'].dt.week #.astype(str)


df['LeadTime'] = df.TestCompletedTime - df.TestSubmittedTime1
df['LeadTime'] = pd.TimedeltaIndex(df['LeadTime'], unit='m')
df['hrs_lead'] = abs(df['LeadTime']/ pd.Timedelta('1 minute'))/60


#Adjust the time to local time
df['TestSubmittedTime1'] = df['TestSubmittedTime1'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

df['TestStartedTime'] = df['TestStartedTime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

df['TestCompletedTime'] = df['TestCompletedTime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

df['AdjustmentStartedTime'] = df['AdjustmentStartedTime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

df['AdjustmentCompletedTime'] = df['AdjustmentCompletedTime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

df['AdjustmentSubmittedTime'] = df['AdjustmentSubmittedTime'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

df['SubWeekDay'] = df['TestSubmittedTime1'].dt.weekday_name
df['SubHour'] = df['TestSubmittedTime1'].dt.hour
df['CompWeekDay'] = df['TestCompletedTime'].dt.weekday_name
df['CompHour'] = df['TestCompletedTime'].dt.hour

df['WeekNo'] = df['TestSubmittedTime1'].apply(lambda x: (x + dt.timedelta(days=1)).week)#.apply(str) + '_'+

df['Week_Day'] = (((df['TestSubmittedTime1'].apply(lambda x: (x + dt.timedelta(days=1)).dayofweek))).apply(str) + '_' +
                  (df['TestSubmittedTime1'].apply(lambda x: (x + dt.timedelta(days=0)).weekday_name)))
df['Hour_sub'] = df['TestSubmittedTime1'].apply(lambda x: (x + dt.timedelta(days=0)).hour)


df['Hour_completed'] = df['TestCompletedTime'].apply(lambda x: (x + dt.timedelta(days=0)).hour)
df['Week_Day_Comp'] = (((df['TestCompletedTime'].apply(lambda x: (x + dt.timedelta(days=1)).dayofweek))).apply(str) + '_' +
                  (df['TestCompletedTime'].apply(lambda x: (x + dt.timedelta(days=0)).weekday_name)))

df['hourly'] = df.Week_Day + '_' + df.Hour_sub.astype(str)

results_key = "submitted"
input_key = "TestSubmittedTime1"
df[results_key] = df[input_key].notna().astype(int)


results_key = "started"
input_key = "TestStartedTime"
df[results_key] = df[input_key].notna().astype(int)

results_key = "completed"
input_key = "TestCompletedTime"
df[results_key] = df[input_key].notna().astype(int)

results_key = "Adjust_Submitted"
input_key = "AdjustmentSubmittedTime"
df[results_key] = df[input_key].notna().astype(int)

results_key = "Adjust_Started"
input_key = "AdjustmentStartedTime"
df[results_key] = df[input_key].notna().astype(int)

results_key = "Adjust_Comp"
input_key = "AdjustmentCompletedTime"
df[results_key] = df[input_key].notna().astype(int)

df


df.sort_values(['MachineNumber','TestTypeName','TestSubmittedTime1'], inplace=True)

date1 = df[['TestStartedTime','started']]
date1 = date1.loc[date1.TestStartedTime.notnull()]
date1.set_index("TestStartedTime", inplace = True)

date2 = df[['TestCompletedTime','completed']]
date2 = date2.loc[date2.TestCompletedTime.notnull()]
date2.set_index("TestCompletedTime", inplace = True)

date3 = df[['AdjustmentSubmittedTime','Adjust_Submitted']]
date3 = date3.loc[date3.AdjustmentSubmittedTime.notnull()]
date3.set_index("AdjustmentSubmittedTime", inplace = True)

date4 = df[['AdjustmentStartedTime','Adjust_Started']]
date4 = date4.loc[date4.AdjustmentStartedTime.notnull()]
date4.set_index("AdjustmentStartedTime", inplace = True)

date5 = df[['AdjustmentCompletedTime','Adjust_Comp']]
date5 = date5.loc[date5.AdjustmentCompletedTime.notnull()]
date5.set_index("AdjustmentCompletedTime", inplace = True)

date6 = df.set_index("TestSubmittedTime1")


# In[2]:


import pandas as pd
import numpy as np

manpower = pd.read_excel(open('G:/Locations_NA/LON\QU/02_Lead_Technicians/Batch_Tracking/2017/Manpower.xlsx',
                           'rb'), sheet=0)

shift = pd.merge(df, manpower[['User', 'Shift']],
                   how='left', left_on=['TestTestedBy'],right_on=['User'])

# 1. Reads in an excel file

df = shift # pd.read_csv(open('G:/Locations_NA/LON/QU/20_Daily_Shift_Notes/10_Batch Tracking Sheets/2017/export.csv'), skiprows=1, usecols=range(25),index_col=False,parse_dates=parse_dates)

parts = pd.read_excel(open('G:/Locations_NA/LON/QU/02_Lead_Technicians/Batch_Tracking/2018/New folder/Part_Numbers_Info.xlsx',
                           'rb'), sheet_name='Part_No')

parts['CEP_Helper']= parts.PartNumber.astype(str) + parts.CEP_Sample
parts['UTM_Helper']= parts.PartNumber.astype(str) + parts.UTM_Sample

numbers = df
numbers['AdjustmentTime'] = np.where(numbers['AdjustmentTypeName'].fillna(0) != 0, 0.15,0)
numbers['Helper']= numbers.PartNumber.astype(str) + numbers.TestTypeName


numbers = pd.merge(df, parts[['CEP_Helper', 'CEP']],
                   how='left', left_on=['Helper'],right_on=['CEP_Helper'])

numbers2 = pd.merge(numbers, parts[['CEP_Helper', 'Health_Chart']],
                    how='left', left_on=['Helper'],right_on=['CEP_Helper'])

numbers3 = numbers2.loc[numbers2.AdjustmentTypeName.isin(['Reteach']), 'CEP'] = 0

numbers3 = pd.merge(numbers2, parts[['UTM_Helper', 'UTM']],
                    how='left', left_on=['Helper'],right_on=['UTM_Helper'])


#drops the not needed columns after they are no longer required.
numbers3.drop(['CEP_Helper_x', 'CEP_Helper_y','Helper','UTM_Helper'], axis=1, inplace=True)

numbers3 = numbers3.drop_duplicates(['PartNumber','BatchNumber','TestTypeName',
                                     'BatchType','AdjustmentTypeName','AdjustmentCompletedTime'],
                                    keep='first')

numbers4 = numbers3#.fillna(0)
numbers4[['AdjustmentTime','CEP','Health_Chart','UTM']] = numbers4[['AdjustmentTime',
          'CEP','Health_Chart','UTM']].fillna(value=0)

numbers4['CEP_Hrs']=((numbers4.CEP*7)/60)/1.3
numbers4['UTM_Hrs']=((numbers4.UTM*5)/60)/1.3
numbers4['AdjustmentTime'] = numbers4.AdjustmentTime
#numbers4['Extra_CEP_Hrs']=((numbers4['No. Welds']*7)/60)/1.3
#numbers4['Extra_UTM_Hrs']=((numbers4['No. Extra Pulls']*5)/60)/1.3
numbers4['Total_Hrs'] = ((numbers4.CEP*7+numbers4.UTM*5+numbers4.Health_Chart+numbers4.AdjustmentTime)/60/1.3)

#numbers4.rolling('8h', min_periods=1).sum()


grouped = numbers4.groupby(['WeekNo','TestTestedBy']).agg({'Total_Hrs' : 'sum',
                                                           'CEP_Hrs' : 'sum','UTM_Hrs' :'sum',
                                                           'AdjustmentTime' : 'sum',
                                                           'hrs_lead' : 'mean'}).round().reset_index()
#grouped = numbers4.groupby(['WeekNo','TestTestedBy'])['Total_Hrs','CEP_Hrs','UTM_Hrs','AdjustmentTime'].sum().reset_index()


# In[ ]:


from pivottablejs import pivot_ui
pivot_ui(numbers4)


# # The results for number of hours completed

# In[3]:


grouped = grouped.sort_values(['WeekNo','Total_Hrs'], ascending=[False,False]).reset_index()
#grouped = grouped.sort_values(['WeekNo','Total_Hrs'], ascending=[False,False]).reset_index()
grouped.index = grouped.index + 1
grouped.drop(['index'], axis=1, inplace=True)
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 0])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'barneje'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'loncmm1'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'lonutm1'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'lonutm2'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'fostesc'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'hetupie'])
grouped


# In[4]:


import matplotlib
import matplotlib.pyplot as plt
# show plots inline
get_ipython().magic('matplotlib inline')
from pylab import rcParams
rcParams['figure.figsize'] = 10, 8

limit = 30


grouped = numbers4.groupby(['WeekNo','TestTestedBy'])['Total_Hrs'].sum().reset_index()
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 0])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'barneje'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'loncmm1'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'lonutm1'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'lonutm2'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'fostesc'])
grouped = grouped.drop(grouped.index[grouped.TestTestedBy == 'hetupie'])
#grouped = grouped[grouped.Group != 0]

t=grouped.pivot_table(grouped,index=['WeekNo'],columns=['TestTestedBy'],aggfunc=np.sum)
fig, ax = plt.subplots(1,1)
ax.set_prop_cycle(plt.cycler('color', plt.cm.Accent(np.linspace(0, 1, limit))))
t.plot(ax=ax)
plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))

import matplotlib
import matplotlib.pyplot as plt
# show plots inline
get_ipython().magic('matplotlib inline')
from pylab import rcParams
rcParams['figure.figsize'] = 10, 8

sort = grouped
tps = sort.pivot_table(values=['Total_Hrs'], 
                      index='WeekNo',
                      #columns='Changeover',
                      columns='TestTestedBy',
                      aggfunc='sum')

tps = tps.div(tps.sum(1), axis=0)
tps.plot(kind='bar', stacked=True, colormap='Paired', sort_columns=True)

plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
# Add a table at the bottom of the axes


# In[5]:


cha = grouped#.convert_objects(convert_numeric=True)    
sub_df = cha.groupby(['WeekNo','TestTestedBy'])['Total_Hrs'].sum().unstack()
sub_df.plot(kind='bar',stacked=True, colormap='Paired', sort_columns=True)

plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))


# In[6]:


ytd = numbers4.groupby(['TestTestedBy'])['Total_Hrs','CEP_Hrs','UTM_Hrs',
                                         'AdjustmentTime',].sum().round().reset_index()
ytd = ytd.sort_values(['Total_Hrs'], ascending=[False]).reset_index()
ytd = ytd.drop(ytd.index[ytd.TestTestedBy == 0])
ytd = ytd.drop(ytd.index[ytd.TestTestedBy == 'barneje'])
ytd = ytd.drop(ytd.index[ytd.TestTestedBy == 'loncmm1'])
ytd = ytd.drop(ytd.index[ytd.TestTestedBy == 'lonutm1'])
ytd = ytd.drop(ytd.index[ytd.TestTestedBy == 'lonutm2'])
ytd = ytd.drop(ytd.index[ytd.TestTestedBy == 'fostesc'])
ytd = ytd.drop(ytd.index[ytd.TestTestedBy == 'hetupie'])
ytd


# In[7]:


import matplotlib.pyplot as plt
import seaborn as sns

## Recommended way
#sns.lmplot(x='WeekNo', y='Total_Hrs', data=grouped,
#          fit_reg=False, # No regression line
#           hue='TestTestedBy')   # Color by evolution stage
## Tweak using Matplotlib
#plt.ylim(0, None)
#plt.xlim(0, None)

sns.jointplot(x='WeekNo', y='Total_Hrs', data=grouped)

plt.ylim(0, None)
plt.xlim(0, None)
 
# Alternative way
# sns.lmplot(x=df.Attack, y=df.Defense)


# In[8]:


import matplotlib.pyplot as plt
import seaborn as sns

## Recommended way
#sns.lmplot(x='WeekNo', y='Total_Hrs', data=grouped,
#          fit_reg=False, # No regression line
#           hue='TestTestedBy')   # Color by evolution stage
## Tweak using Matplotlib
#plt.ylim(0, None)
#plt.xlim(0, None)

sns.jointplot(x='WeekNo', y='hrs_lead', data=grouped)

plt.ylim(0, None)
plt.xlim(0, None)
 
# Alternative way
# sns.lmplot(x=df.Attack, y=df.Defense)


# In[17]:


savepath = 'G:/Locations_NA/LON/QU/02_Lead_Technicians/Batch_Tracking/2018/'
name = '2018_All_Data'
numbers4.to_csv(savepath + name + '.csv', header = True)


# In[ ]:


list(df.Note.unique())


# In[ ]:


df['Note'].value_counts()


# In[13]:


import matplotlib.pyplot as plt
import seaborn as sns

subhrgrouped = numbers4.groupby(['WeekNo','SubHour']).agg({'Total_Hrs' : 'sum',
                                           'CEP_Hrs' : 'sum',
                                           'UTM_Hrs' :'sum',
                                           'Extra_CEP_Hrs' : 'sum',
                                           'AdjustmentTime' : 'sum',
                                           'Extra_UTM_Hrs' : 'sum',
                                          'hrs_lead' : 'mean'}).round().reset_index()
## Recommended way
#sns.lmplot(x='WeekNo', y='Total_Hrs', data=grouped,
#          fit_reg=False, # No regression line
#           hue='TestTestedBy')   # Color by evolution stage
## Tweak using Matplotlib
#plt.ylim(0, None)
#plt.xlim(0, None)

sns.jointplot(x='SubHour', y='Total_Hrs', data=subhrgrouped)

plt.ylim(0, None)
plt.xlim(0, None)


# In[8]:


FO = numbers4.groupby(['TestStatus'])['TestTypeName'].count().round().reset_index()
FO


# # Number of reteaches by weld position

# In[9]:


df2 = df
df2['Weld_Position_Groups'] = df2['Attributes'].str.split('Weld IDs=').str[1]
#df2['Col2'] = df['Col2'].str.lower()
df2 = df2.dropna(subset=['Weld_Position_Groups'])
df2['Number_of_Welds'] = df2['Attributes'].str.extract('No. Welds=(\d+)', expand=False).astype(float)
df2['Weld_Position'] = df2['Weld_Position_Groups'].str.extract('(\d+[.]?\d?)', expand=False).astype(float)
df2['Time_OK_Result'] = df2.TestCompletedTime - df2.TestSubmittedTime1

welds = df2
grouped_welds = welds.groupby(['MachineNumber','PartNumber', 'Weld_Position']).agg({'Number_of_Welds' : 'sum'}).round().reset_index()

grouped_welds = grouped_welds.sort_values(['Number_of_Welds'], ascending=[False])
grouped_welds
#grouped_welds.to_csv('welds.csv', header = True)


# # Broken down by hour

# In[8]:


from IPython.display import display_html
def display_side_by_side(*args):
    html_str=''
    for df in args:
        html_str+=df.to_html()
    display_html(html_str.replace('table','table style="display:inline"'),raw=True)


rs = numbers4
rs['Result'] = (rs.TestCompletedTime - rs.TestSubmittedTime1)


rs['Result'] = pd.TimedeltaIndex(rs['Result'], unit='m')
rs['Minutes_Result'] = abs(rs['Result']/ pd.Timedelta('1 minute'))

sub = rs.groupby(['Hour_sub', 'Week_Day']).agg({'Total_Hrs' : ['sum']}).round().unstack()
#
#
#
#sub
rl = rs.groupby(['Hour_completed', 'Week_Day_Comp']).agg({'Total_Hrs' : ['sum']}).round().unstack()
#rl.drop(rl.columns[[0]], axis=1, inplace=True)

tr = rs.groupby(['Hour_completed', 'Week_Day_Comp']).agg({'Minutes_Result' : ['mean']}).round().unstack()
#tr.drop(tr.columns[[0]], axis=1, inplace=True)

display_side_by_side(sub,rl,tr)

