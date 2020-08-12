#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import modules
import pandas as pd
import numpy as np
import requests

from pandas.io import gbq
from google.cloud import bigquery,storage
from google.oauth2 import service_account

import gspread
import json
from canvasapi import Canvas

#import plotly.express as px


# In[66]:


#choose credential file paths and other possible changes:
run_mode = 'prod_home' #or, 'prod', or 'mig' for when migrating the code


# In[67]:



if run_mode == 'dev':
    cred_file_path = "."
    out_table = "all_courses3"
elif run_mode == 'prod':
    cred_file_path = "/home/aroy/projects/canvas_data_portal/canvas_course_tracking"
    out_table = "all_courses4"

elif run_mode == 'prod_home':
    cred_file_path = "."
    out_table = "all_courses4"

    
elif run_mode == 'mig':
    cred_file_path = "/home/aroy/projects/canvas_data_portal/canvas_course_tracking"
    out_table = "all_courses3"


# In[58]:



#get Google cloud credentials
project_id = 'canvas-portal-data-custom'
cred_file = '{}/canvas-portal-data-custom-6e244db3b826.json'.format(cred_file_path)
data_dl = 'data'
scopes = [ "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file",
            "https://spreadsheets.google.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_file(cred_file,)

#get Canvas credentials
cred_file2 = '{}/instances.json'.format(cred_file_path)
with open(cred_file2,'r') as cred2:
    cred_json = json.load(cred2)

API_KEY = cred_json['ACCES_TOKEN']
API_URL = cred_json['API_URL']#+'/accounts/1'


# In[59]:


# Initialize a new Canvas object
canvas = Canvas(API_URL, API_KEY)
canvas.__dict__


# ### Get courses running on Canvas -- created via migration or by LT's

# In[60]:


#Get the LT list from the Excel sheet
lt_df_cols = ['School', 'Dept_num', 'Dept_name', 'Name', 'Email']
lt_df = pd.read_excel('{}/Learning_Technologists_updating.xlsx'.format(cred_file_path))
lt_df.columns = lt_df_cols
lt_df['Email'] = lt_df.Email.str.lower()
lt_df.tail()


# In[28]:


'''
#Read the Stellar to Canvas migration list on Google Drive, and construct a
#list of course_id s
gs_name = "Stellar to Canvas content migration request (Responses)"
#gs_name = "xyz"
gc = gspread.service_account(filename=cred_file)
sh = gc.open(gs_name).sheet1
sh_data = sh.get_all_values()
head_col = sh_data.pop(0)
stellar_df = pd.DataFrame(sh_data, columns=head_col)
stellar_df['course_id'] = stellar_df['Canvas URL to migrate to'].str.split("/").str[-1]
'''


# In[61]:


def get_course_info(course_id):
    '''This function gets the course information and the file/assignment update times,
    by the course_id, and returns a list of '''
    try:
        c1 = canvas.get_course(course_id)


        course_dept = sub_account_dict[c1.account_id]
        course_name = c1.name

        files_ = c1.get_files()
        assn_ = c1.get_assignments()
        #Get the file updated times
        file_utimes = [f_.updated_at for f_ in files_]

        #Get the assignment updated times
        assn_utimes = [a_.updated_at for a_ in assn_]
        fa_times = file_utimes + assn_utimes

        #Convert the whole thing 
        fa_times = np.array(fa_times, dtype='datetime64')



        return [c1.id, course_dept, course_name, len(file_utimes), len(assn_utimes), 
                len(fa_times), fa_times.max()]
    except Exception as e:
        return [None, None, None, None, None, None, None]


# In[62]:


#Get a list of all department names by the sub-account id:
acc = canvas.get_account(1)
sub_account_dict = {}
accs = acc.get_subaccounts(recursive=True)
for a_ in accs:
    sub_account_dict[a_.id] = a_.name

#sub_account_dict


# In[63]:


#Get a list of all courses:
large_num = 100000 #give a very large number so all courses are pulled recursively
excl_acc = [1,17,76] #mention sub-accounts to be excluded
begin_date = '2020-05-01' #Date since when we are counting

course_rows = []
course_cols = ['course_id', 'course_code', 'course_name', 'account_id', 'created_at']
for acc_course in acc.get_courses()[:large_num]:
    course_rows.append([acc_course.id, acc_course.course_code, acc_course.name, acc_course.account_id, 
                        acc_course.created_at])
    
courses_df = pd.DataFrame.from_records(course_rows, columns=course_cols)

courses_df_dep_excluded = courses_df[~courses_df.account_id.isin(excl_acc)]
'''
try:
    courses_df_dep_excluded['Dept'] = courses_df_dep_excluded.apply(lambda 
                                                        row: sub_account_dict[row['account_id']], axis=1)
except:
    pass
courses_df_dep_excluded
'''
filtered_courses_df = courses_df_dep_excluded[courses_df_dep_excluded.created_at>=begin_date]

filtered_courses_df.to_csv('all_canvas_since_{}.csv'.format(begin_date), index=None)
filtered_courses_df


# In[ ]:


#Check that the migration/production/dev is set correctly
all_course_list_0 = filtered_courses_df.course_id.tolist()
all_course_list_0_rows = []
all_course_list_0_cols = ['course_id','Dept', 'Course_name', 'num_files', 'num_assignments', 'num_tot_fa',
                        'last_update_at']

if run_mode == 'dev' or run_mode=='mig':
    num_courses = 5
elif run_mode == 'prod' or run_mode=='prod_home':
    num_courses = len(all_course_list_0) + 1
    
for c_id in all_course_list_0[:num_courses]:
    all_course_list_0_rows.append(get_course_info(c_id))
    
all_course_list_0_df = pd.DataFrame.from_records(all_course_list_0_rows, columns=all_course_list_0_cols)
all_course_list_0_df['if_LT_led'] = 0
all_course_list_0_df['LT_email'] = np.nan
print(all_course_list_0_df.shape)
all_course_list_0_df.tail()


# In[45]:


#Check that the migration/production/dev is set correctly
lt_courses_row = []
lt_courses_cols = all_course_list_0_cols + ['if_LT_led','LT_email']
courses_to_exclude = [3157, 3158]
lt_list = lt_df.Email.tolist()

if run_mode == 'dev' or run_mode=='mig':
    num_lts = 5
elif run_mode == 'prod' or run_mode=='prod_home':
    num_lts = len(lt_list) + 1


for user_email in lt_list[:num_lts]:
    try:
        user_ = canvas.get_user(user_email, 'sis_login_id')
        course_list = []
        user_courses = user_.get_enrollments(type=['TeacherEnrollment'])

        for uc_ in user_courses:
            #print(uc_.id, uc_.course_id)
            if uc_.course_id not in courses_to_exclude:
                uc_row = get_course_info(uc_.course_id)
                #print(uc_row)
                uc_row.extend([1, user_email])
                lt_courses_row.append(uc_row)
    except Exception as e:
        print('Error {} for user {}'.format(e, user_email))
        pass

print(all_lt_courses.shape)        
all_lt_courses = pd.DataFrame.from_records(lt_courses_row, columns=lt_courses_cols)
#all_lt_courses


# In[46]:


all_courses_df = pd.concat([all_course_list_0_df, all_lt_courses], ignore_index=True)
#all_courses_df


# In[47]:


ts_cutoff_1w = pd.to_datetime('now') - pd.to_timedelta('7days')
ts_cutoff_1m = pd.to_datetime('now') - pd.to_timedelta('30days')
all_courses_df['if_active_last_week'] = np.where(all_courses_df.last_update_at>ts_cutoff_1w, True, False)
all_courses_df['if_active_last_month'] = np.where(all_courses_df.last_update_at>ts_cutoff_1m, True, False)
all_courses_df['if_sandbox_course'] = np.where(all_courses_df.Dept=='Sandboxes', 1, 0)
all_courses_df = all_courses_df[all_courses_df.course_id.notna()].reset_index(drop=True)
all_courses_df.drop_duplicates(subset=['course_id'], keep='last', inplace=True)
#all_courses_df.tail()


# In[48]:


all_courses_df['last_update_at'] = all_courses_df['last_update_at'].dt.strftime('%Y-%m-%d')
#all_courses_df.tail()


# In[49]:


all_courses_df[all_courses_df.if_sandbox_course==0]
all_courses_df.to_gbq('lt_courses.{}'.format(out_table), project_id, if_exists='replace', credentials=credentials)
#all_courses_df.to_csv('all_courses.csv', index=None)

