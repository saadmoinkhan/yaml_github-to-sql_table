#!/usr/bin/env python
# coding: utf-8

""" Code which extracts all the information from _homepage, active_homepage, and inactive_homepage files from github.
It extracts all the data except the commented ones, and arrange it in a tabular format with a serialised indexing which
is in reference to the homepage.

The table has following columns:-
index, type, widget, widgetid, assetid, linkurl, imageurl, titletext, country_code, platform, dataset, timestamp

"""

"""
Created on 12/05/2021

@author: saadmoinkhan

"""

import re
import csv
import requests
import base64
import pandas as pd
import re
import yaml
import pandas_gbq
import datetime
import os


from google.auth.transport.requests import Request
from google.oauth2 import service_account



#Code Block to download data from github

user = ""
#temp credentials to be modified after production access
token = os.environ['GITHUB_TOKEN']
repo = ""

git_repo_url =""

git_repo_dir = requests.get(git_repo_url,auth=(user,token))
print(git_repo_dir.status_code)
git_repo_dir = git_repo_dir.json()

data_homepage = ''
data_active_homepage = ''
data_inactive_homepage = ''

data_homepage_mob = ''
data_active_homepage_mob = ''
data_inactive_homepage_mob = ''

for file in git_repo_dir["tree"]:
    path_list = ['']
    for x in range(0,len(path_list)):
        if(file["path"] == str(path_list[x])):
            #print(file['sha'])
            sh = file['sha']
            git_file_url = ""+ str(sh)

            git_file = requests.get(git_file_url, auth=(user,token))
            #print(git_file.status_code)
            git_file = git_file.json()
            git_file = git_file['content']
            decoded_yaml=base64.b64decode(git_file).decode('UTF-8')
            data = str(decoded_yaml)

            if x == 0:
                data_homepage += data

            elif x == 1:
                data_active_homepage += data
            elif x == 2:
                data_inactive_homepage += data
            elif x == 3:
                data_homepage_mob += data
            elif x == 4:
                data_active_homepage_mob += data
            elif x == 5:
                data_inactive_homepage_mob += data



#Replacement of dynamic link URls and unnecessary code

def data_clean(data):

    data = data.replace('{{lang()}}', 'en')
    data = data.replace("{% if lang('en') %}", '')
    data = data.replace("{% if lang('ar') %}", '  Arabic ')
    data = data.replace('{% endif %}', '')
    data = data.replace('{% else %}', 'Arabic')

    #Converting the string data into a list, separator is '\n' but we keep the '\n' after splitting
    data = data.splitlines(True)

    return data

data_list = [data_homepage, data_active_homepage, data_inactive_homepage,
             data_homepage_mob, data_active_homepage_mob, data_inactive_homepage_mob]

#Loop to clean the data from all the datasets in the data_list
for x in range(0, len(data_list)):
    data_list[x] = data_clean(data_list[x])




#Function to find indices wherever given string ('to_find') is mentioned, excluding the commented area.
#first_index condition is for a special case where we need to find multiple blocks in a widgetId
def index_finder(data, to_find, first_index = False):
    index_list = []

    for x in range(0,len(data)):
        if '#' not in data[x]:
            tup = ()
            temp = str(data[x])
            temp = temp.replace(' ','')
            if first_index == False:
                if to_find in temp:
                    temp = temp.replace(to_find, '')
                    temp = temp.replace('\n','')
                    tup += (temp, x+1)
                    index_list.append(tup)
            else:
                if temp[0] == '-':
                    temp = temp.replace(to_find, '')
                    temp = temp.replace('\n','')
                    tup += (temp, x+1)
                    index_list.append(tup)
    return index_list



#Function to extract links from the string
def extract_link(link, text):
    link = link.replace(text,'').replace(':', '')
    link = link.strip()
    link = link.split('')
    link = link[0].strip()
    link = link.replace('\"', '')
    link = link.replace(' ','')
    if (link != '' and link != '' and link != ''
        and link != '' and link != '' and link != ''):

        link = link.replace('','').strip()
        link = link.replace('','').strip()
        link = link.replace('','').strip()
    try:
        if '-' in link[0]:
            link = link[1:]
    except:
        pass
    link = link.replace("\"","")
    link = link.strip()
    if link.strip() == "":
        link = 'Not Found'
    return link


#This code block will find the indices where ae, sa and eg are mentioned
def country_indices_func(data):

    country_indices = []
    check_list = ["{% if country('') %}", "{% if country('') %}", "{% if country('') %}"]

    for x in check_list:
        country_tup = ()
        try:
            temp = int([i for i, s in enumerate(data) if x in s][0]) #List comprehension loop to find the index where
                                                                     #where country is mentioned
            if 'ae' in x:
                country_tup += ('',)
                country_tup += (temp,)
                country_indices.append(country_tup)

            elif 'sa' in x:
                country_tup += ('',)
                country_tup += (temp,)
                country_indices.append(country_tup)

            elif 'eg' in x:
                country_tup += ('',)
                country_tup += (temp,)
                country_indices.append(country_tup)
        except:
            pass

    return country_indices


'''
Function which extracts all the data and then convert it to a list. Count is used for dataset identification.
    start - starting index
    end   - end index
    type_index - String which tells the type
    country_code - String which tells the country
    count - To identify which dataset are we on
    index_counter - To have range of indices from 0 to end for every single homepage
'''
def homepage_retrieval(start, end, data, type_index, country_code, count, index_counter):
    data = data[start:end]
    widget_index = index_finder(data ,'widgetId')  #Returns a list(made of tuples) of indices wherever widgetId is mentioned
    sub_type_list = []  #List which will be appended with tuples in this function
    temp_index = index_counter  #To increment by 1, and save the iteration index for every new homepage
    if len(widget_index) == 0:   #Finding linkUrl or titleText if there is no widget ID
        linkUrl = 'foo_just_for_check'
        imageUrl = 'foo_just_for_check'   #Using foo_just_for_check to use it in if loop, using '' will create a problem
        titleText = 'foo_just_for_check'
        assetId = 'foo_just_for_check'
        tup = (type_index,)  #Adding type found to the tuple
        tup += ('Not Found',) #Adding Not Found for widget column because len(widget_index) == 0
        tup += ('Not Found',) #Adding Not Found for widgetId column because len(widget_index) == 0
        for x in range(0, len(data)):
            if '#' not in data[x]:
                link = str(data[x])

                if 'imageUrl' in link:
                    imageUrl = extract_link(link, 'imageUrl')
                    if 'https' in imageUrl:
                        try:
                            colon_idx = imageUrl.index('https')
                            imageUrl = imageUrl[:colon_idx+5] + ':' + imageUrl[colon_idx+5:]
                        except:
                            pass
                elif 'linkUrl' in link:
                    linkUrl = extract_link(link, 'linkUrl')
                    if 'https' in linkUrl:
                        try:
                            colon_idx = linkUrl.index('https')
                            linkUrl = linkUrl[:colon_idx+5] + ':' + linkUrl[colon_idx+5:]
                        except:
                            pass
                elif 'titleText' in link:
                    titleText = extract_link(link, 'titleText')
                elif 'assetId' in link:
                    titleText = extract_link(link, 'assetId')

        if imageUrl == 'foo_just_for_check':
            tup += ('Not Found',)
        else:
            tup += (imageUrl,)

        if linkUrl == 'foo_just_for_check':
            tup += ('Not Found',)
        else:
            tup += (linkUrl,)

        if assetId == 'foo_just_for_check':
            tup += ('Not Found',)
        else:
            tup += (assetId,)

        if titleText == 'foo_just_for_check':
            tup += ('Not Found',)
        else:
            tup += (titleText,)

        tup += (country_code,)  # To add country_code

        if count == 0:
            tup += ('_homepage',)
            tup += ('web',)
        elif count == 1:
            tup += ('active_homepage',)
            tup += ('web',)

        elif count == 2:
            tup += ('inactive_homepage',)
            tup += ('web',)

        elif count == 3:
            tup += ('_homepage',)
            tup += ('app',)

        elif count == 4:
            tup += ('active_homepage',)
            tup += ('app',)

        elif count == 5:
            tup += ('inactive_homepage',)
            tup += ('app',)

        tup += (temp_index,)
        temp_index += 1
        sub_type_list.append(tup)


#Starting of else block, if a widgetId is present
    else:
        for x in range(0, len(widget_index)):

            start = widget_index[x][1]
            if x+1 < len(widget_index):
                end = int(widget_index[x][1])
            else:
                end = len(data)

            data_1 = data[start:end-1]  #Splitting data based on widget_index

            link_index = index_finder(data_1, '-', first_index = True) #Finding indexes where '-' is first index as '-'
            #in yaml is used to separate a data block

            #For condition - length of index or 1(if lenth is 0), to make sure the loop runs at least once, kind of a
            #do-while loop
            for y in range(0, len(link_index) if len(link_index) != 0 else 1):
                tup = ()
                tup += (type_index,)  #Tuple Type
                temp = widget_index[x][0].replace(':','')
                if temp != '':
                    temp = str(widget_index[x][0]) + '/' +str(y+1)
                    temp1 = str(widget_index[x][0])
                    temp = temp.replace(':','')
                    temp1 = temp1.replace(':', '')
                    temp = 'W' + temp
                    temp1 = 'W' + temp1
                else:
                    temp = 'Not Found'
                    temp1 = 'Not Found'

                tup += (temp1,) # Tuple widget
                tup += (temp,)  # Tuple widgetId
                linkUrl = 'foo_just_for_check'
                imageUrl = 'foo_just_for_check'   #Using foo_just_for_check to use it in if loop, using '' will create a problem
                assetId = 'foo_just_for_check'
                titleText = 'foo_just_for_check'

                #If lenght is 0,1 - Means there is one or none data block in it, therefore, we can easily traverse till
                #the end of data.
                if len(link_index)<2:
                    start = 0
                    end = len(data_1) - 1
                #If lenght is more than 1 - Means there are multiple data blocks, therefore, we need to split dataset
                #according to the indices
                else:
                    start = link_index[y][1] - 1
                    if y+1 < len(link_index):
                        end = int(link_index[y+1][1]) - 1
                    else:
                        end = len(data_1) -1

                data_2 = data_1[start:end] #Sub-dividing the dataset to get individual data blocks

                #Loop to traverse through individual data blocks and extract the data
                for z in range(0,len(data_2)):
                    if '#' not in data_2[z]:
                        link = str(data_2[z])

                        if 'imageUrl' in link:
                            imageUrl = extract_link(link, 'imageUrl')
                            if 'https' in imageUrl:
                                try:
                                    colon_idx = imageUrl.index('https')
                                    imageUrl = imageUrl[:colon_idx+5] + ':' + imageUrl[colon_idx+5:]
                                except:
                                    pass
                        elif 'linkUrl' in link:
                            linkUrl = extract_link(link, 'linkUrl')
                            if 'https' in linkUrl:
                                try:
                                    colon_idx = linkUrl.index('https')
                                    linkUrl = linkUrl[:colon_idx+5] + ':' + linkUrl[colon_idx+5:]
                                except:
                                    pass
                        elif 'assetId' in link:
                            assetId = extract_link(link, 'assetId')
                            assetId = 'A' + str(assetId)
                        elif 'titleText' in link:
                            titleText = extract_link(link, 'titleText')


                if imageUrl == 'foo_just_for_check':
                    tup += ('Not Found',)
                else:
                    tup += (imageUrl,)

                if linkUrl == 'foo_just_for_check':
                    tup += ('Not Found',)
                else:
                    tup += (linkUrl,)

                if assetId == 'foo_just_for_check':
                    tup += ('Not Found',)
                else:
                    tup += (assetId,)

                if titleText == 'foo_just_for_check':
                    tup += ('Not Found',)
                else:
                    tup += (titleText,)

                tup += (country_code,) #Adding country code

                if count == 0:
                    tup += ('_homepage',)
                    tup += ('web',)
                elif count == 1:
                    tup += ('active_homepage',)
                    tup += ('web',)

                elif count == 2:
                    tup += ('inactive_homepage',)
                    tup += ('web',)

                elif count == 3:
                    tup += ('_homepage',)
                    tup += ('app',)

                elif count == 4:
                    tup += ('active_homepage',)
                    tup += ('app',)

                elif count == 5:
                    tup += ('inactive_homepage',)
                    tup += ('app',)

                tup += (temp_index,)
                temp_index += 1
                sub_type_list.append(tup)

    return (sub_type_list), temp_index



#Main Function

'''Data list was defined above, for reference, it looks like this -
data_list = [data_homepage, data_active_homepage, data_inactive_homepage,
           data_homepage_mob, data_active_homepage_mob, data_inactive_homepage_mob]
'''
count = 0  #Used for data_list
main_list = []

#Looping through elements in the data_list
for y in data_list:
    country_indices = country_indices_func(y)  #Finding the indices where '' are mentioned

    #Looping through each index
    for z in range(0, len(country_indices)):

        start = country_indices[z][1]
        if z+1 < len(country_indices):
            end = int(country_indices[z+1][1])
        else:
            end = len(y)
        country_code = str(country_indices[z][0])
        temp_y = y[start:end]  #Splitting the data into a temporary variable
        type_index = index_finder(temp_y, '-type:')  #Finding indices in this new dataset where '-type:' is mentioned
        index_counter = 1  #Used for tracking the iteration of the individual homepages

        #Looping through the different types found on the homepage
        for x in range(0, len(type_index)):

            start = int(type_index[x][1])
            if x+1 < len(type_index):
                end = int(type_index[x+1][1])
            else:
                end = len(y)

            temp_list, index_counter = homepage_retrieval(start, end, temp_y, type_index[x][0], country_code, count,
                                                         index_counter)

            main_list.extend(temp_list)

    count += 1



# Organising the data and outputting it

df = pd.DataFrame(main_list)

df = df.rename(columns={0:'type', 1:'widget' ,2:'widgetid', 3:'imageurl', 4:'linkurl', 5:'assetid', 6:'titletext',
                       7: 'country_code', 8: 'dataset', 9: 'platform', 10: 'index'})

columns_titles = ['index', 'type', 'widget','widgetid', 'assetid', 'linkurl', 'imageurl', 'titletext',
                  'country_code', 'platform', 'dataset']

df=df.reindex(columns=columns_titles)

df.insert(11, 'timestamp', (pd.to_datetime('now') + datetime.timedelta(hours = 4)).replace(microsecond=0))
df

# #Uncomment to save the data
# df.to_csv('Output_Daily.csv', index = False)



#Code to upload the data to the BQ table.

credentials = service_account.Credentials.from_service_account_file('/credentials/credentials.json')

pandas_gbq.to_gbq(df, '', project_id= '', credentials = credentials, if_exists = 'replace')

pandas_gbq.to_gbq(df, '', project_id= '', credentials = credentials, if_exists = 'append')
