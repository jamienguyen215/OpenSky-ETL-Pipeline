#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd 
import requests


# In[2]:


url="https://opensky-network.org/api/states/all"


# In[3]:


response = requests.get(url)


# In[4]:


if response.status_code==200:
    print ("Successfully Fetching Data")
else: print("Error Fetching Data:",response.status_code)


# In[5]:


data=response.json()


# In[6]:


data=data['states']


# In[7]:


df=pd.DataFrame(data)


# In[8]:


df.head()


# In[9]:


#Define Columns - OpenSky Network source: https://openskynetwork.github.io/opensky-api/rest.html
columns=[
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source"
]


# In[10]:


df=pd.DataFrame(data,columns=columns)


# In[11]:


df.head()


# In[12]:


df.shape


# ### Check Null Values and Data Types

# In[13]:


data_info={
    "Columns": df.columns,
    "Null Count": [df[column].isnull().sum() for column in df.columns],
    "Type":[df[column].dtype for column in df.columns]
}


# In[14]:


df_info=pd.DataFrame(data_info)


# In[15]:


df_info


# ### Check Duplicates

# In[16]:


duplicated_rows=df[df.duplicated()]


# In[17]:


duplicated_rows.shape[0]


# In[18]:


df.columns


# In[19]:


columns_to_check=['icao24', 'callsign', 'origin_country']


# In[20]:


duplicated_rows_subset=df[df.duplicated(subset=columns_to_check,keep='first')]


# In[21]:


duplicated_rows_subset.shape[0]


# **The data doesn't seem to have any duplicates.**

# In[22]:


df['datetime']=pd.to_datetime(df['time_position'],unit='s', utc=True)


# In[23]:


df.head()


# In[24]:


df.shape


# In[25]:


print(df['datetime'].min())
print(df['datetime'].max())


# In[26]:


df_onground = df[df['on_ground']==True]


# In[27]:


df_onground.shape


# In[28]:


#Since my purpose is to identify which airports are the busiest, I just need some columns

df=df.drop(columns=['last_contact','velocity',
       'true_track', 'vertical_rate', 'sensors', 'squawk',
       'spi', 'position_source'])


# In[29]:


df.head()


# In[30]:


data_info={
    "Columns": df.columns,
    "Null Count": [df[column].isnull().sum() for column in df.columns],
    "Type":[df[column].dtype for column in df.columns]
}
df_info=pd.DataFrame(data_info)
df_info


# In[31]:


baro_null_geo_notnull=df[df['baro_altitude'].isnull() & df['geo_altitude'].notnull()]
baro_null_geo_notnull


# In[32]:


baro_notnull_geo_null=df[df['baro_altitude'].notnull() & df['geo_altitude'].isnull()]
baro_notnull_geo_null.head()


# In[33]:


df['baro_altitude']=df['baro_altitude'].fillna(df['geo_altitude'])


# In[34]:


df['baro_altitude'].isnull().sum()


# In[35]:


#drop records with null values in longitude and latitude 


# In[36]:


df.dropna(subset=['longitude','latitude'],inplace=True)


# In[37]:


df.shape


# In[38]:


data_info={
    "Columns": df.columns,
    "Null Count": [df[column].isnull().sum() for column in df.columns],
    "Type":[df[column].dtype for column in df.columns]
}
df_info=pd.DataFrame(data_info)
df_info


# In[39]:


airports = [
    {"name": "Hartsfield-Jackson Atlanta Intl", "code": "ATL", "lat": 33.6407, "lon": -84.4277},
    {"name": "Los Angeles International", "code": "LAX", "lat": 33.9416, "lon": -118.4085},
    {"name": "Chicago O'Hare International", "code": "ORD", "lat": 41.9742, "lon": -87.9073},
    {"name": "Dallas/Fort Worth International", "code": "DFW", "lat": 32.8998, "lon": -97.0403},
    {"name": "Denver International", "code": "DEN", "lat": 39.8561, "lon": -104.6737},
    {"name": "John F. Kennedy International", "code": "JFK", "lat": 40.6413, "lon": -73.7781},
    {"name": "San Francisco International", "code": "SFO", "lat": 37.6213, "lon": -122.3790},
    {"name": "Seattle-Tacoma International", "code": "SEA", "lat": 47.4502, "lon": -122.3088},
    {"name": "Miami International", "code": "MIA", "lat": 25.7959, "lon": -80.2871},
    {"name": "Orlando International", "code": "MCO", "lat": 28.4312, "lon": -81.3081},
    {"name": "Charlotte Douglas International", "code": "CLT", "lat": 35.2140, "lon": -80.9431},
    {"name": "Las Vegas Harry Reid International", "code": "LAS", "lat": 36.0840, "lon": -115.1537},
    {"name": "Phoenix Sky Harbor International", "code": "PHX", "lat": 33.4373, "lon": -112.0078},
    {"name": "Boston Logan International", "code": "BOS", "lat": 42.3656, "lon": -71.0096},
    {"name": "Newark Liberty International", "code": "EWR", "lat": 40.6895, "lon": -74.1745}
]


# In[40]:


from geopy.distance import geodesic
# Step 1: Filter to planes on the ground with valid coordinates
df = df[df['on_ground'] == True]
df = df.dropna(subset=['latitude', 'longitude'])

# Step 2: Function to find nearest airport (within 5 miles)
def find_nearest_airport(lat, lon):
    for airport in airports:
        dist = geodesic((lat, lon), (airport["lat"], airport["lon"])).miles
        if dist <= 5:
            return airport["code"]
    return None

df["nearest_airport"] = df.apply(lambda row: find_nearest_airport(row['latitude'], row['longitude']), axis=1)


# In[41]:


df.head()


# In[46]:


df.shape


# ## Load Data to Warehouse

# In[42]:


import sqlalchemy as db


# In[43]:


#pip install psycopg2-binary


# In[44]:


engine=db.create_engine('postgresql://postgres:210598@localhost:5432/OpenSkyApi')


# In[47]:


df.to_sql("OpenSkyApi",engine,if_exists='append',index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




