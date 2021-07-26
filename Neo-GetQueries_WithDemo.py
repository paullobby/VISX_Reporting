#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import pandas as pd
import numpy as np
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
#import warnings
#warnings.filterwarnings('ignore')


# 1. CONNECTIONS

# CONNECTION TO GRAPHQL

# In[2]:


listVisxEvents='''
    {
  listVisxEvents (limit: 2000){
    items{
      event_id:id
      display_name
      from_date
      to_date
      url
      retailer_id
      store_id
      createdAt
      updatedAt
      shoppers{
          items{
              shopper_id
              email
              createdAt
          }
      }
      status
      type
    }
  }
}
'''

listStores = '''
{
  listStores{
    items{
      store_id:id
      store_name:name
      retailer{
        name
        id
        sandbox
      }
      createdAt
    }
  }
}
'''

listRetailers = '''
{
  listRetailers{
    items{
      id
      name
      sandbox
      convenience_fee_pct
      platform_commission_pct
      retailer_payment_type
      shopper_payment_type
      status
      stores{
          items{
              id
              name
      }
          
      }
    }
  }
}
'''


#CONNECTION
api_token = 'da2-55mm7r3uurhyvcd5f2eg45zuoq'
headers = {'x-api-key': api_token}
url = 'https://ef4sj5sa6nbktijcrgrbzjqgyy.appsync-api.us-east-1.amazonaws.com/graphql'

#connect to listRetailers query
r = requests.post(url, json={'query': listRetailers}, headers=headers)
print('listRetailers query is {}'.format(r.status_code))
json_data=json.loads(r.text)
df_data = json_data['data']['listRetailers']['items']
listRetailer = pd.io.json.json_normalize(df_data)

#connect to listEvents query
r = requests.post(url, json={'query': listVisxEvents}, headers=headers)
print('listEvents query is {}'.format(r.status_code))
json_data=json.loads(r.text)
df_data = json_data['data']['listVisxEvents']['items']
events = pd.io.json.json_normalize(df_data)

#connect to listStores query
r_ls = requests.post(url, json={'query': listStores}, headers=headers)
print('listStores query is {}'.format(r_ls.status_code))
#load data from listStores
json_data_ls=json.loads(r_ls.text)
df_data_ls = json_data_ls['data']['listStores']['items']
stores = pd.io.json.json_normalize(df_data_ls)
stores = stores.rename({'createdAt':'store_createdAt'},axis=1)

#GOOGLE SHEETS CONNECTION
# Configure the connection 
scope = ['https://spreadsheets.google.com/feeds']
# Give the path to the Service Account Credential json file 
credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/hans/Desktop/Main/Python/GRAPHQL/graphqlgs-a97c45fdc0d8.json',scope)
# Authorise your Notebook
gc = gspread.authorize(credentials)
# The sprad sheet ID, which can be taken from the link to the sheet
spreadsheet_key = '1J813SLIQ9eQrvOUQzvCr80Jzl2tZGlfJq9jf0Tn-xps'


# HARD CODED FAKE - NEEDS REMOVAL

# In[3]:



fakeEmails = [
'rberger@ibd.com',
'laura@onnyway.com',
'jlopardi@trinaturk.com',
'tiffanyyy1122@gmail.com',
'jason@comcapllc.com',
'laura@thegiddings.com',
'tiziana.lanza@yahoo.com',
'fdfgfc@gnail.com',
'yamujiang.tan@gmail.com',
'meline_wong@margiela.com',
'cameronckurtz@gmail.com',
'asdfg@gfdsa.org',
'laura@omnuway.com',
'burlingame@trinaturk.com',
'emily_ciafone@margiela.com',
'kelly.vitale@sbcglobal.net',
'skirksey@trinaturk.com',
'kurt.ivey@macerich.com',
'kcavaleri@trinaturk.com',
'annie_ng@marni.com',
'trinaturk@omnway.com',
'tiziana_lanza@margiela.com',
'scott.sutherlanf@ggp.com',
'camk1251@aol.com',
'nicole.zajdel@brookfieldpropertiesretail.com',
'accounting@hammermade.com',
'Micaela.wargo@brookfieldpropertiesretail.com',
'asummers@trinaturk.com',
'trinaturk3@test.com',
'trinaturk1@test.com',
'trinaturk2@test.com',
    'laura.collins@brookfieldpropertiesretail.com',
    'lindaxrobinson@gmail.com',
    'CAMERONCKURTZ@GMAIL.COM',
    'ty@gmail.com',
    'adrianjwenzel@gmail.com',
    '1@2.com',
    't@gmail.com',
    'GIOVANNA.VIEIRA@GANNI.DK',
    'khanmx99@gmail.com',
    'rose@rosenyc.me',
    'amitaabh.malhotra@gmail.com',
          'nisha@gmail.com',
      'swabha@gmail.com',
      'email@email.com',
      'nisha.kannoth@gmail.com',
      'swabha+ph@gmail.com',
    'lorilynjmiller@gmail.com',
    'tiffany@gmail.com'

    
]

#retailer_id
not_paying = [
    '6749e866-b8f2-487c-8b7a-3baa807db622',
    'f44ee744-fbb6-405c-b83c-a9b89c11af1d',
    'dfe36011-fd64-4930-83a3-f24635d26962',
    'bdd9d515-84c5-4979-8012-fccb9e2ad9bc',
    '7c3d110b-7fae-4323-8ef2-fb6ff4a3722f',
    '1fe518e7-8874-4538-9151-5e3cbcbffea1',
    'd06c4c4d-bc6d-4abe-991f-e402da55f762',
    '5e4ede79-9f94-4746-9493-1fcff7d24d2b',
    'c3ba5c4f-9351-41fb-b759-2ae5626af1a8'
    
]

deeptest = [ 
'c317f3e1-49bf-4787-93af-973e9bd93a3d',
'1fb08354-1e0a-47fe-ae17-a001be971b06',
'fd590a6b-0c58-4a15-becc-4f00c9bfe082',
'1b7ee4f9-d8b6-4312-b943-2cdc94c32f0b',
'5898e7f8-6abc-4055-924b-78fe014a2eac',
'7bb56ff9-bf54-4a55-aa5b-4e0c6da89513',
'9fc7a444-0a62-41c3-a25c-2d5a8da8620f',
'c65ce0c1-dc61-4ad8-b832-f4a7542263e2',
'43c40ab7-280b-4179-a8ba-9dbce8949b6a',
'e030f53b-10ed-4471-84dc-39c67fe0603a',
'cbe62a71-92ff-4143-9f93-a2ea9660d331'
]


retailer_email=['3nyboutiques.com',
                'danielpatrick.net',
                'margiela.com',
                'marni.com', 
                'trinaturk.com',
                'ganni.dk', 
                'ganni.com',
                'vince.com',
                'sammalouf.com',
                'allsaints.com', 
                'valentino.com',
                'thewebster.us',
                'evereve.com',
                'hammermade.com',
                'shagglund.com',
                'godashdot.com',
                'usa.maxmara.com',
                'maximasrl.it',
               'boutique.moncler.com',
               'sjk.com']


# 2.FUNCTIONS

# In[4]:


def getBagsByRetailer(a):
    getBagsByRetailer='''
    {{getBagsByRetailer(limit:2000, retailer_id:"{}")  {{
        items {{
          bag_id:id
          shopper_id
          createdAt
          updatedAt
          retailer_total
          total
          sub_total
          shipping_fee
          fulfillment_type
          payment{{
            createdAt
            updatedAt
          }}
          event{{
            id
            display_name
            from_date
            to_date
            url
            store{{
              id
              name
              retailer {{
                name
              }}
            }}
          }}
          shopper{{
            display_name
            email
            shipping_name
          }}
          shipping{{
          name,
          address_line_1
          address_line_2
          city
          state
          postal_code
          email
          }}
          line_items{{
            sku_id
            retailer_sku_id
            name
            quantity
            list_price
            price
            size
            color
          }}
          
        }}
      }}
    }}
    '''.format(a)
    
    #connect to listEvents query
    r = requests.post(url, json={'query': getBagsByRetailer}, headers=headers)
    print('getBagsByRetailer query is {}'.format(r.status_code))
    json_data=json.loads(r.text)
    df_data = json_data['data']['getBagsByRetailer']['items']
    bags = pd.io.json.json_normalize(df_data)
    return bags

def getShoppersByRetailer(a):
    getShoppersByRetailer='''
    {{getShoppersByRetailer(limit:2000, retailer_id:"{}")  {{
        items {{
          shopper_event_id:id
          shopper{{
            id
            display_name
            email
            phone
          }}
          createdAt
          updatedAt
          event{{
            id
            display_name
            from_date
            from_date_utc
            timezone
            to_date
            url
            store{{
              id
              name
              retailer {{
                name
              }}
            }}
          }}
          invited
          rsvp
          attended
          
        }}
      }}
    }}
    '''.format(a)
    
    #connect to listEvents query
    r = requests.post(url, json={'query': getShoppersByRetailer}, headers=headers)
    print('getShoppersByRetailer query is {}'.format(r.status_code))
    json_data=json.loads(r.text)
    df_data = json_data['data']['getShoppersByRetailer']['items']
    shoppers = pd.io.json.json_normalize(df_data)
    return shoppers

def getPaymentsByRetailer(a):
    getPaymentsByRetailer='''
    {{getPaymentsByRetailer(limit:2000, retailer_id:"{}") {{
        items{{
          id
          retailer_payment_id
          shopper_payment_id
          shopper_payment_auth_id
          createdAt
          updatedAt
          status
          bag{{
            id
            shopper_id
            shopper{{
             id
             email
            }}
            event_id
            sub_total
            tax
            retailer_total
            total
            service_fee
            shipping_fee
            shipping_method
            fulfillment_type
            event{{
                display_name
                from_date
                store{{
                    id
                    name
                    retailer{{
                        id
                        name
                    }}
                }}

            }}
            line_items{{
              retailer_sku_id
              name
              quantity
              list_price
              price
              size
              color
            }}
            shipping{{
                name
                address_line_1
                address_line_2
                city
                state
                postal_code
                email
            }}
          }}
          
        }}
      }}
    }}
    '''.format(a)
    
    #connect to getPaymentsByRetailer query
    r = requests.post(url, json={'query': getPaymentsByRetailer}, headers=headers)
    print('getPaymentsByRetailer query is {}'.format(r.status_code))
    json_data=json.loads(r.text)
    df_data = json_data['data']['getPaymentsByRetailer']['items']
    payments = pd.io.json.json_normalize(df_data)
    payments = payments.rename({'bag.shopper.id':'shopper_event_id',
                                'bag.shopper.email':'shopper.email',
                                'bag.event.from_date':'event.from_date',
                                'bag.line_items':'line_items',
                                'bag.event.display_name':'event.display_name',
                                'bag.total':'total',
                                'bag.retailer_total':'retailer_total',
                                'bag.sub_total':'sub_total',
                                'bag.service_fee':'service_fee',
                                'bag.shipping_fee':'shipping_fee',
                                'bag.event_id':'event.id'},axis=1)
    
    return payments

#filter only real and latest events
def clean_event(a):
    a = a[        (a['from_date'].str.contains('2021', regex=True))
                   & (~a['display_name'].str.contains('Test|demo|trial|rehearsal|example|rob',case=False, regex=True))
                    &(a['retailer_id'].isin(cleaned_listRetailer['id']))
                   #& (a['display_name'].str.len()>10)
         ]
    return a

def clean_event_all(a):
    a = a[    (~a['display_name'].str.contains('Test|demo|trial|rehearsal|example|rob',case=False, regex=True))
                    &(a['retailer_id'].isin(cleaned_listRetailer['id']))
                   #& (a['display_name'].str.len()>10)
         ]
    return a


def clean_stores(a):
    a = a[
        #(a['retailer.name'].notna())
        (a['retailer.id'].isin(cleaned_listRetailer['id']))]
    a = a[['store_id','store_name','retailer.name','store_createdAt']]
    
    return a

def clean_retailer(a):
    a = a[ (a['sandbox'].isnull())
&   (~a['id'].isin(deeptest))
&   (~a['id'].isin(not_paying))
         ]
    return a


# In[7]:


cleaned_listRetailer = clean_retailer(listRetailer)


# 3. EVENT LIST

# In[10]:


#cleaning for EVENTS - 2020 & 2021 ALL
shopper_list = events['shoppers.items'].str.len()
events['shoppers.count']=shopper_list
#merge with stores
esn = events.merge(stores, on="store_id",how="left")
esn = esn[['event_id', 'display_name', 'store_name', 'retailer.name','from_date','to_date','shoppers.count',
                                     'shoppers.items','createdAt', 'updatedAt', 'url', 'retailer_id', 'store_id','status','type']].copy()
#apply cleaning function
esn_all = clean_event_all(esn).sort_values('from_date',ascending=False)
#esn_all


# In[12]:


#cleaning for EVENTS - 2021 ONLY
esn_2021 = esn_all[(esn_all['from_date'].str.contains('2021', regex=True))]


# Store List For Store Monitoring

# In[14]:


#STORE - ALERTS

#cleaned_stores
cleaned_stores = clean_stores(stores)
cleaned_stores.sort_values(by='retailer.name', ascending=True, inplace=True)
#cleaned_stores.info()


# 

# 

# In[13]:


#remove fake and internal shopper emails
def removing(a):
    #split emails
    email = a['shopper.email']
    email_name=[]
    email_address=[]
    for e in email:
        x = e.split('@')[0].strip()
        y = e.split('@')[1].strip() #.split('.')[0]
        email_name.append(x)
        email_address.append(y)
        
    #email_name=map(lambda x:x.lower(),email_name)
    email_address= [x.lower() for x in email_address]
    a['email_name']=email_name
    a['email_address']=email_address

    a = a[(~a['email_address'].isin(retailer_email)) 
             & (a['email_address'] != 'omnyway.com')
             & (~a['shopper.email'].isin(fakeEmails))
             & (a['event.id'].isin(esn['event_id']) )
             & (a['email_name'].str.len()>3) 
            ].copy()
    return a 

def cleanBagList(a):
    #remove empty stuff
    a = a[a['total'].notna() & a['line_items'].notna() & a['event.id'].notna() & a['shopper.email'].notna()& a['event.display_name'].notna()]
#    a = a[        (a['event.from_date'].str.contains('2021', regex=True))
#                   & (~a['event.display_name'].str.contains('Test|demo|trial|rehearsal|example|rob',case=False, regex=True))]
    #total to divide by 100
    a['total'] = a['total'].div(100).replace({ 0 : np.nan })
    a['total'] = a['total'].round(2)
    a['retailer_total'] = a['retailer_total'].div(100).replace({ 0 : np.nan })
    a['retailer_total'] = a['retailer_total'].round(2)
    a['sub_total'] = a['sub_total'].div(100).replace({ 0 : np.nan })
    a['sub_total'] = a['sub_total'].round(2)
    #add num
    num_of_items = a['line_items'].str.len()
    a['num_of_items']=num_of_items
    a.drop(['event','payment','shopper'],axis=1,inplace=True)
    a.fillna(value='',inplace=True)
    return a

def cleanPayList(a):
    #remove empty stuff
    a = a[a['total'].notna() & a['line_items'].notna() & a['event.id'].notna() & a['shopper.email'].notna()& a['event.display_name'].notna()]
#    a = a[        (a['event.from_date'].str.contains('2021', regex=True))
#                   & (~a['event.display_name'].str.contains('Test|demo|trial|rehearsal|example|rob',case=False, regex=True))]
    #total to divide by 100
    a['total'] = a['total'].div(100).round(2)
    a['retailer_total'] = a['retailer_total'].div(100).round(2)
    a['sub_total'] = a['sub_total'].div(100).round(2)
    a['bag.tax'] = a['bag.tax'].div(100).replace({ 0 : np.nan })
    a['bag.tax'] = a['bag.tax'].round(2)
    a['shipping_fee'] = a['shipping_fee'].div(100).replace({ 0 : np.nan })
    a['shipping_fee'] = a['shipping_fee'].round(2)
    #a['service_fee'] = a['service_fee'].div(100).replace({ 0 : np.nan })
    #a['service_fee'] = a['service_fee'].round(2)

    num_of_items = a['line_items'].str.len()
    a['num_of_items']=num_of_items
    a.drop(['bag.shipping','bag.event'],axis=1,inplace=True)
    a.fillna(value='',inplace=True)
    return a
#,'bag.shopper'

def cleanPayListAll(a):
    #remove empty stuff
    #a = a[a['total'].notna() & a['line_items'].notna() & a['event.id'].notna() & a['shopper.email'].notna()& a['event.display_name'].notna()]
    a = a[a['total'].notna()]
    #total to divide by 100
    a['total'] = a['total'].div(100).round(2)
    a['retailer_total'] = a['retailer_total'].div(100).round(2)
    a['sub_total'] = a['sub_total'].div(100).round(2)
    a['bag.tax'] = a['bag.tax'].div(100).replace({ 0 : np.nan })
    a['bag.tax'] = a['bag.tax'].round(2)
    a['shipping_fee'] = a['shipping_fee'].div(100).replace({ 0 : np.nan })
    a['shipping_fee'] = a['shipping_fee'].round(2)
    a['service_fee'] = a['service_fee'].div(100).replace({ 0 : np.nan })
    #a['service_fee'] = a['service_fee'].round(2)
    #num_of_items = a['line_items'].str.len()
    #a['num_of_items']=num_of_items

    a.drop(['bag.shipping','bag.event'],axis=1,inplace=True)
    a.fillna(value='',inplace=True)
    return a

def cleanShopperList(a):
    a = a[ a['event.id'].notna() & a['shopper.email'].notna()]
#    a = a[        (a['event.from_date'].str.contains('2021', regex=True))
#                   & (~a['event.display_name'].str.contains('Test|demo|trial|rehearsal|example|rob',case=False, regex=True))]
    a = a[a['shopper.email'].str.contains(r'[^@]+@[^@]+\.[^@]+')]
    a.fillna(value='',inplace=True)
    return a


# 4. BAG LOOP

# In[19]:


#BAG LOOP
listRetailer = clean_retailer(listRetailer).copy()
listRetailers_cleaned = listRetailer['id']

masterBagList = []
for l in listRetailers_cleaned:
    i = getBagsByRetailer(l)
    masterBagList.append(i)
    total_bags=pd.concat(masterBagList,keys=listRetailer['name'])
t = total_bags
b = cleanBagList(total_bags)
c = removing(b)


c['createdAt'] = pd.to_datetime(c['createdAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")
c['updatedAt'] = pd.to_datetime(c['createdAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")
c['payment.createdAt'] = pd.to_datetime(c['payment.createdAt'], format="%Y-%m-%dT%H:%M:%S.%fZ",errors='coerce')
c.fillna(value='',inplace=True)



total_bags = c[['bag_id','shopper.email',  'event.display_name', 'event.store.name','event.store.retailer.name', 
                'total','retailer_total','sub_total','num_of_items',
                'fulfillment_type','createdAt','payment.createdAt','line_items',
                'shopper_id','event.id','shipping_fee', 'shopper.display_name',
                'shipping.name', 'shipping.address_line_1',
                'shipping.address_line_2', 'shipping.city', 'shipping.state',
                'shipping.postal_code', 'shipping.email','updatedAt','payment.updatedAt']].copy()


# In[20]:


total_bags


# 5. Bag Details

# In[21]:


#bag details
a = list(total_bags['line_items'])
b = list(total_bags['bag_id'])
sku_id = []
retailer_sku_id = []
sku_name = []
quantity = []
list_price = []
price = []
size = []
color = []
bag_id = []
for idx, l in enumerate(a):
    for i in l:
        map(''.join, i)
        sku_id.append(i['sku_id'])
        retailer_sku_id.append(i['retailer_sku_id'])
        sku_name.append(i['name'])
        quantity.append(i['quantity'])
        list_price.append(i['list_price'])
        price.append(i['price'])
        size.append(i['size'])
        color.append(i['color'])
        bag_id.append(b[idx])
        
        
        
bag_details = pd.DataFrame(
    {'sku_id':sku_id,
     'retailer_sku_id':retailer_sku_id,
     'sku_name':sku_name,
     'quantity':quantity,
     'list_price':list_price,
     'price':price,
     'size':size,
     'color':color,
     'bag_id':bag_id
    } 
)

bag_details = bag_details.merge(total_bags,on='bag_id',how='left')
#clean total to divide by 100
bag_details['price'] = bag_details['price'].div(100).round(2)
bag_details['list_price'] = bag_details['list_price'].div(100).round(2)
bag_details['fulfillment_type'].fillna(value='',inplace=True)
bag_details['payment.createdAt'].fillna(value='',inplace=True)


# 6. SHOPPER LOOP

# In[22]:


#Shopper GetQuery Loop
listRetailer = clean_retailer(listRetailer).copy()
listRetailers_cleaned = listRetailer['id']
masterShopperList = []
for l in listRetailers_cleaned:
    i = getShoppersByRetailer(l)
    masterShopperList.append(i)
    total_shoppers=pd.concat(masterShopperList,keys=listRetailer['name'])
b = cleanShopperList(total_shoppers)
c = removing(b)

c['createdAt'] = pd.to_datetime(c['createdAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")
c['updatedAt'] = pd.to_datetime(c['updatedAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")

shoppers = c[['shopper.email','event.id', 'shopper_event_id', 'createdAt','event.display_name',
                      'event.store.name','event.store.retailer.name','shopper.display_name','email_name','email_address','event.from_date','invited','rsvp','attended','updatedAt','event.timezone']].copy()

shoppers0 = c[['shopper.email','event.id', 'shopper_event_id', 'createdAt','event.display_name',
                      'event.store.name','event.store.retailer.name','shopper.display_name','email_name','email_address','event.from_date','invited','rsvp','attended','updatedAt','event.timezone','event.from_date_utc']].copy()

'''
shoppers['createdAt'] = pd.to_datetime(shoppers['createdAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")
shoppers['updatedAt'] = pd.to_datetime(shoppers['updatedAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")
'''


# 7. PAYMENT LOOP

# In[23]:


#Payments GetQuery Loop
listRetailer = clean_retailer(listRetailer).copy()
listRetailers_cleaned = listRetailer['id']
masterPaymentList = []
for l in listRetailers_cleaned:
    i = getPaymentsByRetailer(l)
    masterPaymentList.append(i)
    total_payments=pd.concat(masterPaymentList,keys=listRetailer['name'])
b = cleanPayList(total_payments)
c = removing(b)

c['createdAt'] = pd.to_datetime(c['createdAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")
c['updatedAt'] = pd.to_datetime(c['updatedAt'], format="%Y-%m-%dT%H:%M:%S.%fZ")

payments = c[['id', 'bag.event.store.retailer.id', 'createdAt', 'bag.event.store.id', 'bag.id',
              'event.id', 'sub_total', 'bag.tax', 'retailer_total', 'total','service_fee', 'shipping_fee', 
              'bag.fulfillment_type', 'shopper.email', 'line_items', 
              'bag.shipping.name', 'bag.shipping.address_line_1','bag.shipping.address_line_2', 
              'bag.shipping.city','bag.shipping.state', 'bag.shipping.postal_code', 
              'bag.shipping.email','num_of_items', 'email_name', 'email_address',
              'bag.shipping_method','event.display_name','event.from_date','status',
              'bag.event.store.retailer.name','bag.event.store.name','updatedAt']].copy()


# In[34]:


payments[['sub_total', 'bag.tax', 'retailer_total', 'total',
       'service_fee', 'shipping_fee','status','bag.event.store.retailer.name', 'bag.event.store.name','bag.shipping.name']]


# 8. PAID-LINE-ITEMS

# In[24]:


#create line_items dataframe with bag_id
a = list(payments['line_items'])
b = list(payments['id'])
retailer_sku_id = []
sku_name = []
quantity = []
list_price = []
price = []
size = []
color = []
pay_id = []
for idx, l in enumerate(a):
    for i in l:
        map(''.join, i)
        retailer_sku_id.append(i['retailer_sku_id'])
        sku_name.append(i['name'])
        quantity.append(i['quantity'])
        list_price.append(i['list_price'])
        price.append(i['price'])
        size.append(i['size'])
        color.append(i['color'])
        pay_id.append(b[idx])
        
        
        
pay_details = pd.DataFrame(
    {'retailer_sku_id':retailer_sku_id,
     'sku_name':sku_name,
     'quantity':quantity,
     'list_price':list_price,
     'price':price,
     'size':size,
     'color':color,
     'id':pay_id
    } 
)

pay_details = pay_details.merge(payments,on='id',how='left')


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





# APPENDEX

# 

# 

# 

# 

# 

# 

# 

# 

# 

# 

# 

# PRINT TO GOOGLE SHEETS

# In[25]:


wks_name = 'events'
cell_of_start_df = 'A1'
d2g.upload(esn_all,
           spreadsheet_key,
           wks_name,
           credentials=credentials,
           col_names=True,
           row_names=False,
           start_cell = cell_of_start_df,
           clean=False)
print ('The sheet is updated successfully')


# In[26]:


wks_name = 'Stores-Alert'
cell_of_start_df = 'A1'
# upload the dataframe of the clients we want to delete
d2g.upload(cleaned_stores,
           spreadsheet_key,
           wks_name,
           credentials=credentials,
           col_names=True,
           row_names=False,
           start_cell = cell_of_start_df,
           clean=False)
print ('The sheet is updated successfully')


# In[27]:


wks_name = 'bags_raw_2021'
cell_of_start_df = 'A1'
d2g.upload(total_bags,
           spreadsheet_key,
           wks_name,
           credentials=credentials,
           col_names=True,
           row_names=False,
           start_cell = cell_of_start_df,
           clean=False)
print ('The sheet is updated successfully')


# In[28]:


wks_name = 'details'
cell_of_start_df = 'A1'
# upload the dataframe of the clients we want to delete
d2g.upload(bag_details,
           spreadsheet_key,
           wks_name,
           credentials=credentials,
           col_names=True,
           row_names=False,
           start_cell = cell_of_start_df,
           clean=False)
print ('The sheet is updated successfully')


# In[352]:


wks_name = 'shoppers'
cell_of_start_df = 'B1'
d2g.upload(shoppers,
           spreadsheet_key,
           wks_name,
           credentials=credentials,
           col_names=True,
           row_names=False,
           start_cell = cell_of_start_df,
           clean=False)
print ('The sheet is updated successfully')


# In[29]:


wks_name = 'Paid_items'
cell_of_start_df = 'A1'
# upload the dataframe of the clients we want to delete
d2g.upload(payments,
           spreadsheet_key,
           wks_name,
           credentials=credentials,
           col_names=True,
           row_names=False,
           start_cell = cell_of_start_df,
           clean=False)
print ('The sheet is updated successfully')


# In[30]:


wks_name = 'pay_details'
cell_of_start_df = 'A1'
# upload the dataframe of the clients we want to delete
d2g.upload(pay_details,
           spreadsheet_key,
           wks_name,
           credentials=credentials,
           col_names=True,
           row_names=False,
           start_cell = cell_of_start_df,
           clean=False)
print ('The sheet is updated successfully')


# In[ ]:




