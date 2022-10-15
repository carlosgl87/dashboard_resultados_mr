from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId
import pandas as pd
from datetime import date, timedelta
import numpy as np
import ast
import streamlit as st

pd.set_option('display.float_format', lambda x: '%.2f' % x)


######################
##### Functions ######
######################

def posicionTag(dfTags, i,TotalX,TotalY):
  w = dfTags['tag'].loc[i]['w']
  h = dfTags['tag'].loc[i]['h']
  InitialX = dfTags['tag'].loc[i]['startX']
  InitialY = dfTags['tag'].loc[i]['startY']
  if w < 0:
    InitialX = InitialX + w
    w = w * -1
  if h < 0:
    InitialY = InitialY + h
    h = h * -1
  if InitialX < 0:
    InitialX = 0
  if InitialY < 0:
    InitialY = 0
  InitialX = InitialX/TotalX
  InitialY = InitialY/TotalY
  w = w/TotalX
  h = h/TotalY
  return [InitialX,InitialY,InitialX + w, InitialY + h]

def isRectangleOverlap(R1, R2):
  if (R1[0]>=R2[2]) or (R1[2]<=R2[0]) or (R1[3]<=R2[1]) or (R1[1]>=R2[3]):
    return False
  else:
    return True

def posicionAWS(i,dfTextExtractSelection):
  Left = dfTextExtractSelection.loc[i]['BoundingBox']['Left']
  Top = dfTextExtractSelection.loc[i]['BoundingBox']['Top']
  Width = dfTextExtractSelection.loc[i]['BoundingBox']['Width']
  Height = dfTextExtractSelection.loc[i]['BoundingBox']['Height']
  return [Left,Top,Left+Width,Top+Height]


######################
######## Data ########
######################

CONNECTION_STRING = "mongodb+srv://carlos:carlos123@cluster2.cdx6k.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(CONNECTION_STRING)
db = client['clinic']

collectionTagType = db['tagType']
cursorTagType = collectionTagType.find({})
resultTagType = list(cursorTagType)
dfTagType = pd.DataFrame(resultTagType)
dfTagType['_id'] = dfTagType['_id'].map(str)
dfTagType = dfTagType.rename(columns={'_id': 'tagID','tag':'tagTitle'})

collectionTags = db['tags']
cursorTags = collectionTags.find({})
resultTags = list(cursorTags)
dfTags = pd.DataFrame(resultTags)
df = dfTags["tag"].astype('str')
df = df.apply(lambda x: ast.literal_eval(x))
df = df.apply(pd.Series)
df = df.rename(columns={'tag': 'tagID'})
dfTags = dfTags.join(df)
dfTags = pd.merge(dfTags,dfTagType, how='left',on='tagID')

collectionTextExtract = db['textExtract']
cursorTextExtract = collectionTextExtract.find({})
resultTextExtract = list(cursorTextExtract)
dfTextExtract = pd.DataFrame(resultTextExtract)
df = dfTextExtract["Geometry"].astype('str')
df = df.apply(lambda x: ast.literal_eval(x))
df = df.apply(pd.Series)
dfTextExtract = dfTextExtract.join(df)

######################
#### UI Streamlit ####
######################

## SUMMARY TABLE
st.header('SUMMARY')

df_temp = dfTags.groupby('fileName').agg({'_id':'count'}).reset_index()
df_temp['Number Documents'] = 1
df_temp = df_temp.rename(columns={'fileName': 'Case Name','_id':'Number Tags'})
df_temp = df_temp[['Case Name','Number Documents','Number Tags']]
df_temp = df_temp.sort_values('Number Tags',ascending=False)

st.dataframe(data=df_temp)

## DETAILS
st.header('DETAILS')

documento = st.selectbox(
    '¿De qué documento quiere ver los tags?',
    [x[:-4] for x in list(dfTags['fileName'].unique())if x not in '9781234 (1).pdf'])

## DISPLAY EACH SECTION

st.subheader('Family History')

dfTagsSelection = dfTags[dfTags['fileName']=='Caso_2_1652446328707-1.pdf']
for text in dfTagsSelection[dfTagsSelection['tagTitle']=='Family History']['text'].unique():
  text = "- " + text 
  st.markdown(text)



# st.subheader('Case Summary')




# st.subheader('Key Studies and Interventions')




# st.subheader('History of Present Illness')



# st.subheader('Past Medical History')



# st.subheader('Past Surgical History')



# st.subheader('Social History - EtOH')


# st.subheader('Social History - Smoking Status')


# st.subheader('Social History - Illicit Substance Use')


# st.subheader('Family History')


# st.subheader('Allergies')



# st.subheader('Medications')





# df_interseccion = pd.DataFrame(columns=['summary_tag','summary_text','text_extract'])
# dfTagsSelection = dfTags[dfTags['fileName']==documento+'.pdf'].reset_index(drop=True)
# dfTextExtractSelection = dfTextExtract[dfTextExtract['Documento']==documento].reset_index(drop=True)
# cont = 0
# for i in range(len(dfTagsSelection)):
#   pagina = dfTagsSelection.loc[i]['page']
#   TotalY = dfTagsSelection['pageHeight'].loc[0]
#   TotalX = dfTagsSelection['pageWidth'].loc[0]
#   tag_eval = posicionTag(dfTagsSelection,i,TotalX,TotalY)
#   summary_tag = dfTagType[dfTagType['_id']==ObjectId(dfTagsSelection.loc[i]['tag']['tag'])].reset_index(drop=True)['tag'].loc[0]
#   summary_text = dfTagsSelection.loc[i]['tag']['text']
#   for j in list(dfTextExtractSelection[dfTextExtractSelection['Page']==pagina].index):
#     if isRectangleOverlap(tag_eval,posicionAWS(j,dfTextExtractSelection)):
#       text_extract = dfTextExtractSelection.loc[j]['Text']
#       df_interseccion.loc[cont] = [summary_tag,summary_text,text_extract]
#       cont = cont + 1

# st.dataframe(data=df_interseccion)