from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId
import pandas as pd
from datetime import date, timedelta
import numpy as np
import ast
import streamlit as st
import boto3
import time
import pandas as pd
from botocore.exceptions import NoCredentialsError
import requests
import mimetypes

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

## Textract APIs used - "start_document_text_detection", "get_document_text_detection"
def InvokeTextDetectJob(s3BucketName, objectName):
    response = None
    #client = boto3.client('textract')
    client = boto3.client("textract",aws_access_key_id="AKIAXZJ4IUZWWT34UQ5G",aws_secret_access_key='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', region_name="us-east-1")
    response = client.start_document_text_detection(
            DocumentLocation={
                      'S3Object': {
                                    'Bucket': s3BucketName,
                                    'Name': objectName
                                }
           })
    return response["JobId"]

def CheckJobComplete(jobId):
    time.sleep(5)
    #client = boto3.client('textract')
    client = boto3.client("textract",aws_access_key_id="AKIAXZJ4IUZWWT34UQ5G",aws_secret_access_key='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', region_name="us-east-1")
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))
    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))
    return status

def JobResults(jobId):
    pages = []
    #client = boto3.client('textract')
    client = boto3.client("textract",aws_access_key_id="AKIAXZJ4IUZWWT34UQ5G",aws_secret_access_key='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', region_name="us-east-1")
    response = client.get_document_text_detection(JobId=jobId)
 
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
        while(nextToken):
            response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
            pages.append(response)
            print("Resultset page recieved: {}".format(len(pages)))
            nextToken = None
            if('NextToken' in response):
                nextToken = response['NextToken']
    return pages

def process_document_aws(file_name):
  # S3 Document Data
  s3BucketName = "polbet-assets-dev"
  documentName = file_name
  print(documentName)
  # Function invokes
  jobId = InvokeTextDetectJob(s3BucketName, documentName)
  print("Started job with id: {}".format(jobId))
  if(CheckJobComplete(jobId)):
      response = JobResults(jobId)
  
  print(response)
  df_temp = pd.DataFrame()
  lista_num = []
  lista_tex = []
  lista_pag = []
  lista_geo = []
  for resultPage in response:
      for item in resultPage["Blocks"]:
          if item["BlockType"] == "LINE":
              lista_num.append(round(item['Confidence'],4))
              lista_tex.append(item['Text'])
              lista_pag.append(item['Page'])
              lista_geo.append(item['Geometry'])

  df_temp['Confidence'] = lista_num
  df_temp['Page'] = lista_pag
  df_temp['Text'] = lista_tex
  df_temp['Geometry'] = lista_geo

  df = df_temp["Geometry"].astype('str')
  df = df.apply(lambda x: ast.literal_eval(x))
  df = df.apply(pd.Series)
  df_temp = df_temp.join(df)

  return df_temp, response

def upload_file(remote_url,file_name):
  bucket_name = "polbet-assets-dev"
  s3 = boto3.client('s3', aws_access_key_id="AKIAXZJ4IUZWWT34UQ5G", aws_secret_access_key="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")
  try:
      imageResponse = requests.get(remote_url, stream=True).raw
      content_type = imageResponse.headers['content-type']
      extension = mimetypes.guess_extension(content_type)
      s3.upload_fileobj(imageResponse, bucket_name, file_name[:-4] + extension)
      print("Upload Successful")
      return True
  except FileNotFoundError:
      print("The file was not found")
      return False
  except NoCredentialsError:
      print("Credentials not available")
      return False

def insert_textExtract_mongo(response,file_name):
  CONNECTION_STRING = "mongodb+srv://carlos:carlos123@cluster2.cdx6k.mongodb.net/?retryWrites=true&w=majority"
  client = MongoClient(CONNECTION_STRING)
  db = client['clinic']
  collection = db.textExtract
  total_documentos = 0
  for resultPage in response:
    for item in resultPage["Blocks"]:
        if item["BlockType"] == "LINE":
          diccionario = {}
          diccionario['Documento'] = file_name
          diccionario['Confidence'] = round(item['Confidence'],4)
          diccionario['Text'] = item['Text']
          diccionario['Page'] = item['Page']
          diccionario['Date'] = pd.to_datetime("today")
          diccionario['Geometry'] = item['Geometry']
          collection.insert_one(diccionario)
          total_documentos = total_documentos + 1
  print('Se han insertado {} documentos'.format(total_documentos))
  return total_documentos

def run_write_textExtract(fileName):
  url = dfTags[dfTags['fileName']==fileName].reset_index()['fileURL'][0]
  upload_file(url,fileName)
  df_temp, response = process_document_aws(fileName)
  insert_textExtract_mongo(response,fileName[:-4])
  return True

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

collectionCase = db['case']
cursorCase = collectionCase.find({})
resultCase = list(cursorCase)
dfCase = pd.DataFrame(resultCase)
dfCase['_id'] = dfCase['_id'].map(str)
dfCase = dfCase.rename(columns={'_id': 'caseId','name':'nameCase'})

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
dfTags = pd.merge(dfTags,dfCase, how='left',on='caseId')

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

def main_page():
  ## SUMMARY TABLE
  st.header('SUMMARY')

  df_temp = dfTags.groupby('nameCase').agg({'_id':'nunique','fileName':'nunique'}).reset_index()
  df_temp = df_temp.rename(columns={'nameCase': 'Case Name','fileName':'Number Documents','_id':'Number Tags'})
  
  df_temp = df_temp[['Case Name','Number Documents','Number Tags']]
  dfCasesDocuments = pd.DataFrame(columns=['Document'])
  dfCasesDocuments['Document'] = dfTags['fileName'].unique()
  temp = pd.merge(dfCasesDocuments,dfTags[['fileName','nameCase']],how='left',left_on='Document',right_on='fileName')
  temp = temp.groupby(['Document']).agg({'nameCase':'first'}).reset_index()
  temp = temp[temp['nameCase']!='Gerson']
  temp = temp.dropna().sort_values('nameCase').reset_index()

  lista_textExtract = []
  for index,row in temp.iterrows():
    name = row['Document']
    name = name[:-4]
    textExtract = len(dfTextExtract[dfTextExtract['Documento']==name])
    if textExtract == 0:
      lista_textExtract.append('No')
    else:
      lista_textExtract.append('Yes')
  temp['TextExtract'] = lista_textExtract
  temp = temp[['nameCase','Document','TextExtract']]
  st.dataframe(data=temp)
  documentsList = list(temp[temp['TextExtract']=='No']['Document'].unique())
  option = st.selectbox(
    'Which document process with TextExtract?',
    documentsList)

  st.write('You selected:', option)

  def add(a):
        #c = a+b
        return a

  if st.button('Run TextExtract Algorithm'):
    result = add(str(option))
    st.write('result: %s' % result)
  else:
    st.write('No action')

  ## DETAILS
  st.header('DETAILS')

  documento = st.selectbox(
      '¿What Case do you want to select?',
      [x[:-4] for x in list(df_temp['Case Name'].unique())if x not in '9781234 (1).pdf'])

  ## DISPLAY EACH SECTION

  dfTagsSelection = dfTags[dfTags['nameCase']=='Cari_Woodford']


  for caseTitle in list(dfTagsSelection['tagTitle'].unique()):
    st.subheader(caseTitle)
    for text in list(dfTagsSelection[dfTagsSelection['tagTitle'] == caseTitle]['text'].unique()):
      st.markdown(text)
      temp = dfTagsSelection[dfTagsSelection['text'] == text]
      df_interseccion = pd.DataFrame(columns=['Document','Page','Text','Position'])
      cont = 0
      for i in list(temp.index):
        documento = temp.loc[i]['fileName']
        pagina = temp.loc[i]['page']
        TotalY = temp['pageHeight'].loc[i]
        TotalX = temp['pageWidth'].loc[i]
        tag_eval = posicionTag(dfTagsSelection,i,TotalX,TotalY)
        dfTextExtractSelection = dfTextExtract[dfTextExtract['Documento']==documento[:-4]].reset_index(drop=True)
        print(tag_eval)
        text_extract = ''
        for j in list(dfTextExtractSelection[dfTextExtractSelection['Page']==pagina].index):
          if isRectangleOverlap(tag_eval,posicionAWS(j,dfTextExtractSelection)):
            text_extract = text_extract + dfTextExtractSelection.loc[j]['Text']
        #print(text_extract)
        df_interseccion.loc[cont] = [documento,pagina,text_extract,tag_eval]
        cont = cont + 1
        #print(text_extract)
      st.dataframe(data=df_interseccion)
    #  print(df_interseccion)

  #dfTagsSelection = dfTags[dfTags['fileName']=='Caso_2_1652446328707-1.pdf']
  #for text in dfTagsSelection[dfTagsSelection['tagTitle']=='Family History']['text'].unique():
  #  text = "- " + text 
  #  st.markdown(text)
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

def page2():
  df_temp = dfTags.groupby('nameCase').agg({'tagID':'nunique','fileName':'nunique'}).reset_index()
  df_temp = df_temp.rename(columns={'nameCase': 'Case Name','fileName':'Number Documents','tagID':'Number Tags'})
  df_temp = df_temp[['Case Name','Number Documents','Number Tags']]
  documento = st.selectbox(
  '¿What Case do you want to select?',
  [x[:-4] for x in list(df_temp['Case Name'].unique())if x not in '9781234 (1).pdf'])
  
  dfCasedfTagsSelection = dfTags[dfTags['nameCase']=='Cari_Woodford']
  for tagTitle in list(dfCasedfTagsSelection['tagTitle'].unique()):
    st.subheader(tagTitle)
    texto = list(dfCasedfTagsSelection[dfCasedfTagsSelection['tagTitle']==tagTitle]['text'].unique())
    st.markdown('. '.join(texto))


page_names_to_funcs = {
    "Main Page": main_page,
    "Printable version": page2
}

selected_page = st.sidebar.selectbox("Select a page", page_names_to_funcs.keys())
page_names_to_funcs[selected_page]()

