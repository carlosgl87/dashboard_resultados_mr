import pandas as pd
import streamlit as st
import numpy as np
from pymongo import MongoClient
import ssl
from bson.objectid import ObjectId
from datetime import date, timedelta
from google.oauth2 import service_account
from apiclient.discovery import build

pd.set_option('display.float_format', lambda x: '%.2f' % x)

######################
######## Data ########
######################

CONNECTION_STRING = "mongodb+srv://carlos:carlos123@cluster0.cdx6k.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client = MongoClient(CONNECTION_STRING, ssl_cert_reqs=ssl.CERT_NONE)
db = client['betpo-bel']

## Bets
collectionBets = db['bets']
cursorBets = collectionBets.find({})
resultBets = list(cursorBets)
dfBets = pd.DataFrame(resultBets)

## Contests
collectionContests = db['contests']
cursorContests = collectionContests.find({})
resultContests = list(cursorContests)
dfContests = pd.DataFrame(resultContests)

## Users
collectionUsers = db['users']
cursorUsers = collectionUsers.find({})
resultUsers = list(cursorUsers)
dfUsers = pd.DataFrame(resultUsers)

## depositos y retiros
num_depositos = 0
mon_depositos = 0
num_retiros = 0
mon_retiros = 0
df_retiros = pd.DataFrame(columns=['Usuario','Monto','Fecha'])
df_depositos = pd.DataFrame(columns=['Usuario','Monto','Fecha'])

######################
##### Analytics ######
######################

your_view_id = '269046440'
ga_keys = 'betpol-21eeb29ef8a9.json'

def format_summary(response):
    try:
        # create row index
        try: 
            row_index_names = response['reports'][0]['columnHeader']['dimensions']
            row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
            row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                        names = np.array(row_index_names))
        except:
            row_index_named = None
        
        # extract column names
        summary_column_names = [item['name'] for item in response['reports'][0]
                                ['columnHeader']['metricHeader']['metricHeaderEntries']]
    
        # extract table values
        summary_values = [element['metrics'][0]['values'] for element in response['reports'][0]['data']['rows']]
    
        # combine. I used type 'float' because default is object, and as far as I know, all values are numeric
        df = pd.DataFrame(data = np.array(summary_values), 
                          index = row_index_named, 
                          columns = summary_column_names).astype('float')
    
    except:
        df = pd.DataFrame()
        
    return df

def format_pivot(response):
    try:
        # extract table values
        pivot_values = [item['metrics'][0]['pivotValueRegions'][0]['values'] for item in response['reports'][0]
                        ['data']['rows']]
        
        # create column index
        top_header = [item['dimensionValues'] for item in response['reports'][0]
                      ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        column_metrics = [item['metric']['name'] for item in response['reports'][0]
                          ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        array = np.concatenate((np.array(top_header),
                                np.array(column_metrics).reshape((len(column_metrics),1))), 
                               axis = 1)
        column_index = pd.MultiIndex.from_arrays(np.transpose(array))
        
        # create row index
        try:
            row_index_names = response['reports'][0]['columnHeader']['dimensions']
            row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
            row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                        names = np.array(row_index_names))
        except: 
            row_index_named = None
        # combine into a dataframe
        df = pd.DataFrame(data = np.array(pivot_values), 
                          index = row_index_named, 
                          columns = column_index).astype('float')
    except:
        df = pd.DataFrame()
    return df

def format_report(response):
    summary = format_summary(response)
    pivot = format_pivot(response)
    if pivot.columns.nlevels == 2:
        summary.columns = [['']*len(summary.columns), summary.columns]
    
    return(pd.concat([summary, pivot], axis = 1))

def run_report(body, credentials_file):
    #Create service credentials
    credentials = service_account.Credentials.from_service_account_file(credentials_file, 
                                scopes = ['https://www.googleapis.com/auth/analytics.readonly'])
    #Create a service object
    service = build('analyticsreporting', 'v4', credentials=credentials)
    
    #Get GA data
    response = service.reports().batchGet(body=body).execute()
    
    return(format_report(response))

vista = {
  "reportRequests":
  [
    {
      "viewId": your_view_id,
      "dateRanges": [{"startDate": "2022-05-01", "endDate": "today"}],
      "metrics": [{"expression": "ga:users"},
                  {"expression": "ga:sessions"},
                  {"expression": "ga:newUsers"}],
       "dimensions": [
            {"name": "ga:pagePath"},
            {"name": "ga:date"}
            ]
    }
  ]
}

ga_report = run_report(vista, ga_keys)
ga_report = ga_report.reset_index()
ga_report['ga:date'] = pd.to_datetime(ga_report['ga:date'],format='%Y%m%d')
ga_report['indicador_contest_page'] = ga_report['ga:pagePath'].str.contains('/contest-detail-page')
ga_report['id_contest'] = ga_report['ga:pagePath'].apply(lambda x: x[x.find('-page/') + 6: 1000])

######################
######## Page ########
######################

st.set_page_config(
    page_title = 'PolBet Dashboard',
    page_icon = 'polbet.png',
    layout = 'wide'
)

filtro_usuarios = st.select_slider("Filtrar usuarios PolBet:", ["sinFiltro", "conFiltro"])


lista_usuarios_excluidos = [
'admin@gmail.com',
'byjgphotos@gmail.com',
'fk.salasf@gmail.com',
'luis@inka-labs.com',
'gabriella@email.com',
'devtorres@gmail.com',
'devtorres2@gmail.com']

lista_usuarios_excluidos_ids = dfUsers[dfUsers['email'].isin(lista_usuarios_excluidos)]['_id'].to_list()

if filtro_usuarios == 'conFiltro':
    st.write('Se esta filtrando a los usuarios relacionados a PolBet')
    dfUsers = dfUsers[~dfUsers['email'].isin(lista_usuarios_excluidos)]
    dfBets = dfBets[~dfBets['userId'].isin(lista_usuarios_excluidos_ids)]


for index, row in dfUsers.iterrows():
    name_user = row['email']
    lista_cuenta = dfUsers[dfUsers['email']==name_user]['balance_history'].values[0]
    if type(lista_cuenta) is list:
        for i in range(len(lista_cuenta)):
            if lista_cuenta[i]['state'] == 'approved':
                if lista_cuenta[i]['balanceType'] == 'deposit':
                    fecha = pd.to_datetime(lista_cuenta[i]['createdAt'])- pd.Timedelta(hours=5)
                    df_depositos.loc[num_depositos] = [name_user,lista_cuenta[i]['amount'],fecha]
                    num_depositos = num_depositos + 1
                    mon_depositos = mon_depositos + lista_cuenta[i]['amount']
                if lista_cuenta[i]['balanceType'] == 'withdrawal':
                    fecha = pd.to_datetime(lista_cuenta[i]['createdAt'])- pd.Timedelta(hours=5)
                    df_retiros.loc[num_retiros] = [name_user,lista_cuenta[i]['amount'],fecha]
                    num_retiros = num_retiros + 1
                    mon_retiros = mon_retiros + lista_cuenta[i]['amount']


dfBets['createdAt'] = dfBets['createdAt'] - pd.Timedelta(hours=5)
dfBets['contests'] = 1
dfBets['amount'] = pd.to_numeric(dfBets['amount'])
dfBets['potentialGain'] = pd.to_numeric(dfBets['potentialGain'])
dfBets = pd.merge(dfBets,dfContests[['_id','isContestOpenStatus']],how='left',left_on='contestId',right_on='_id')
dfBets['contest_open'] = dfBets['isContestOpenStatus'].map({True: 1, False: 0})
dfBets['contest_close'] = dfBets['isContestOpenStatus'].map({True: 0, False: 1})
dfBets['contest_winner'] = dfBets['winner'].map({True: 1, False: 0})
dfBets['contest_loser'] = dfBets['winner'].map({True: 0, False: 1})
dfBets['marginal_gain'] = dfBets['potentialGain'] - dfBets['amount']
dfBets['monto_curso'] = dfBets['contest_open'] * dfBets['amount']
dfBets['monto_perdido'] = dfBets['contest_close'] * dfBets['contest_loser'] * dfBets['amount']
dfBets['monto_ganado'] = dfBets['contest_close'] * dfBets['contest_winner'] * dfBets['marginal_gain']

dfBets_users = dfBets.groupby('userId').agg({'createdAt':'last','amount':'sum','monto_curso':'sum','monto_perdido':'sum','monto_ganado':'sum','contests':'sum'}).reset_index()
dfBets_users.rename(columns={'amount':'amount_bets','contests': 'contests_bets','createdAt':'createdAt_last_bet'}, inplace=True)
df_temp = pd.merge(dfUsers,dfBets_users,how='left',left_on='_id',right_on='userId')

## Main KPIS
num_users = len(dfUsers[dfUsers['role']=='user'])
num_users_active = len(df_temp[df_temp['amount_bets']>0])
total_amount_bets = df_temp['amount_bets'].sum()

## Secondary KPIS
total_events = len(dfContests)
total_events_active = len(dfContests[dfContests['isContestOpenStatus']==True])
users_events_active = len(dfBets[dfBets['contestId'].isin(list(dfContests[dfContests['isContestOpenStatus']==True]['_id']))]['userId'].unique())
number_bets_events_active = len(dfBets[dfBets['contestId'].isin(list(dfContests[dfContests['isContestOpenStatus']==True]['_id']))])
amount_bets_events_active = dfBets[dfBets['contestId'].isin(list(dfContests[dfContests['isContestOpenStatus']==True]['_id']))]['amount'].sum()

def dif_prob(id_event):
    df_odds = pd.DataFrame(columns=['Opcion','Probabilidad Pagina','Numero Apuestas','Monto Apuestas','Probabilidad Usuarios'])
    options_dict = dfContests[dfContests['_id']==id_event]['options'].reset_index(drop=True).loc[0]
    total_amount = float(dfBets[dfBets['contestId']==id_event]['amount'].sum())
    for i in list(range(len(options_dict))):
        opti = options_dict[i]['option_explanation']
        prob = options_dict[i]['probability']
        option_amount = float(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)]['amount'].sum())
        option_num_bets = len(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)])
        df_odds.loc[i] = [opti,prob,option_num_bets,option_amount,option_amount/total_amount]
    df_odds['error'] = df_odds['Probabilidad Pagina'] - df_odds['Probabilidad Usuarios']
    df_odds['error'] = df_odds['error'].abs()
    return df_odds['error'].sum()

## tabla resumen 
df_events_active = pd.DataFrame(columns=['ID','EVENTO','OPEN','NUM_PERSONAS','NUM_APUESTA','MONTO_APUESTA','TICKET_PROMEDIO','DIF_PROB'])
cont = 0
for index, row in dfContests.iterrows():
    id_event = row['_id']
    name_event = row['name']
    open_status = row['isContestOpenStatus']
    number_pers = len(dfBets[dfBets['contestId']==ObjectId(row['_id'])]['userId'].unique())
    number_bets = len(dfBets[dfBets['contestId']==ObjectId(row['_id'])])
    amount_bets = dfBets[dfBets['contestId']==ObjectId(row['_id'])]['amount'].sum()
    mean_amount = amount_bets / number_bets
    if amount_bets > 0:
        diferencia_probabilidades = dif_prob(row['_id'])
    else:
        diferencia_probabilidades = np.nan
    df_events_active.loc[cont] = [id_event,name_event, open_status, number_pers, number_bets,amount_bets,mean_amount,diferencia_probabilidades]
    cont = cont + 1

df_temp_1 = df_temp[['email','amount','monto_curso','monto_perdido','monto_ganado','contests_bets','createdAt_last_bet','createdAt']]
df_temp_1['createdAt_last_bet'] = df_temp_1['createdAt_last_bet'].dt.strftime('%y-%m-%d')
df_temp_1['createdAt'] = df_temp_1['createdAt'].dt.strftime('%y-%m-%d')
df_temp_1.rename(columns={'email': 'Usuario', 'amount':'Saldo','monto_curso': 'Apuestas en Curso','monto_perdido':'Monto Perdido','monto_ganado':'Monto Ganado','contests_bets':'Apuestas','createdAt_last_bet':'Fecha Ultima Apuesta','createdAt':'Fecha Registro'}, inplace=True)
df_temp_1 = df_temp_1.sort_values('Apuestas en Curso', ascending=False).reset_index(drop=True)




## First Rows KPIs
st.markdown("## Principales KPIs")
col1, col2, col3 = st.columns(3)
col1.metric("Numero Usuarios", num_users)
col2.metric("Numero Usuarios Activos", num_users_active)
col3.metric("Total Monto Apostado", int(total_amount_bets))

## Second Rows KPIs

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Eventos Totales", total_events)
col2.metric("Eventos Activos", total_events_active)
col3.metric("Usuarios Eventos Activos", users_events_active)
col4.metric("Numero Apuestas Eventos Activos", number_bets_events_active)
col5.metric("Monto Apuestas Eventos Activos", int(amount_bets_events_active))

## Third Rows KPIs

col1, col2, col3, col4 = st.columns(4)
col1.metric("Monto Depositos", mon_depositos)
col2.metric("Numero Depositos", num_depositos)
col3.metric("Monto Retiros", mon_retiros)
col4.metric("Numero Retiros", num_retiros)

## Tabla Eventos Usuarios y Montos

date_7_days = ga_report['ga:date'].max() - pd.Timedelta(days=7)
date_3_days = ga_report['ga:date'].max() - pd.Timedelta(days=3)

df_id_contest = ga_report[(ga_report['indicador_contest_page']==True)].groupby('id_contest').agg({'ga:sessions':'sum'}).reset_index()

# temp = ga_report[(ga_report['indicador_contest_page']==True)&(ga_report['ga:date'] >= date_3_days)].groupby('id_contest').agg({'ga:sessions':'sum'}).reset_index()
# temp = temp.rename(columns={'ga:sessions': 'ga:sessions_3_days'})
# df_id_contest = pd.merge(df_id_contest,temp,how='left',on='id_contest')
# df_id_contest['ga:sessions_3_days'] = df_id_contest['ga:sessions_3_days'].fillna(0) 

# temp = ga_report[(ga_report['indicador_contest_page']==True)&(ga_report['ga:date'] >= date_7_days)].groupby('id_contest').agg({'ga:sessions':'sum'}).reset_index()
# temp = temp.rename(columns={'ga:sessions': 'ga:sessions_7_days'})
# df_id_contest = pd.merge(df_id_contest,temp,how='left',on='id_contest')
# df_id_contest['ga:sessions_7_days'] = df_id_contest['ga:sessions_7_days'].fillna(0)

st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Eventos Activos")
df_events_active['ID'] = df_events_active['ID'].map(str)
df_events_active['ID'] = df_events_active['ID'].str.strip()
df_id_contest['id_contest'] = df_id_contest['id_contest'].map(str)
df_id_contest['id_contest'] = df_id_contest['id_contest'].str.strip()

df_events_active = pd.merge(df_events_active,df_id_contest,how='left',left_on='ID',right_on='id_contest')
print(df_events_active.head())
del df_events_active['ID']
del df_events_active['id_contest']
df_events_active['ga:sessions'] = df_events_active['ga:sessions'].fillna(0)
df_events_active = df_events_active.sort_values('MONTO_APUESTA', ascending=False).reset_index(drop=True)
st.dataframe(df_events_active)

## Evolucion entradas a la pagina
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Evolucion sesiones pagina web")

dfSessions = ga_report.groupby('ga:date').agg({'ga:sessions':'sum'}).reset_index()
today = date.today()
Dateslist = [today - timedelta(days = day) for day in range(20)]
df_days_sessions = pd.DataFrame(Dateslist,columns=['day'])
df_days_sessions['day'] = pd.to_datetime(df_days_sessions['day'])
df_days_sessions = pd.merge(df_days_sessions,dfSessions,how='left',left_on='day',right_on='ga:date')
del df_days_sessions['ga:date']
df_days_sessions['ga:sessions'] = df_days_sessions['ga:sessions'].fillna(0)
df_days_sessions['day'] = df_days_sessions['day'].dt.strftime('%y-%m-%d')
df_days_sessions = df_days_sessions.set_index('day')
st.bar_chart(df_days_sessions)


## Evolucion montos de apuestas ultimos 20 dias
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Evolucion montos de apuestas ultimos 20 dias")
dfBets['day'] = dfBets['createdAt'].dt.floor("D")
today = date.today()
Dateslist = [today - timedelta(days = day) for day in range(20)]
df_days_bets = pd.DataFrame(Dateslist,columns=['day'])
df_days_bets['day'] = pd.to_datetime(df_days_bets['day'])
df_days_bets = pd.merge(df_days_bets,dfBets.groupby('day').agg({'amount':'sum'}).reset_index(),how='left',on='day')
df_days_bets['amount'] = df_days_bets['amount'].fillna(0)
df_days_bets['day'] = df_days_bets['day'].dt.strftime('%y-%m-%d')
df_days_bets = df_days_bets.set_index('day')
st.bar_chart(df_days_bets)

## Tabla Usuarios Principales
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Usuarios Principales")
#st.dataframe(df_temp_1)
st.dataframe(df_temp_1.style.format({"Saldo": "{:.1f}","Apuestas en Curso": "{:.1f}","Monto Perdido":"{:.1f}","Monto Ganado":"{:.1f}","Apuestas": "{:.0f}"}))
#st.dataframe(df_temp_1.style.format({"Saldo": "{:.1f}", "Apuestas en Curso": "{:.1f}", "Monto Perdido":"{:.1f}","Monto Ganado":"{:.1f}","Apuestas": "{:.0f}"}))

######################
## SECCION USUARIOS ##
######################

st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Usuario")
temp = pd.merge(dfUsers[['_id','email']],dfBets.groupby('userId').agg({'amount':'sum'}).reset_index(),how='left',left_on='_id',right_on='userId')

option_user = st.selectbox(
    'Usuario',
    list(temp[temp['amount']>0]['email'].unique()))
id_user = dfUsers[dfUsers['email']==option_user]['_id'].reset_index(drop=True)[0]

df_actividad_usuario = pd.DataFrame(columns=['Evento','Open','Apuesta','Monto','Fecha'])
cont = 0
for index, row in dfBets[dfBets['userId']==id_user].iterrows():
    nom_evento = dfContests[dfContests['_id']==row['contestId']]['name'].reset_index(drop=True).loc[0]
    open_evento = dfContests[dfContests['_id']==row['contestId']]['isContestOpenStatus'].reset_index(drop=True).loc[0]
    df_actividad_usuario.loc[cont] = [nom_evento,open_evento,row['option'],row['amount'],row['createdAt']]
    cont =cont + 1
df_actividad_usuario['Fecha'] = df_actividad_usuario['Fecha'].dt.floor("D")
#df_actividad_usuario['Fecha'] = df_actividad_usuario['Fecha'].dt.strftime('%y-%m-%d')
st.dataframe(df_actividad_usuario.style.format({"Monto": "{:.2f}"}))

today = date.today()
Dateslist = [today - timedelta(days = day) for day in range(20)]
df_bets_user = pd.DataFrame(Dateslist,columns=['Fecha'])
df_bets_user['Fecha'] = pd.to_datetime(df_bets_user['Fecha'])
df_bets_user = pd.merge(df_bets_user,df_actividad_usuario.groupby('Fecha').agg({'Monto':'sum'}).reset_index(),how='left',on='Fecha')
df_bets_user['Monto'] = df_bets_user['Monto'].fillna(0)
#df_bets_user['Fecha'] = df_bets_user['Fecha'].dt.strftime('%y-%m-%d')
df_bets_user = df_bets_user.set_index('Fecha')
st.bar_chart(df_bets_user)

#####################
## SECCION EVENTOS ##
#####################

st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Evento")
option_event = st.selectbox(
    'Evento Activo',
    list(df_events_active['EVENTO']))
id_event = dfContests[dfContests['name']==option_event]['_id'].reset_index(drop=True)[0]

df_actividad_evento = pd.DataFrame(columns=['Evento','Open','Usuario','Apuesta','Monto','Fecha'])
cont = 0
for index, row in dfBets[dfBets['contestId']==id_event].iterrows():
    nom_evento = dfContests[dfContests['_id']==row['contestId']]['name'].reset_index(drop=True).loc[0]
    open_evento = dfContests[dfContests['_id']==row['contestId']]['isContestOpenStatus'].reset_index(drop=True).loc[0]
    nom_usuario = dfUsers[dfUsers['_id']==row['userId']]['email'].reset_index(drop=True).loc[0]
    df_actividad_evento.loc[cont] = [nom_evento,open_evento,nom_usuario,row['option'],row['amount'],row['createdAt']]
    cont = cont + 1
df_actividad_evento['Fecha'] = df_actividad_evento['Fecha'].dt.floor("D")
#df_actividad_evento['Fecha'] = df_actividad_evento['Fecha'].dt.strftime('%y-%m-%d')
st.dataframe(df_actividad_evento.style.format({"Monto": "{:.2f}"}))

today = date.today()
Dateslist = [today - timedelta(days = day) for day in range(20)]
df_bets_contest = pd.DataFrame(Dateslist,columns=['Fecha'])
df_bets_contest['Fecha'] = pd.to_datetime(df_bets_contest['Fecha'])
df_bets_contest = pd.merge(df_bets_contest,df_actividad_evento.groupby('Fecha').agg({'Monto':'sum'}).reset_index(),how='left',on='Fecha')
df_bets_contest['Monto'] = df_bets_contest['Monto'].fillna(0)
#df_bets_user['Fecha'] = df_bets_user['Fecha'].dt.strftime('%y-%m-%d')
df_bets_contest = df_bets_contest.set_index('Fecha')
st.bar_chart(df_bets_contest)




df_odds = pd.DataFrame(columns=['Opcion','Probabilidad Pagina','Numero Apuestas','Monto Apuestas','Probabilidad Usuarios'])
options_dict = dfContests[dfContests['_id']==id_event]['options'].reset_index(drop=True).loc[0]
total_amount = float(dfBets[dfBets['contestId']==id_event]['amount'].sum())
for i in list(range(len(options_dict))):
    opti = options_dict[i]['option_explanation']
    prob = options_dict[i]['probability']
    option_amount = float(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)]['amount'].sum())
    option_num_bets = len(dfBets[(dfBets['contestId']==id_event)&(dfBets['option']==opti)])
    df_odds.loc[i] = [opti,prob,option_num_bets,option_amount,option_amount/total_amount]

st.dataframe(df_odds.style.format({"Probabilidad Pagina": "{:.2f}", "Numero Apuestas": "{:.0f}",
                                   "Monto Apuestas": "{:.1f}", "Probabilidad Usuarios": "{:.2f}"}))


df_temp_2 = df_temp[df_temp['role']!='admin'][['email','amount_bets','contests_bets']]
df_temp_2.rename(columns={'email': 'Usuario', 'amount_bets': 'Monto Apostado','contests_bets':'Apuestas'}, inplace=True)
df_temp_2 = df_temp_2.sort_values('Monto Apostado', ascending=False).reset_index(drop=True)




## Tabla todos los usuarios
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Usuarios Registrados")
st.dataframe(df_temp_2.style.format({"Monto Apostado": "{:.1f}", "Apuestas": "{:.0f}"}))

## Tabla Depositos
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Depositos")
st.dataframe(df_depositos.style.format({"Monto": "{:.1f}"}))

## Tabla Retiro
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Retiros")
st.dataframe(df_retiros.style.format({"Monto": "{:.1f}"}))


##########################
## Bajar Archivo Marilu ##
##########################

df_balance_usuario = pd.DataFrame(columns=['usuario','tipo','estado','fecha','monto_apostado','monto'])
cont = 0
for index, row in dfUsers.iterrows():
    #name_user = 'cyjys87@gmail.com'
    name_user = row['email']
    #name_id = ObjectId('61a1b16a121e35f5b8278323')
    name_id = row['_id']
    lista_cuenta = dfUsers[dfUsers['email']==name_user]['balance_history'].values[0]
    
    df_balance_usuario.loc[cont] = [name_user,'regalo','regalo',np.nan,np.nan,20]
    cont = cont + 1
    
    if type(lista_cuenta) is list:
        for i in range(len(lista_cuenta)):
            if lista_cuenta[i]['state'] == 'approved':
                if lista_cuenta[i]['balanceType'] == 'deposit':
                    fecha = pd.to_datetime(lista_cuenta[i]['createdAt'])- pd.Timedelta(hours=5)
                    df_balance_usuario.loc[cont] = [name_user,'deposito','aprobado',fecha,lista_cuenta[i]['amount'],lista_cuenta[i]['amount']]
                    cont = cont + 1
                if lista_cuenta[i]['balanceType'] == 'withdrawal':
                    fecha = pd.to_datetime(lista_cuenta[i]['createdAt'])- pd.Timedelta(hours=5)
                    df_balance_usuario.loc[cont] = [name_user,'retiro','aprobado',fecha,lista_cuenta[i]['amount'],lista_cuenta[i]['amount']]
                    cont = cont + 1

    for index, row in dfBets[dfBets['userId']==name_id].iterrows():
        if dfContests[dfContests['_id']==row['contestId']]['isContestOpenStatus'].reset_index(drop=True).loc[0] == False:
            ganada = row['winner']
            fecha = pd.to_datetime(row['createdAt'])- pd.Timedelta(hours=5)
            if ganada:
                df_balance_usuario.loc[cont] = [name_user,'apuesta',ganada,fecha,row['amount'],row['potentialGain']]
                cont = cont + 1
            else:
                df_balance_usuario.loc[cont] = [name_user,'apuesta',ganada,fecha,row['amount'],row['amount']]
                cont = cont + 1
        else:
            fecha = pd.to_datetime(row['createdAt'])- pd.Timedelta(hours=5)
            df_balance_usuario.loc[cont] = [name_user,'apuesta','proceso',fecha,row['amount'],row['amount']]
            cont = cont + 1

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

csv = convert_df(df_balance_usuario)

st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Descargar Datos de Balance de Usuarios")

st.download_button(
     label="Descargar Datos",
     data=csv,
     file_name='Balance_usuarios.csv',
     mime='text/csv',
 )



## Simulacion Ganancia Cierre Apuesta
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Simulador cierre evento")

dfBets['amount'] = pd.to_numeric(dfBets['amount'])
dfBets['potentialGain'] = pd.to_numeric(dfBets['potentialGain'])

simulator_option_event = st.selectbox(
    'Evento',
    list(dfContests[dfContests['isContestOpenStatus']==True]['name']))

simulator_id_event = dfContests[dfContests['name']==simulator_option_event]['_id'].reset_index(drop=True)[0]
options_dict = dfContests[dfContests['_id']==simulator_id_event]['options'].reset_index(drop=True).loc[0]
options_list = []
for i in range(len(options_dict)):
    options_list.append(options_dict[i]['option_explanation'])

option_winner = st.selectbox(
    'Opcion Ganadora',
    options_list)

st.write('Evolucion montos apuestas: ', simulator_option_event)
st.write('Opcion Ganadora: ', option_winner)

amount_total = dfBets[dfBets['contestId']==simulator_id_event]['amount'].sum()
amount_winnet_total = dfBets[(dfBets['contestId']==simulator_id_event)&(dfBets['option']==option_winner)]['amount'].sum()
amount_winner_gain = dfBets[(dfBets['contestId']==simulator_id_event)&(dfBets['option']==option_winner)]['potentialGain'].sum() - amount_winnet_total
amount_loser = dfBets[(dfBets['contestId']==simulator_id_event)&(dfBets['option']!=option_winner)]['amount'].sum()

df_resultado_evento = pd.DataFrame(columns=['Concepto','Monto'])
df_resultado_evento.loc[0] = ['Monto Apostado',amount_total]
df_resultado_evento.loc[1] = ['Ganancia',amount_loser-amount_winner_gain]
df_resultado_evento.loc[2] = ['% Ganancia',(amount_loser-amount_winner_gain)/amount_total]

st.dataframe(df_resultado_evento.style.format({"Monto": "{:.2f}"}))




## Analisis Apuestas Cerradas
st.markdown("<hr/>",unsafe_allow_html=True)
st.markdown("## Analisis Apuestas Cerradas")

# KPIS Eventos Cerrados
num_eventos_cerrados = len(dfContests[dfContests['isContestOpenStatus']==False])
num_apuestas_cerradas = len(dfBets[dfBets['isContestOpenStatus']==False])
monto_apuestas_cerradas = dfBets[(dfBets['isContestOpenStatus']==False)]['amount'].sum()

num_apuestas_cerradas_winner = len(dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==True)])
num_apuestas_cerradas_losser = len(dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==False)])
monto_apuestas_cerradas_losser = dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==False)]['amount'].sum()
monto_apuestas_cerradas_winner = dfBets[(dfBets['isContestOpenStatus']==False)&(dfBets['winner']==True)]['marginal_gain'].sum()

col1, col2, col3 = st.columns(3)
col1.metric("Numero Eventos Cerrados", int(num_eventos_cerrados))
col2.metric("Numero Apuestas", int(num_apuestas_cerradas))
col3.metric("Monto Apuestas", int(monto_apuestas_cerradas))

col1, col2, col3, col4 = st.columns(4)
col1.metric("Monto Ganado", int(monto_apuestas_cerradas_winner))
col2.metric("Monto Perdido", int(monto_apuestas_cerradas_losser))
col3.metric("Apuestas Ganadas", int(num_apuestas_cerradas_winner))
col4.metric("Apuestas Perdidas", int(num_apuestas_cerradas_losser))
