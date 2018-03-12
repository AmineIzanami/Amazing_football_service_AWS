%pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import operator
import pandas as pd
import pandas as pd
import requests
import time


ops = { "+": operator.add, "-": operator.sub }

def summup(value):
        value = str(value)
        if '+' in value:
            tmp = [int(i) for i in (value.split("+"))]
            result = ops["+"](tmp[0],tmp[1])
            return str(result)
        if '-' in value:
            tmp = [int(i) for i in (value.split("-"))]
            result = ops["-"](tmp[0],tmp[1])
            return str(result)
        else :
            return value
			
def getlat(country):
    api_key_google = "my_google_key"
    api_response = requests.get(
        'https://maps.googleapis.com/maps/api/geocode/json?address={0}&key={1}'.format(country, api_key_google))
    api_response_dict = api_response.json()

    if api_response_dict['status'] == 'OK':
        latitude = api_response_dict['results'][0]['geometry']['location']['lat']
        return latitude
def getlng(country):
    api_key_google = "my_google_key"
    api_response = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address={0}&key={1}'.format(country, api_key_google))
    api_response_dict = api_response.json()

    if api_response_dict['status'] == 'OK':
        longitude = api_response_dict['results'][0]['geometry']['location']['lng']
        return longitude
		
dLat = {}
dLng = {}
def createDictCountryLat(dataframe):
    for i in range(dataframe['lat'].size):
        if dataframe["Country"][i] not in dLat:
            dLat[dataframe["Country"][i]] = getlat(dataframe["Country"][i])


def createDictCountryLng(dataframe):
    for i in range(dataframe['lat'].size):
        if dataframe["Country"][i] not in dLng:
            dLng[dataframe["Country"][i]] = getlng(dataframe["Country"][i])


spark = SparkSession.builder.appName('Data cleaning').getOrCreate()

# CLEANING - FORMATING - TRANSFORMING FOR PLAYER ATTRIBUTE DATA
df=spark.read.csv('s3://players-stats-s3/PlayerAttributeDataToBeProcessed/PlayerAttributeData.csv', mode="DROPMALFORMED",inferSchema=True, header = True)
new_pd_attribute = df.toPandas()
new_pd_attribute.rename(index=str, columns={"_c0": "Intern_id"}, inplace=True)
new_pd_attribute= new_pd_attribute.applymap(summup)
sdf = sqlContext.createDataFrame(new_pd_attribute)
sdf.write.save("s3://players-stats-transformed-s3/PlayerAttributeData_transformed", format='csv', header=True)

# CLEANING - FORMATING - TRANSFORMING FOR PLAYER PERSONAL DATA
df=spark.read.csv('s3://players-stats-s3/PlayerPersonalDataToBeProcessed/PlayerPersonalData.csv', mode="DROPMALFORMED",inferSchema=True, header = True)
new_pd_personal = df.toPandas()
new_pd_personal.rename(index=str, columns={"_c0": "Intern_id"}, inplace=True)
new_pd_personal.rename(index=str, columns={"Unnamed: 0": "Unnamed0"}, inplace=True)
sdf = sqlContext.createDataFrame(new_pd_personal)
sdf.write.save("s3://players-stats-transformed-s3/PlayerPersonalData_transformed", format='csv', header=True)

# CLEANING - FORMATING - TRANSFORMING FOR PLAYER POSITION DATA
df=spark.read.csv('s3://players-stats-s3/PlayerPlayingPositionDataToBeProcessed/PlayerPlayingPositionData.csv', mode="DROPMALFORMED",inferSchema=True, header = True)
new_pd_playerposition = df.toPandas()
new_pd_playerposition.rename(index=str, columns={"_c0": "Intern_id"}, inplace=True)
sdf = sqlContext.createDataFrame(new_pd_playerposition)
sdf.write.save("s3://players-stats-transformed-s3/PlayerPlayingPositionData_transformed", format='csv', header=True)




createDictCountryLat(new_pd_football_club)
createDictCountryLng(new_pd_football_club)
# TRANSFORMING FOR ALL PLAYERS AND ADD THE COUNTRY OF CLUB FOR 
# CLEANING - FORMATING - TRANSFORMING FOR PLAYER POSITION DATA
df=spark.read.csv('s3://players-stats-transformed-s3/Club_football/ClubFootball.csv', mode="DROPMALFORMED",inferSchema=True, header = True)
new_pd_football_club = df.toPandas()
new_pd_football_club['lat'] = 0
new_pd_football_club['lng'] = 0
for i in range(new_pd_football_club['lat'].size):
    lng = dLng(new_pd_football_club["Country"][i])
    new_pd_football_club['lat'][i] = dLat(new_pd_football_club["Country"][i])
    new_pd_football_club['lng'][i] = dLng(new_pd_football_club["Country"][i])
new_pd_merge = pd.merge(new_pd_personal, new_pd_football_club, on='Club', how='left')
Cleaned_pd_for_quicksight = pd.merge(new_pd_merge, new_pd_attribute, on='ID', how='left')
Cleaned_pd_for_quicksight = Cleaned_pd_for_quicksight[Cleaned_pd_for_quicksight['Age']<25]
Cleaned_pd_for_quicksight= Cleaned_pd_for_quicksight.drop(columns =['Intern_id','Unnamed0','Photo','Flag','Club Logo','extern_id'])
Cleaned_pd_for_quicksight = pd.merge(Cleaned_pd_for_quicksight, new_pd_playerposition, on='ID', how='left')
Cleaned_pd_for_quicksight.fillna("NaN")

df_GK = Cleaned_pd_for_quicksight[Cleaned_pd_for_quicksight['Preferred Positions'].str.contains("GK")].sort_values(['Potential'],ascending=False)
df_B = Cleaned_pd_for_quicksight[Cleaned_pd_for_quicksight['Preferred Positions'].str.contains("B")].sort_values(['Potential'],ascending=False)
df_M = Cleaned_pd_for_quicksight[Cleaned_pd_for_quicksight['Preferred Positions'].str.contains("M")].sort_values(['Potential'],ascending=False)
df_ST = Cleaned_pd_for_quicksight.loc[Cleaned_pd_for_quicksight['Preferred Positions'].isin(['ST','CF','LW','RW'])].sort_values(['Potential'],ascending=False)

# GET THE TOP PLAYERS FOR THE GOOD POSITION
top_players = [df_GK.iloc[0],
                  df_B.iloc[0],
                  df_B.iloc[1],
                  df_B.iloc[2],
                  df_B.iloc[3],
                  df_M.iloc[0],
                  df_M.iloc[1],
                  df_M.iloc[2],
                  df_M.iloc[3],
                  df_ST.iloc[0],
                  df_ST.iloc[1],
                  ]
cols = df_ST.columns.tolist()
top_players_dataframe = pd.DataFrame(top_players, columns=cols)
sdf = sqlContext.createDataFrame(top_players_dataframe)
sdf.write.save("s3://players-stats-transformed-s3/top_players_quicksight", format='csv', header=True)

