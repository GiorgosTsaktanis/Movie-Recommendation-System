from elasticsearch import Elasticsearch,helpers
import pandas as pd
import csv
import os,sys,csv
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import keras
#ο κώδικας εδώ είναι ίδιος με την άσκηση 2 με την διαφορά ότι όταν δεν υπάρχει βαθμολογία
#για μια ταινία τότε αυτή προβλέπεται απο το νευρωνικό του κάθε χρήστη
def load_data(csv_name,index_name):
  curdir = os.path.dirname(__file__)
  csv_file = curdir+"/datasets/"+csv_name
  f = open(csv_file,"r")
  dictionar = csv.DictReader(f)
  res = helpers.bulk(es,dictionar,index=index_name)
  print("Data Loaded")    
def load_csv(csv_name):
    curdir = os.path.dirname(__file__)
    csv_file = curdir+"/datasets/"+csv_name
    csv = pd.read_csv(csv_file)
    return csv
def get_movies(phrase,num = 10):
  query_body = {
    "query":{
      "bool":{
        "should":[{
          "match":{
            "title":{
              "query":phrase,
              "fuzziness": "AUTO"
              }
          }},
          {
          "match":{
            "genres":{
              "query":phrase,
              "fuzziness": "AUTO"
              }
          }}
        ]
      }
    }
  }
  
  res = es.search(index="movies",body=query_body,size=num)
     
  return res  
#======================================================================================  
#δέχεται ως είσοδο το movieId και το νευρωνικό
#ψάχνει το κατάλληλο Input για το νευρωνικό, δηλαδή βρίσκει απο το Encodedmovies.csv
#την ταινία που αντιστοιχεί στο movieId που έχουμε απο την είσοδο
#και προβλέπει την βαθμολογία του χρήστη την οποία επιστρέφει με ακρίβεια 1 δεκαδικού ψηφίου
def getPrediction(movieId,model):
    encoded_movies = load_csv('encodedMovies.csv')
    movies = encoded_movies[encoded_movies["movieId"]==int(movieId)]
    movie = movies.drop(['movieId'],axis=1)
    evaluation = model.predict([movie])
    return round(evaluation[0][0],1)
#====================================================================================
def getUserEval(userId,movieId):
    query_body={
        "query": {
            "bool": {
                "must": [
                {
                    "match": {
                        "userId": userId
                    }
                },{
                    "match" : {
                        "movieId":movieId
                    }
                }
                    ]
            }
        }
    }
    res = es.search(index="ratings",body=query_body,filter_path=["hits.hits._source.rating"])
    if res:
        return float(res["hits"]["hits"][0]["_source"]["rating"])
    else: return -1

def getAverageEval(movieId):
    query_body={
  "query": {
    "bool": {
      "must": [
      {
        "match" : {
          "movieId":movieId
        }
        }
      ]
    }
  }
}
    res= es.search(index="ratings",body=query_body,size=671)
    
    n = len(res["hits"]["hits"])
    if n==0: return 0
    rating = 0
    for x in res["hits"]["hits"]:
        rating = rating + float(x["_source"]["rating"])

    return rating/n    

def print_movies(res):
    if not res:
        print("no results")
        return
    else:
        for x in res:
            print()
            print("===========================")
            print("ID: ",str(x["_id"]))
            print("Title: ",x["_source"]["title"])
            print("Genres: ",x["_source"]["genres"])
            print('Normalized Elastic Rating: %.2f' % x["_oldScore"])
            
            if x["_model"]:
                print('Users Rating: N/A')
                print('User predicted Rating: ',x["_userEval"])
            else:
                print('Users Rating: ',x["_userEval"])    
            print('Average Rating: %.2f / 5' % x["_movieEval"])
            print('New_Score: %.2f'% x["_score"],"/ 10")
           

def calculate_score(old_score,userEval,movieEval):
    if userEval>=0:
        return 6*old_score + 2*movieEval + 2*2*(userEval-0.5)
    else:
        return 6*old_score + 4*movieEval

def search_movie(phrase,userId,num=10):
    res = get_movies(phrase,num)
    if res["hits"]["total"]["value"]==0: return
    elastic_max_score = res["hits"]["max_score"]

    #===============load the user Model
    path = 'models/'+str(userId)
    user_model = keras.models.load_model(path)
    #=========================================
    for x in res["hits"]["hits"]:
        userEval = getUserEval(movieId=x["_id"],userId=userId)
        #αν το userEval= N/A δες την πρόβλεψη του νευρωνικού
        if userEval==-1:
            userEval = getPrediction(movieId=x["_id"],model = user_model)
            x["_model"] = True 
        movieEval = getAverageEval(x["_id"])
        x["_movieEval"]= movieEval
        old_score = x["_score"]/elastic_max_score
        x["_oldScore"] =  old_score
        if userEval>=0: x["_userEval"] = userEval
        else: x["_userEval"] = "N/A"
        new_score = calculate_score(old_score,userEval/5,movieEval/5)
        x["_score"] = new_score
        

    sorted_movies = sorted(res["hits"]["hits"], key=lambda x: x["_score"],reverse=True)
   
    return sorted_movies

        

#Τα csv  movies.csv και ratings.csv βρίσκονται ήδη στην elastic search
#απο τις προηγούμενες ασκήσεις 
if __name__=="__main__":
    es = Elasticsearch(host="localhost",port=9200,timeout=500)
    a = input("Search for a movie: ")
    b = int(input("User ID: "))
    res = search_movie(phrase=a,userId=b,num=10)
    print_movies(res)
