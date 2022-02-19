from elasticsearch import Elasticsearch
import pandas as pd
import os,sys,csv

##Για εγκατάσταση των libraries που χρειάζονται
## 1) cmd στον αρχικό φάκελο
## 2) pip install -r requirements.txt



#movies.columns -> Id,title,genre
#movies.isna().sum() -->> Δεν εχω κενά δεδομένα
#movies["movieId"].nunique() ειναι μοναδικη η κάθε ID αρα μπορώ
#να την χρησιμοποιήσω για id της elastic
# έτσι όταν χρειάζεται η αναζήτηση μιας ταινίας μέσω Id
# θα γίνεται πιο γρήγορα, ωστόσο δεν έγινε χρήση του bulk εδώ
# οπότε απαιτείται παραπάνω χρόνος κατα την προσθήκη του Index.
def load_data(csv_name,index_name,keys):
  

    print("loading data...")
    curdir = os.path.dirname(__file__)
    csv_file = curdir+"/datasets/"+csv_name
    df = pd.read_csv(csv_file).to_dict("records")
    
    for c,item in enumerate(df,1):
      entry={}
      for key in keys:
          entry[key] = item.get(key)          
      es.index(index = index_name,id=item.get("movieId"),body = entry)

    print("Data Loaded")    

#Αν δεν επιλέξω την παράμετρο Num επιστρέφει ενα response από την elastic search
#με τις 10 πιο συναφείς ταινίες.
#Επειδή υπάρχει το fuzziness μπορεί η είσοδος να έχει και ορθογραφικά λάθη
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

#Τυπώνει τις ταινίες απο το response της elastic search
def print_movies(res):
    if res["hits"]["total"]["value"]==0: print("no results")
    else:
        for x in res["hits"]["hits"]:
            
            print("\n==================================\n")
            print("Title: ",x["_source"]["title"])
            print("Genres: ",x["_source"]["genres"])
            print("ID: ",str(x["_id"]))
            print("Score: ",x["_score"])
   


if __name__=="__main__":
  #Σύνδεση με elastic search
  es = Elasticsearch(host="localhost",port=9200,timeout=500)
  #φόρτωση των ταινιών στην elastic search
  load_data("movies.csv","movies",["title","genres"])
  
  a  = input("Search for a movie: ")
  res = get_movies(a,num=10)
  print_movies(res)
