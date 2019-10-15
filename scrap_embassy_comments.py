# Define get links function and get all comments on south african embasy in harare
import bs4
from bs4 import BeautifulSoup
import requests
import pandas
from pandas import DataFrame
import csv
from google.cloud import bigquery

client = bigquery.Client()

#command to create a structure of csv file in which we will populate our scraped data

with open('c:\\embassy\\South_Africa_Embasy_in_Harare.csv', mode='w') as csv_file:
    fieldnames = ['Title', 'Para', 'Author', 'Date']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

#Creatibg empty lists of variables

#article_link = []
article_title = []
article_para = []
article_author = []
article_date = []
page_no = []

def scrap_comms(webpage, page_number):
    next_page = webpage.format(str(page_number))
    response = requests.get(str(next_page))
    soup = BeautifulSoup(response.content,"html.parser")
    soup_title = soup.findAll("div",{"class": "comment-username"})
    soup_para = soup.findAll("div",{"class": "comment-content"})
    soup_date = soup.findAll("div",{"class": "comment-date"})
    
    for x in range(len(soup_title)):
        article_author.append(soup_title[x].text.strip())
        article_date.append(soup_date[x].text.strip())
        # page_no.append(page_no)
        # article_link.append(soup_title[x].a['href'])
        # missing or blank titles
        if '<b>' in str(soup_para[x]):
            article_title.append(soup_para[x].b.text.strip())
            article_para.append(soup_para[x].text.split(soup_para[x].b.text)[1].strip())
        else:
            article_title.append("")
            article_para.append(soup_para[x].text)
    
#Generating the next page url
    
    if page_number < 100:
        page_number = page_number + 1
        scrap_comms(webpage, page_number)

#calling the function with relevant parameters

scrap_comms('https://embassy-finder.com/south-africa_in_harare_zimbabwe?page={}#comments', 0)

#creating the data frame and load a Bigquery table create in setp 1
data = {'Article_Date':article_date, 'Article_Author':article_author, 'Article_Title':article_title, 'Article_Para':article_para} # , 'Page_No':page_no}

df = DataFrame(data, columns = ['Article_Date','Article_Author','Article_Title','Article_Para']) #,'Page_No'])

# write to BQ
dataset_ref = client.dataset('embasy_pipeline')
table_ref = dataset_ref.table('stg_sa_emb_hre_comm')
client.load_table_from_dataframe(df, table_ref)
