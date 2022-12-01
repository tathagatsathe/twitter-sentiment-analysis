'''
Created on 22-Oct-2022

@author: tathagat
'''
# from collections.abc import Mapping;
import pandas as pd;
import numpy as np;
import streamlit as st;
import mysql.connector;

def init_connection():
    return mysql.connector.connect(**st.secrets["mysql"])

@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def sentiment_color(sentimentName):
    if(sentimentName == "Positive"):
        return "green"
    elif(sentimentName == "Negative"):
        return "red"
    else :
        return "black"
     
    
if __name__ == '__main__':
    conn = init_connection()
    
    rows = run_query("SELECT text,sentimentName from tweets ORDER BY created DESC;")

# Print results.
    for row in rows:
        # st.write(f"{row[0]} -----> {row[1]}:")
        st.markdown(f'<p style="color:{sentiment_color(row[1])};font-size:18px;border-bottom:1px solid grey; margin-top:10px;">{row[0]}</p>', unsafe_allow_html=True)

    
