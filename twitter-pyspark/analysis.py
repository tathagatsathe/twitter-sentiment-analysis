from pyspark.sql import SparkSession
from flask import Flask, render_template
import os, io, base64, requests, csv
import pandas as pd, numpy as np
import plotly.graph_objs as go
# from plotly.subplots import make_subplots
from dotenv import load_dotenv
# from datetime import timedelta, datetime
import matplotlib.pyplot as plt
# from altair.examples.pyramid import df
import yfinance as yf
import pytz
# from statistics import mode
# from collections import Counter
from wordcloud import WordCloud, STOPWORDS
from PIL import Image
import nltk
from nltk.corpus import words
# from pyspark.examples.src.main.python.ml import stopwords_remover_example
nltk.download('words')
load_dotenv()

url='jdbc:mysql://localhost:3306/twitter'

user=os.getenv('user')
password=os.getenv('password')
jdbc_connector_jar = os.getenv('jdbc_connector_jar')

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {jdbc_connector_jar} pyspark-shell'

ticker = "TSLA"
company = "Tesla"
# stock_df = yf.download(ticker,period="1d", interval="2m")
# app = Flask(__name__)

spark = SparkSession.builder.appName("twitterApp").getOrCreate()

table = f"(select * from sentiments where tag='{company}') as tmp"
properties = {
    "driver": "com.mysql.jdbc.Driver",
    "user": user,
    "password": password
}
df = spark.read.jdbc(url=url, table=table, properties=properties)
df = df.toPandas()
labels = df['tag'].unique()
query = "(select tag, sentimentName, COUNT(*) as count from sentiments GROUP BY tag, sentimentName) as tmp"
sentiment_count_df = spark.read.jdbc(url=url, table=query, properties=properties)
sentiment_count_df = sentiment_count_df.toPandas()
sentiment_count_df = sentiment_count_df.groupby(["tag","sentimentName"])['count'].sum()
nested_dict = sentiment_count_df.unstack().to_dict()
df = df[df['tag']==company]
count_df = df.groupby(['sentimentName']).count()
stock_df = yf.download(ticker,interval="2m", start="2023-02-28",end="2023-03-01")


eastern = pytz.timezone('US/Eastern')
ist = pytz.timezone('Asia/Kolkata')
df['created_at'] = df['created_at'].dt.tz_localize(ist).dt.tz_convert(eastern)

df = df[(df['created_at']>=stock_df.index.min()) & (df['created_at']<=stock_df.index.max())]
tweets_df = df.groupby([pd.Grouper(key='created_at', freq='2T'),'sentimentName'])['text'].count().reset_index(['sentimentName'])

def create_wordcloud(text):
    english_words = set(words.words())
    stopwords = set(STOPWORDS)
    stopwords.update([ticker,company,'https','co'])
    text = ' '.join(word.lower() for word in text.split() if word.lower() in english_words)
    text =  ' '.join(word for word in text.split() if word.lower() not in stopwords)
    mask = np.array(Image.open('/home/tathagat/twitter/twitter-pyspark/twitter.png'))
    wordcloud = WordCloud(max_font_size=50, max_words=350, background_color="white",mask=mask).generate(text)
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode()

# text = ' '.join(df['text'].tolist())
# text = clean_text(text)

wordcloud = create_wordcloud(' '.join(df['text'].tolist()))

all_df = tweets_df.groupby(['created_at'])[['text']].sum()
neutral_df = tweets_df[tweets_df["sentimentName"]=="neutral"]
positive_df = tweets_df[tweets_df["sentimentName"]=="positive"]
negative_df = tweets_df[tweets_df["sentimentName"]=="negative"]

# @app.route('/plots')
def plots():
    # Create candlestick trace
    trace1 = go.Candlestick(x=stock_df.index, open=stock_df.Open, high=stock_df.High, low=stock_df.Low, close=stock_df.Close,yaxis='y1',name="price",showlegend=False)
    trace2 = go.Bar(x=stock_df.index, y=stock_df.Volume,marker_color='blue', opacity=0.5, name="",yaxis='y2',showlegend=False)
    trace3 = go.Scatter(x=all_df.index, y=all_df['text'],mode='lines', name='tweets',yaxis='y3',visible='legendonly',line=dict(color='blue'))
    trace4 = go.Scatter(x=positive_df.index, y=positive_df['text'],mode='lines', name='positive tweets',yaxis='y4',line=dict(color='green'),visible='legendonly')
    trace5 = go.Scatter(x=negative_df.index, y=negative_df['text'],mode='lines', name='negative tweets',yaxis='y5',line=dict(color='red'),visible='legendonly')
    trace6 = go.Scatter(x=neutral_df.index, y=neutral_df['text'],mode='lines', name='neutral tweets',yaxis='y6',line=dict(color='gray'),visible='legendonly')
    
    # Create layout
    layout = go.Layout(
        yaxis=dict(domain=[0.25, 1]),
        yaxis2=dict(domain=[0, 0.25], showgrid=False, side='left',showticklabels=False),
        yaxis3=dict(domain=[0.25,1], showgrid=False, overlaying='y', side='right',showticklabels=False),
        yaxis4=dict(domain=[0.25,1], showgrid=False, overlaying='y', side='right',showticklabels=False),
        yaxis5=dict(domain=[0.25,1], showgrid=False, overlaying='y', side='right',showticklabels=False),
        yaxis6=dict(domain=[0.25,1], showgrid=False, overlaying='y', side='right',showticklabels=False),
        # margin=dict(l=50, r=50, t=50, b=50, pad=4),
        xaxis_rangeslider_visible=False,
        title=company
    )
    
    fig = go.Figure(data=[trace1, trace2, trace3, trace4, trace5, trace6], layout=layout)
    plot_div = fig.to_html(full_html=False)
    
    label_colors = {'positive':'green','negative':'red','neutral':'blue'}
    pie_fig = go.Figure(go.Pie(labels=count_df.index, values=count_df.id, name='Pie chart',marker=dict(colors=[label_colors[label] for label in count_df.index])))
    pie_div = pie_fig.to_html(full_html=False)
    
    layout = go.Layout(xaxis={"title": "Companies"}, yaxis={"title": "No. of tweets"})
    barfig = go.Figure(layout=layout)
    for state in nested_dict:
        if state == "neutral":
            continue
        color = "red" if state=="negative" else "green" if state=="positive" else "blue"
        barfig.add_trace(go.Bar(x=list(nested_dict[state].keys()),y=list(nested_dict[state].values()),name=state,marker_color = color))
        
    bar_div = barfig.to_html(full_html=False)
    # Render the bar chart in an HTML template
    return render_template('plots.html',company=company, plot_div=plot_div,pie_div=pie_div,wordcloud=wordcloud,bar_div=bar_div)

# if __name__ == '__main__':
#     app.run(debug=True)
