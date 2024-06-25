import os
import pickle

import numpy as np
import pandas as pd
import nltk
from nltk import PorterStemmer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from langdetect import detect
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import WordNetLemmatizer
from nltk.tag import pos_tag
import pyLDAvis
import pyLDAvis.lda_model
from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from IPython.core.display import HTML
import pyLDAvis.gensim
#pyLDAvis.enable_notebook(local=True)


nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('vader_lexicon')
nltk.download('averaged_perceptron_tagger')

sia = SentimentIntensityAnalyzer()
lemmatizer = WordNetLemmatizer()

def get_sentiment(text):
    sentiment = sia.polarity_scores(text)
    return sentiment

string_cancellation_filter = 'The host canceled this reservation'
def filter_english_reviews(reviews):
    english_reviews = []
    for review in reviews:
        try:
            if detect(review) == 'en':
                english_reviews.append(review)
        except:
            pass
    return english_reviews


stop_words = set(stopwords.words('english'))
#dovremmo toglire dopo stemming
#stop_words.update(['nice', 'amsterdam', 'great', 'city', 'good','stay'])
lemmatizer = WordNetLemmatizer()

def preprocess(text):
    with open(r'C:\Users\flavi\PycharmProjects\lda_test\processed_data\host_names.pkl', 'rb') as name_file:
        host_names = pickle.load(name_file)
    tokens = nltk.word_tokenize(text)
    tokens = [word for word in tokens if word.isalpha()]
    tokens = ['host' if word in host_names else word for word in tokens]
    tokens = [lemmatizer.lemmatize(token) for token in tokens if token.lower() not in stop_words]
    #tagged_tokens = pos_tag(tokens)
    #no_adj_tokens = [word for word, pos in tagged_tokens if pos not in ['JJ', 'JJR', 'JJS']]
    #tokens = [ps.stem(token) for token in tokens]
    return ' '.join(tokens)

def generate_word_cloud_from_lda(lda, vectorizer, num_topics):
    for topic_idx, topic in enumerate(lda.components_):
        print(f"Topic {topic_idx + 1}:")
        topic_words = {vectorizer.get_feature_names_out()[i]: topic[i] for i in topic.argsort()[-20:]}
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(topic_words)
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title(f"Topic {topic_idx + 1}")
        plt.show()


def get_review_topics(review, vectorizer, lda_model):
    preprocessed_review = preprocess(review)
    review_vectorized = vectorizer.transform([preprocessed_review])
    topic_distribution = lda_model.transform(review_vectorized)
    topic_idx = topic_distribution.argmax()
    print(topic_distribution)
    print(topic_idx)
    return topic_idx

def read_and_apply_lda(file_path, column_index=5, num_topics=9, vectorizer = CountVectorizer(max_df=0.80, min_df=2, stop_words='english'), Mdtf = 90, mdtf = 2):
    global lda
    if not os.path.isfile(file_path):
        print(f"Errore: Il file {file_path} non esiste.")
        return

    try:
        df = pd.read_csv(file_path, nrows=419937)
        df['comments'] = df['comments'].fillna('')
        df = df[~df['comments'].str.startswith(string_cancellation_filter)]
    except Exception as e:
        print(f"Errore nella lettura del file CSV: {e}")
        return

    if column_index < len(df.columns):
        try:
            with open(r'C:\Users\flavi\PycharmProjects\lda_test\processed_data\processed_reviews_filtered.pkl', 'rb') as rf:
                english_reviews = pickle.load(rf)
            print('Prefiltered reviews loaded')
        except:
            print('data not found creating new data...')
            processed_docs = df.iloc[:, column_index].astype(str).apply(preprocess)
            english_reviews = filter_english_reviews(processed_docs.tolist())
            with open(r'C:\Users\flavi\PycharmProjects\lda_test\processed_data\processed_reviews_filtered.pkl', 'wb') as rf:
                pickle.dump(english_reviews, rf)

        dtm = vectorizer.fit_transform(english_reviews)

        lda = LatentDirichletAllocation(n_components=num_topics, verbose=1,max_iter= 100, n_jobs=4)
        lda.fit(dtm)
        with open(f'ldaS/lda_{num_topics}_Mdf{Mdtf}_mdf{mdtf}_100EPOCHS.pkl', 'wb') as f:
            pickle.dump(lda, f)

        with open(f'ldaS/vectorizer_{num_topics}_Mdf_{Mdtf}_{mdtf}_100EPOCHS.pkl', 'wb') as vf:
            pickle.dump(vectorizer, vf)

        generate_word_cloud_from_lda(lda, vectorizer, num_topics)

        for index, topic in enumerate(lda.components_):
            print(f"Topic {index + 1}:")
            print([vectorizer.get_feature_names_out()[i] for i in topic.argsort()[-20:]])
    else:
        print(f"Errore: La colonna {column_index + 1} non esiste nel file CSV.")

    return lda, dtm

def get_topic(topic_distribution):
    return topic_distribution.argmax()
def review_to_prhases_and_topics(review, lda, vectorizer,sentiment_analizer):
    sentences = nltk.sent_tokenize(text=review)
    review_matrix = []
    #processed_sentences = preprocess(sentences)
    for sentence in sentences:
        processed_sentence = [preprocess(sentence)]
        vect_sentence = vectorizer.transform(processed_sentence)
        sentence_topic_distribution = lda.transform(vect_sentence)
        topic = get_topic(sentence_topic_distribution)
        review_matrix.append([sentence, topic,sentiment_analizer.polarity_scores(sentence)])
    for el in review_matrix:
        print(el)
        print()


file_path = r'C:\Users\flavi\Desktop\data\reviews_details.csv'
vectorizer = CountVectorizer(max_df=0.90, min_df=10, stop_words='english')
lda,dmt = read_and_apply_lda(file_path, column_index=5, num_topics=8, vectorizer=vectorizer, mdtf=10, Mdtf=90)

with open(r'C:\Users\flavi\PycharmProjects\lda_test\processed_data\processed_reviews_filtered.pkl', 'wb') as out_dmt:
    pickle.dump(dmt,out_dmt)

#pyLDAvis.enable_notebook()
plot = pyLDAvis.lda_model.prepare(lda_model=lda, dtm=dmt, vectorizer=vectorizer)
pyLDAvis.show(plot)
with open('plots/lda_10_90_10_50EPOCHS.html', 'w') as rf:
    pyLDAvis.save_html(plot, rf)
    rf.close()


def train_all():
    upper_bounds = range(80, 100)
    lower_bounds = range(2, 11)
    number_of_topics = range(9,15)
    for n_topics in number_of_topics:
        for upper_bound in upper_bounds:
            for lower_bound in lower_bounds:
                vectorizer = CountVectorizer(max_df=upper_bound/100, min_df=lower_bound)
                lda, dmt = read_and_apply_lda(file_path, column_index=5, num_topics=n_topics, Mdtf=upper_bound, mdtf=lower_bound, vectorizer=vectorizer)
                plot = pyLDAvis.lda_model.prepare(lda_model=lda, dtm=dmt, vectorizer=vectorizer)
               # pyLDAvis.show(plot)
                with open(f'plots/lda_topics{n_topics}_Mdtf{upper_bound}_mdtf{lower_bound}.html', 'w') as rf:
                    pyLDAvis.save_html(plot, rf)

