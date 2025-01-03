from sqlalchemy import text
from collections import defaultdict
from sqlalchemy.exc import SQLAlchemyError
from collections import defaultdict
import pandas as pd
from DBController import LoadConfig, ConnectDB, ReadBatchFromDB, InsertWordPairsToDB
import re
import stanza


nlp = stanza.Pipeline('sv', processors='tokenize,pos,lemma', )
MinimumWindow = 2
MaximumWindow = 10

  
def CleanInputText(tweets, remove_urls=True, remove_special_chars=True, remove_digits=True):
    
    CleanedTweets = []
    for tweet in tweets:
        tweet = tweet.lower()
        if remove_urls:
            tweet = re.sub(r'http\S+|www\S+|https\S+', '', tweet, flags=re.MULTILINE)
        if remove_special_chars:
            tweet = re.sub(r'[^A-Za-zåäöÅÄÖ\s]', '', tweet)
        if remove_digits:
            tweet = re.sub(r'\d+', '', tweet)
        tweet = re.sub(r'\s+', ' ', tweet).strip()
        CleanedTweets.append(tweet)
    return CleanedTweets

def ProcessInputText(tweets, model):
    docs = nlp('\n'.join(tweets)).sentences
    CleanedLemmatizedText = [
        ' '.join(word.lemma for word in sentence.words if word.lemma)
        for sentence in docs
    ]
    return CleanedLemmatizedText





def GetConnections(InputText, MinimumWindow=MinimumWindow, MaximumWindow=MaximumWindow):
    
    #df = pd.read_sql(FilterQuery, engine, params=parameters)
    #InputText = DataFrame['text'].tolist()
    AllConnections = {size: defaultdict(int) for size in range(MinimumWindow, MaximumWindow + 1)} 
    #FinalText = CleanLemmatizeInputText(InputText)
    for Text in InputText:
        connections =  SlidingWindowWithOverlap(Text)
        for k, connection in connections.items():
            for word_pair, count in connection.items():
                AllConnections[k][word_pair] += count
    
    
    return AllConnections


def ProcessBatch(engine, batch_name):
    DataFrame = ReadBatchFromDB(engine, batch_name)
    InputText = DataFrame['text'].tolist()
    AllConnections = GetConnections(InputText)
    InsertData = [
            {'window_size': k, 'word1': pair[0], 'word2': pair[1], 'word_count': count}
            for k, connection in AllConnections.items()
            for pair, count in connection.items()
        ]
    return InsertData


def SlidingWindowWithOverlap(InputText):

    words = InputText.split()
    connections_by_window = {}
    for k in range(2, MaximumWindow + 1):
        connections = defaultdict(int)
        for i in range(len(words) - k + 1):
            window = words[i:i + k]
            if k == 2:
                word_pair = (window[0], window[1])
                reverse_pair = (window[1], window[0])
                if reverse_pair in connections:
                    connections[word_pair] += connections.pop(reverse_pair)
                else:
                    connections[word_pair] += 1
            else:
                last_word = window[-1]
                first_word = window[0]
                word_pair = (first_word, last_word)
                reverse_pair = (last_word, first_word)
                if reverse_pair in connections:
                    connections[word_pair] += connections.pop(reverse_pair)
                else:
                    connections[word_pair] += 1
        connections_by_window[k] = dict(connections)
    return connections_by_window


def SlidingWindowWithoutOverlap(InputText):
    #Not being used to process anything
    words = InputText.split()
    connections = defaultdict(int)
    for i in range(0, len(words) - MaximumWindow + 1, MaximumWindow):
        window = words[i:i + MaximumWindow]
        for j in range(MaximumWindow):
            for k in range(j + 1, MaximumWindow):
                word_pair = (window[j], window[k])
                reverse_pair = (window[k], window[j])
                if reverse_pair in connections:
                    connections[word_pair] += connections.pop(reverse_pair)
                else:
                    connections[word_pair] += 1
    return {pair: count for pair, count in connections.items() if count > 0}






if __name__ == '__main__':

    config = LoadConfig()  # Assuming you have a function that loads config
    engine = ConnectDB(config)  # Assuming this function connects to the DB
    InsertConnectionsToDB(engine, "aftonbladet")
