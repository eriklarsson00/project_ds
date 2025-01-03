import redis
import pickle
from sqlalchemy import text
from collections import defaultdict
from DBController import LoadConfig, ConnectDB, ReadBatchFromDB
import re
import stanza

# Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parameters
MinimumWindow = 2
MaximumWindow = 10

def LoadModel(language='sv', processors='tokenize,pos,lemma'):
    """
    Load the model from Redis or initialize it if not present.
    """
    model_key = f"stanza_model_{language}"
    model_pickle = redis_client.get(model_key)

    if model_pickle:
        print("Loading Stanza model from Redis...")
        model = pickle.loads(model_pickle)
    else:
        print("Initializing Stanza model...")
        model = stanza.Pipeline(language, processors=processors)
        redis_client.set(model_key, pickle.dumps(model))
    return model

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
    docs = model('\n'.join(tweets)).sentences
    CleanedLemmatizedText = [
        ' '.join(word.lemma for word in sentence.words if word.lemma)
        for sentence in docs
    ]
    return CleanedLemmatizedText

def GetConnections(InputText, MinimumWindow=MinimumWindow, MaximumWindow=MaximumWindow):
    AllConnections = {size: defaultdict(int) for size in range(MinimumWindow, MaximumWindow + 1)}
    for Text in InputText:
        connections = SlidingWindowWithOverlap(Text)
        for k, connection in connections.items():
            for word_pair, count in connection.items():
                AllConnections[k][word_pair] += count
    return AllConnections

def ProcessBatch(engine, batch_name):
    DataFrame = ReadBatchFromDB(engine, batch_name)
    InputText = DataFrame['text'].tolist()

    # Load model from Redis or initialize
    model = LoadModel()

    # Process text
    InputText = CleanInputText(InputText)
    ProcessedText = ProcessInputText(InputText, model)
    AllConnections = GetConnections(ProcessedText)

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

if __name__ == '__main__':
    config = LoadConfig()  # Assuming you have a function that loads config
    engine = ConnectDB(config)  # Assuming this function connects to the DB
    ProcessBatch(engine, "aftonbladet")
