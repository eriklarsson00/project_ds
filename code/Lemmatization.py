from collections import defaultdict
from DBController import LoadConfig, ConnectDB, ReadBatchFromDB, InsertWordPairsToDB
import re
import stanza

# Parameters
MinimumWindow = 2
MaximumWindow = 10

def LoadModel(language='sv', processors='tokenize,pos,lemma'):
    """
    Initialize the Stanza model locally.
    """
    print("Initializing Stanza model locally...")
    model = stanza.Pipeline(language, processors=processors)
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
    InsertData = [
        {'window_size': k, 'word1': pair[0], 'word2': pair[1], 'word_count': count}
        for k, connection in AllConnections.items()
        for pair, count in connection.items()
    ]
    return InsertData

def ProcessBatch(engine, batch_name):
    DataFrame = ReadBatchFromDB(engine, batch_name)
    InputText = DataFrame['text'].tolist()

    # Load model locally
    model = LoadModel()

    # Process text
    InputText = CleanInputText(InputText)
    ProcessedText = ProcessInputText(InputText, model)
    InsertData = GetConnections(ProcessedText)
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
    config = LoadConfig()
    engine = ConnectDB(config)
    DataFrame = ReadBatchFromDB(engine, "aftonbladet")
    print("Hello")
    print(DataFrame)
    InputText = DataFrame['text'].tolist()
    stanzaModel = LoadModel()
    CleanedText = CleanInputText(InputText)
    ProcessedText = ProcessInputText(CleanedText, stanzaModel)
    InsertData = GetConnections(ProcessedText)
    
    InsertWordPairsToDB(engine, InsertData)
