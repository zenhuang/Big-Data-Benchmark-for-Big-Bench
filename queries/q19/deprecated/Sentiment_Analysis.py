import nltk


def get_words_in_tweets(tweets):
    
    all_words = []
    for (words, sentiment) in tweets:
        all_words.extend(words)
    
    return all_words


def get_word_features(wordlist):

    wordlist = nltk.FreqDist(wordlist)
    word_features = wordlist.keys()
    
    return word_features


def extract_features(document):

    document_words = set(document)
    features = {}

    word_features = get_word_features(document_words)

    for word in word_features:
        features['contains(%s)' % word] = (word in document_words)

    return features



def extract_sentiment(tweet):
    #if __name__ == "__main__":
    
    pos_tweets = [('I love my cell phone', 'positive'),
                  ('The display is amazing', 'positive'),
                  ('This coffee makes me feel great', 'positive'),
                  ('I am very excited for Nexus 5', 'positive'),
                  ('This device is my new best friend', 'positive')]    
    
    neg_tweets = [('I do not like these headphones', 'negative'),
                  ('This sound is horrible', 'negative'),
                  ('This product makes me sick', 'negative'),
                  ('I hate this tablet!', 'negative'),                  
                  ('I am disappointed with this product', 'negative'),
                  ('I wouldn\'t pay half as much for something like \
                  this again', 'negative')]    

    # irrelevant information is treated as neutral
    neg_tweets = [('This product is okay', 'neutral'),
                  ('so-so', 'neutral'),
                  ('Does not exceed expectations', 'neutral'),
                  ('Meets my needs', 'neutral'),                  
                  ('I am content, not happy', 'neutral')]

    tweets = []
    
    for (words, sentiment) in pos_tweets + neg_tweets:
        words_filtered = [e.lower() for e in words.split() if len(e) >= 3] 
        tweets.append((words_filtered, sentiment))    
            
    training_set = nltk.classify.apply_features(extract_features, tweets)

    classifier = nltk.NaiveBayesClassifier.train(training_set)
    
    #print label_probdist.prob('positive')
    #print classifier.show_most_informative_features(32)
    
    #tweet = 'I like this computer'
    
    return classifier.classify(extract_features(tweet.split()))

    
if __name__=="__main__":
    
    print(extract_sentiment("meets my needs"))