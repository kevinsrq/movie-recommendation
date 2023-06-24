import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

# Download the stopwords corpus and WordNet corpus if not already downloaded
nltk.download('stopwords', './references', quiet=True)
nltk.download('wordnet', './references', quiet=True)

# Set the English stopwords and initialize the WordNet lemmatizer
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

# Function to remove stopwords from text
def remove_stopwords(text):
    if isinstance(text, float) and text != text:  # Check if the text is NaN
        tokens = word_tokenize(text)  # Tokenize the text into words
        tokens_sem_stopwords = (token for token in tokens if token.lower() not in stop_words)  # Remove stopwords
        texto_sem_stopwords = ' '.join(tokens_sem_stopwords)  # Join the remaining tokens back into text
        return texto_sem_stopwords
    else:
        return text

# Function to lemmatize text
def lemmatize_text(text):
    if isinstance(text, float) and text != text:  # Check if the text is NaN
        # Lemmatize each word in the text and join them back into a string
        return ' '.join(lemmatizer.lemmatize(word) for word in text.split())
    else:
        return text
