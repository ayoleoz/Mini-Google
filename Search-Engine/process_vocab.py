from numpy import append
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

stop_words = set(stopwords.words('english'))

f = open("google-10000-english-usa-no-swears.txt")
appendFile = open('no_stop_words.txt', 'a')

lines = f.read()
words = lines.split()
f.close()
for w in words:
    if not w in stop_words:
        appendFile.write(w+"\n")
appendFile.close()