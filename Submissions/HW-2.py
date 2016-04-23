# Name: Michael Galarnyk
# Email: A09134209 
# PID: A09134209
from pyspark import SparkContext
sc = SparkContext()
# coding: utf-8

# # Homework 2
# The data is represented as rows of of [JSON](https://en.wikipedia.org/wiki/JSON#Example) strings.
# It consists of [tweets](https://dev.twitter.com/overview/api/tweets), [messages](https://dev.twitter.com/streaming/overview/messages-types), and a small amount of broken data (cannot be parsed as JSON).
# 
#     * 1GB test: `../Data/hw2-files-1gb.txt`
#     * 5GB test: `../Data/hw2-files-5gb.txt`
#     * 20GB test: `../Data/hw2-files-20gb.txt`
# 
# ## Tweets
# 
# A tweet consists of many data fields. [Here is an example](https://gist.github.com/arapat/03d02c9b327e6ff3f6c3c5c602eeaf8b). You can learn all about them in the Twitter API doc. We are going to briefly introduce only the data fields that will be used in this homework.
# 
# * `created_at`: Posted time of this tweet (time zone is included)
# * `id_str`: Tweet ID - we recommend using `id_str` over using `id` as Tweet IDs, becauase `id` is an integer and may bring some overflow problems.
# * `text`: Tweet content
# * `user`: A JSON object for information about the author of the tweet
#     * `id_str`: User ID
#     * `name`: User name (may contain spaces)
#     * `screen_name`: User screen name (no spaces)
# * `retweeted_status`: A JSON object for information about the retweeted tweet (i.e. this tweet is not original but retweeteed some other tweet)
#     * All data fields of a tweet except `retweeted_status`
# * `entities`: A JSON object for all entities in this tweet
#     * `hashtags`: An array for all the hashtags that are mentioned in this tweet
#     * `urls`: An array for all the URLs that are mentioned in this tweet
# 
# 
# ## Data source
# 
# All tweets are collected using the [Twitter Streaming API](https://dev.twitter.com/streaming/overview).
# 
# 
# ## Users partition
# 
# Besides the original tweets, we will provide you with a Pickle file, which contains a partition over 452,743 Twitter users. It contains a Python dictionary `{user_id: partition_id}`. The users are partitioned into 7 groups.

# # Part 0: Load data to a RDD

# The tweets data is stored on AWS S3. We have in total a little over 1 TB of tweets. We provide 10 MB of tweets for your local development. For the testing and grading on the homework server, we will use different data.
# 
# ## Testing on the homework server
# In the Playground, we provide three different input sizes to test your program: 1 GB, 10 GB, and 100 GB. To test them, read files list from `../Data/hw2-files-1gb.txt`, `../Data/hw2-files-5gb.txt`, `../Data/hw2-files-20gb.txt`, respectively.
# 
# For final submission, make sure to read files list from `../Data/hw2-files-final.txt`. Otherwise your program will receive no points.
# 
# ## Local test
# 
# For local testing, read files list from `../Data/hw2-files.txt`.
# Now let's see how many lines there are in the input files.
# 
# 1. Make RDD from the list of files in `hw2-files.txt`.
# 2. Mark the RDD to be cached (so in next operation data will be loaded in memory) 
# 3. call the `print_count` method to print number of lines in all these files

# In[1]:

import string
def print_count(rdd):
    print 'Number of elements:', rdd.count()


# In[2]:

# server: ../Data/hw2-files-20gb.txt
# local: ../Data/hw2-files.txt

with open('../Data/hw2-files-final.txt') as f:
    files = (line.translate(None, "\n") for line in f)
    RDD = sc.textFile(','.join(files)).cache()
print_count(RDD)


# # Part 1: Parse JSON strings to JSON objects

# Python built-in json library is too slow. In our experiment, 70% of the total running time is spent on parsing tweets. Therefore we recommend using [ujson](https://pypi.python.org/pypi/ujson) instead of json. It is at least 15x faster than the built-in json library according to our tests.

# ## Broken tweets and irrelevant messages
# 
# The data of this assignment may contain broken tweets (invalid JSON strings). So make sure that your code is robust for such cases.
# 
# In addition, some lines in the input file might not be tweets, but messages that the Twitter server sent to the developer (such as [limit notices](https://dev.twitter.com/streaming/overview/messages-types#limit_notices)). Your program should also ignore these messages.
# 
# *Hint:* [Catch the ValueError](http://stackoverflow.com/questions/11294535/verify-if-a-string-is-json-in-python)
# 
# 
# (1) Parse raw JSON tweets to obtain valid JSON objects. From all valid tweets, construct a pair RDD of `(user_id, text)`, where `user_id` is the `id_str` data field of the `user` dictionary (read [Tweets](#Tweets) section above), `text` is the `text` data field.

# starting time: 10.9 ms <br>
# 

# In[3]:

import ujson
def safe_parse(raw_string):
    dummy_tuple = ('', '') # Dummy values to filter
    try:
        parse = ujson.loads(raw_string)
        if 'text' in parse and 'user' in parse: 
            return (parse['user']['id_str'].encode('utf-8'), parse['text'].encode('utf-8'))
        else: 
            return dummy_tuple 
    except ValueError, e:
        return dummy_tuple


# In[4]:

parseRDD = RDD.map(lambda x: safe_parse(x)).filter(lambda x: x[0] != '').cache()


# (2) Count the number of different users in all valid tweets (hint: [the `distinct()` method](https://spark.apache.org/docs/latest/programming-guide.html#transformations)).

# In[5]:

def print_users_count(count):
    print 'The number of unique users is:', count


# starting time: 87.9 ms <br>

# In[6]:

count = parseRDD.map(lambda x:x[0]).distinct().count()
print_users_count(count)


# # Part 2: Number of posts from each user partition

# Load the Pickle file `../Data/users-partition.pickle`, you will get a dictionary which represents a partition over 452,743 Twitter users, `{user_id: partition_id}`. The users are partitioned into 7 groups. For example, if the dictionary is loaded into a variable named `partition`, the partition ID of the user `59458445` is `partition["59458445"]`. These users are partitioned into 7 groups. The partition ID is an integer between 0-6.
# 
# Note that the user partition we provide doesn't cover all users appear in the input data.

# (1) Load the pickle file.

# original timing: 2.51s

# In[7]:

import cPickle as pickle


# In[8]:

with open('../Data/users-partition.pickle') as dic_file:
    partitionDict = pickle.load(dic_file)
partition_Dict_Broad = sc.broadcast(partitionDict)


# (2) Count the number of posts from each user partition
# 
# Count the number of posts from group 0, 1, ..., 6, plus the number of posts from users who are not in any partition. Assign users who are not in any partition to the group 7.
# 
# Put the results of this step into a pair RDD `(group_id, count)` that is sorted by key.

# original timing: 677 ms <br>
# 

# In[9]:

#Count group by value
counts = parseRDD.map(lambda x:partition_Dict_Broad.value.get(x[0], 7)).countByValue().items()


# (3) Print the post count using the `print_post_count` function we provided.

# In[10]:

def print_post_count(counts):
    for group_id, count in counts:
        print 'Group %d posted %d tweets' % (group_id, count)


# In[11]:

print_post_count(counts)


# # Part 3:  Tokens that are relatively popular in each user partition

# In this step, we are going to find tokens that are relatively popular in each user partition.
# 
# We define the number of mentions of a token $t$ in a specific user partition $k$ as the number of users from the user partition $k$ that ever mentioned the token $t$ in their tweets. Note that even if some users might mention a token $t$ multiple times or in multiple tweets, a user will contribute at most 1 to the counter of the token $t$.
# 
# Please make sure that the number of mentions of a token is equal to the number of users who mentioned this token but NOT the number of tweets that mentioned this token.
# 
# Let $N_t^k$ be the number of mentions of the token $t$ in the user partition $k$. Let $N_t^{all} = \sum_{i=0}^7 N_t^{i}$ be the number of total mentions of the token $t$.
# 
# We define the relative popularity of a token $t$ in a user partition $k$ as the log ratio between $N_t^k$ and $N_t^{all}$, i.e. 
# 
# \begin{equation}
# p_t^k = \log \frac{C_t^k}{C_t^{all}}.
# \end{equation}
# 
# 
# You can compute the relative popularity by calling the function `get_rel_popularity`.

# (0) Load the tweet tokenizer.

# In[12]:

# %load happyfuntokenizing.py
#!/usr/bin/env python

"""
This code implements a basic, Twitter-aware tokenizer.

A tokenizer is a function that splits a string of text into words. In
Python terms, we map string and unicode objects into lists of unicode
objects.

There is not a single right way to do tokenizing. The best method
depends on the application.  This tokenizer is designed to be flexible
and this easy to adapt to new domains and tasks.  The basic logic is
this:

1. The tuple regex_strings defines a list of regular expression
   strings.

2. The regex_strings strings are put, in order, into a compiled
   regular expression object called word_re.

3. The tokenization is done by word_re.findall(s), where s is the
   user-supplied string, inside the tokenize() method of the class
   Tokenizer.

4. When instantiating Tokenizer objects, there is a single option:
   preserve_case.  By default, it is set to True. If it is set to
   False, then the tokenizer will downcase everything except for
   emoticons.

The __main__ method illustrates by tokenizing a few examples.

I've also included a Tokenizer method tokenize_random_tweet(). If the
twitter library is installed (http://code.google.com/p/python-twitter/)
and Twitter is cooperating, then it should tokenize a random
English-language tweet.

"""

######################################################################

import re
import htmlentitydefs

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most importantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# This particular element is used in a couple ways, so we define it
# with a name:
emoticon_string = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth      
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
    )"""

# The components of the tokenizer:
regex_strings = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?            
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?    
      \d{3}          # exchange
      [\-\s.]*   
      \d{4}          # base
    )"""
    ,
    # URLs:
    r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    ,
    # Emoticons:
    emoticon_string
    ,    
    # HTML tags:
     r"""<[^>]+>"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # Remaining word types:
    r"""
    (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots. 
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:
    
word_re = re.compile(r"""(%s)""" % "|".join(regex_strings), re.VERBOSE | re.I | re.UNICODE)

# The emoticon string gets its own regex so that we can preserve case for them as needed:
emoticon_re = re.compile(regex_strings[1], re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
html_entity_digit_re = re.compile(r"&#\d+;")
html_entity_alpha_re = re.compile(r"&\w+;")
amp = "&amp;"

######################################################################

class Tokenizer:
    def __init__(self, preserve_case=False):
        self.preserve_case = preserve_case

    def tokenize(self, s):
        """
        Argument: s -- any string or unicode object
        Value: a tokenize list of strings; conatenating this list returns the original string if preserve_case=False
        """        
        # Try to ensure unicode:
        try:
            s = unicode(s)
        except UnicodeDecodeError:
            s = str(s).encode('string_escape')
            s = unicode(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        # Tokenize:
        words = word_re.findall(s)
        # Possible alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:            
            words = map((lambda x : x if emoticon_re.search(x) else x.lower()), words)
        return words

    def tokenize_random_tweet(self):
        """
        If the twitter library is installed and a twitter connection
        can be established, then tokenize a random tweet.
        """
        try:
            import twitter
        except ImportError:
            print "Apologies. The random tweet functionality requires the Python twitter library: http://code.google.com/p/python-twitter/"
        from random import shuffle
        api = twitter.Api()
        tweets = api.GetPublicTimeline()
        if tweets:
            for tweet in tweets:
                if tweet.user.lang == 'en':            
                    return self.tokenize(tweet.text)
        else:
            raise Exception("Apologies. I couldn't get Twitter to give me a public English-language tweet. Perhaps try again")

    def __html2unicode(self, s):
        """
        Internal metod that seeks to replace all the HTML entities in
        s with their corresponding unicode characters.
        """
        # First the digits:
        ents = set(html_entity_digit_re.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, unichr(entnum))	
                except:
                    pass
        # Now the alpha versions:
        ents = set(html_entity_alpha_re.findall(s))
        ents = filter((lambda x : x != amp), ents)
        for ent in ents:
            entname = ent[1:-1]
            try:            
                s = s.replace(ent, unichr(htmlentitydefs.name2codepoint[entname]))
            except:
                pass                    
            s = s.replace(amp, " and ")
        return s


# In[13]:

from numpy import log

tok = Tokenizer(preserve_case=False)

def get_rel_popularity(c_k, c_all):
    return log(1.0 * c_k / c_all) / log(2)

def print_tokens(tokens, gid = None):
    group_name = "overall"
    if gid is not None:
        group_name = "group %d" % gid
    print '=' * 5 + ' ' + group_name + ' ' + '=' * 5
    for t, n in tokens:
        print "%s\t%.4f" % (t, n)
    print


# (1) Tokenize the tweets using the tokenizer we provided above named `tok`. Count the number of mentions for each tokens regardless of specific user group.
# 
# Call `print_count` function to show how many different tokens we have.

# In[19]:

tokenRDD0 = parseRDD.flatMap(lambda x: ((x[0],new_token) for new_token in tok.tokenize(x[1]))).distinct()
grouptok_one = tokenRDD0.map(lambda x:((partition_Dict_Broad.value.get(x[0], 7),x[1]),1))
group_tok_sum = grouptok_one.reduceByKey(lambda x,y:x+y).cache()
all_tokens = group_tok_sum.map(lambda (x,y):(x[1],y)).reduceByKey(lambda x,y:x+y)
print_count(all_tokens)


# (2) Tokens that are mentioned by too few users are usually not very interesting. So we want to only keep tokens that are mentioned by at least 100 users. Please filter out tokens that don't meet this requirement.
# 
# Call `print_count` function to show how many different tokens we have after the filtering.
# 
# Call `print_tokens` function to show top 20 most frequent tokens.

# In[16]:

tokenRDD2 = all_tokens.filter(lambda x: x[1] > 100).cache()
print_count(tokenRDD2)
print_tokens(tokenRDD2.top(20, lambda x:x[1]))


# (3) For all tokens that are mentioned by at least 100 users, compute their relative popularity in each user group. Then print the top 10 tokens with highest relative popularity in each user group. In case two tokens have same relative popularity, break the tie by printing the alphabetically smaller one.
# 
# **Hint:** Let the relative popularity of a token $t$ be $p$. The order of the items will be satisfied by sorting them using (-p, t) as the key.

# original time: 893 ms

# In[17]:

def group(gid):
    # no cache() after filter. It slows down for 20GB. 
    group_counts = group_tok_sum.filter(lambda (x,y): x[0] == gid).map(lambda (x,y): (x[1], y))
    merged = group_counts.join(tokenRDD2)
    tok_RelPop = merged.map(lambda (x,(c_k,c_all)): (x, get_rel_popularity(c_k , c_all)))
    return tok_RelPop


# In[18]:

for group_num in xrange(0,8):
    final = group(group_num)
    print_tokens(final.top(10, lambda x:x[1]), gid=group_num)


# In[ ]:



