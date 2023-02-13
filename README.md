# Real-Time Climate Change Tweets Analysis
**Course:**
Data Engineering (w205) - UC Berkeley, Fall 2021

**Contributors:**
- Heesuk Jang
- Mile

**Motivation:**

Twitter is an online news and social networking site where people communicate in short messges called tweets. Twitter boasts approximately 400 million users, of which 75% are based outside of the US[1]. As such, users post tweets on various topics, ranging from politics, lifestyle, memes and so on. Twitter is also often used by users to voice opinions and share information regarding ongoing events like sports games, political events and even protests. This digital infrastructure of twitter creates enormous amounts of data that contains invaluable information and insights. However, the task to collect, filter and analyze data from twitter is a difficult one and requires setting up a data pipeline using several state-of-the-art technologies that are capable of handling data at a large scale.

Specifically, this project focuses on collecting tweet data related to climate change. Climate change is a very important topic in today's politics, both in the US and around the world. It is potentially the biggest existential challenge that humans have ever faced. In addition, it is also a controvertial topic, with ideas for addressing it ranging from fundamental tranformation of world economies to completely ignoring it. Hence, understanding what people are saying about climate change could be of interest to political parties, environmental groups and corporations. This project seeks to build a pipeline that would receive streaming data from twitter API related to climate change in real-time, transform it appropriately and store it for further analysis. The analysis of the twitter data in the project seeks to asnwer the following questions related to climate change:

    What are the most frequently used words in the entire set of tweets?
    What are the frequently used words in the top 100 most retweeted tweets?
    What are the top 5 most active usernames?
    What are the top 5 most trending hastags?
    What are the top 10 most retweeted tweets?
    What fraction of retweets, likes and replies are from tweets with a URL in them, compared to tweets without a URL?
    What fraction of retweets, likes and replies are from tweets with a hashtag, compared to tweets without a hashtag?
    What percentage of tweets contain the hastags happeningnow or happening?

**Description of the Data:**

The data is received using an API provided by twitter[2]. Before gaining access to the API, one needs to apply for a developer account with twitter, which includes providing information on the intended use of the data from the Twitter API. Once a developer account is approved, an authentication token is provided to the user. The connection to the twitter stream is established using a specific URL. The stream is specified using two parameters, namely, rules and fields. Rules are used to filter tweets, such that the data received through the stream is particularly applicable for a specific application. Some example rules could be tweets containing a specific phrase, tweets from specific accounts, tweets containing specific hashtags, tweets containing images or links and tweets that are not retweets. Any of these rules can be combined using boolean operators. Multiple separate rules can also be provided. Each separate rule is assigned an ID and each tweet in the data stream contains information about the rules it satifies, allowing for possibilities of interesting analyses.

The other parameter that specifies a data stream is fields. Fields are the specific attributes of a tweet object that are requested in the stream. By default, each tweet in the data stream contains a tweet ID and the text of the tweet. In addition, numerous other fields can be requested, such as, ID and username of the author of the tweet, ID's and usernames of other users tagged in the tweet, location of the tweet, type of device used to send the tweet, time of creating of the tweet, metrics like likes, retweets, comments on the tweet and any attachments like images or videos attached to the tweet. Each tweet is received in a nested JSON format with different levels for tweet data, user data and matching rules.

