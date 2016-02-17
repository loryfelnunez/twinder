# Twinder 
Finding the your Twitter Matches

## Data:
1. Twitter Static Data Set 
2. Twitter Streaming Data

## Technical Description and Definitions
*Twinder* is a Inverted Index approach to finding matching users.
*User* is a Twitter user
*User_Represention*  is a combintion of a user's tweets and her description 
*Match* is the maximum intersection of UserA's and UserB's User_Representation
*User_Query* - Query to the user_vocab table where table has a row of user and a vocab list is the User_Representation
*Word_Query* - Query to the vocab_user table where table has a row of word and a list of users (Inverted Index)

## Data Analysis and Cleaning
Tokenization
Metadata Weights
Stopword removal

### Experiments
POS Tagging
Chunking to get Noun Phrases

## Possible Optimizations
1. Avro and getting Schema first
