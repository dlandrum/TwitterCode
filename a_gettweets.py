""" This is the docstring.
"""
################################################################################
##
import sys
from io import StringIO
import tweepy, json
from tweepy import OAuthHandler
from tweepy.parsers import JSONParser

from collections import defaultdict

sys.path.append('/Users/buell/Current/Python/')

from DABUtilities.dabfunctions.checkargs import checkargs
from DABUtilities.dabfunctions.printoutput import printoutput
from DABUtilities.dabfunctions.dabtimer import DABTimer

################################################################################
## Simple function to dump a tweet's keys and values, with a preceding label.
def dumptweet(label, thetweet, logfile):
    outstring = '\nDUMPONETWEET %s' % (label)
    printoutput(outstring, logfile)
    for key, value in thetweet.items():
        outstring = 'DUMPTWEET    %s %8s %s' % (label, key, value)
        printoutput(outstring, logfile)

################################################################################
## Get tweets.
##
## This function gets the tweets and writes them to an output file, after having
## attempted to strip them down to just the tweet and not the tweepy wrapper.
##
def gettweets(howmanytweets, dataoutputfile, logfile):
    consumer_key = 'bqb2614rUsb9Ts7DhtyTaPuWI'
    consumer_secret = 'Q9UMklSd41gMzUKIMzt5CzZQZNOS6E5ndHNGk58pUxBShvGO2r'
    access_token = '925969087997542400-mqhVvH56uE8pA5sqprq6vCh7XVB00Wj'
    access_secret = 'a9xyKdciNSoZdg6AapOA6774AVEYgnpId5eelD9DvXwRS'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    api = tweepy.API(auth)

    seqkeyfile = open('tweetsequencekey.txt')
    tweetsequencekey = int(seqkeyfile.read())
    seqkeyfile.close()

    seqkeyfile = open('tweetsequencekey.txt', 'w')
    tweetsequencenew = tweetsequencekey + howmanytweets + 1
    seqkeyfile.write('%d\n' % (tweetsequencenew))
    seqkeyfile.close()

    count = 0
    for result in limit_handled(tweepy.Cursor(api.search, q=" mom ").items(), logfile):

        jsonversion = json.dumps(result._json)
#        print('\nJSONVERSION')
#        print(jsonversion)

#        print('\nJSONDUMPS')
#        print(json.dumps(jsonversion, sort_keys=True, indent=4))

        jsonstring = StringIO(jsonversion)
        thisdict = json.load(jsonstring)

#        print('\nTHISDICT')
#        outstring = '%8d %s' % (count, thisdict)
#        outputDAB.write(outstring + '\n')

        outstring = 'KEY VALUE PAIRS COUNTER %5d' % (count)
        printoutput(outstring, logfile)
        for key, value in sorted(thisdict.items()):
            outstring = '%8d XXZZXX %s XXZZXX %s ZZXXZZ' % (tweetsequencekey, key, value)
            dataoutputfile.write(outstring + '\n')
#        outstring = 'ZZXXZZ'
#        outputDAB.write(outstring + '\n')
#        sys.exit()

#        outputJSON.write(str(result))
#        outputJSON.write("\n")
#        outputText.write(result.text)
#        outputText.write("\n")
#        outputUserID.write(str(result.user.id))
#        outputUserID.write("\n")
        count += 1
        tweetsequencekey += 1

        # This is a kluge in here to limit execution so we can test code.
        # In a real application this might go quite a while before exiting.
        if count >= howmanytweets: break

#    outputDAB.close()

#    outputJSON.close()
#    outputText.close()
#    outputUserID.close()

################################################################################
##
def limit_handled(cursor, logfile):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            outstring = "limit reached"
            printoutput(outstring, logfile)
        #    time.sleep(15 * 60)
        except tweepy.TweepError:
            outstring = "limit reached"
            printoutput(outstring, logfile)

################################################################################
## Read the tweets back from the file.
##
## This is a major kluge because DAB can't find the magic function to convert
## from tweepy into a Python dictionary.
## 
## The big hassle is that there can be embedded newline characters. So in 
## the 'gettweets' function we have separated the JSON/dict keys from their
## values with a string 'XXZZXX', and we have appended a string 'ZZXXZZ' to
## the end of a single value for a JSON/Twitter key
## This allows us to read lines from the file until we get something ending
## in 'ZZXXZZ', which is how we know we have come to the end of the value for
## that key.
## 
## We read the file and create seqnum-key-value triples, which we append to  
## a list. We then process the list to create 'thebigdictionary' of all the 
## tweets, each of which is a dictionary of key-value mappings. 
## 
def readtweets():
    inputfile = open('outputDAB.txt', 'r')

    ############################################################################
    ## Build the list of input lines, making sure to deal with embedded newline 
    ## characters. the result of this is a list of [seqnum, key, value] triples.
    oneline = ''
    thelines = []
    for line in inputfile:
        if not line.strip().endswith('ZZXXZZ'):
            print('FOUND MULTILINE %s' % (line))
            oneline = oneline + line + '\n'
        else:
#            print('FOUND ONELINE   %s' % (line))
            oneline = oneline + line
            thelines.append(oneline)
            oneline = ''

    ############################################################################
    ## Dump the list.
#    for item in thelines:
#        print('\nLINE')
#        print(item)

    ############################################################################
    ## Walk through the list and create the dictionary of dictionaries.
    ## Note that we have to create a dummy for the first time we see the sequence
    ## number for a tweet, and then after that we add to that dictionary.
    thebigdictionary = dict()
    for item in thelines:
#        print('\nPROCESS')
#        print(item)
        item = item.replace('ZZXXZZ', '')
        itemsplit = item.split('XXZZXX')
        sequencekey = int(itemsplit[0])
        tweetkey = itemsplit[1]
        tweetvalue = itemsplit[2]
#        print(' SEQ: %d' % (sequencekey))
#        print(' KEY: %s' % (tweetkey))
#        print(' VAL: %s' % (tweetvalue))

        # If there is no tweet for this sequence key, create one.
        if sequencekey not in thebigdictionary.keys():
            thistweet = defaultdict(str)
            thebigdictionary[sequencekey] = thistweet

        # Pull up the tweet for this sequence key.
        # Add the new line's value.
        # Put the tweet back
        thistweet = thebigdictionary[sequencekey]
        thistweet[tweetkey] = tweetvalue
        thebigdictionary[sequencekey] = thistweet

#    ############################################################################
#    ## Dump the dictionary of tweets to verify that we have them stored.
#    print('DUMP THE TWEETS')
#    for seqkey, thistweet in sorted(thebigdictionary.items()):
##        print('TWEETA %8s' % (seqkey))
#        label = 'TWEETB %8d:' % (seqkey)
#        dumptweet(label, thistweet)

################################################################################
##
def main(howmanytweets, dataoutputfilename, logfilename):

    dataoutputfile = open(dataoutputfilename, 'w')
    logfile = open(logfilename, 'w')

    outstring = 'MAIN: GET %d TWEETS' % (howmanytweets)
    printoutput(outstring, logfile)

    outstring = "MAIN: WRITE TWEETS TO FILE '%s'" % (dataoutputfilename)
    printoutput(outstring, logfile)

    outstring = "MAIN: WRITE LOG TO FILE    '%s'" % (logfilename)
    printoutput(outstring, logfile)

    gettweets(howmanytweets, dataoutputfile, logfile)
    dataoutputfile.close()
#    readtweets()

################################################################################
##
checkargs(3, 'usage: a.out howmanytweets dataoutputfile logfile')
howmanytweets = int(sys.argv[1])
dataoutputfilename = sys.argv[2]
logfilename = sys.argv[3]
main(howmanytweets, dataoutputfilename, logfilename)

