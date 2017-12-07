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

#sys.path.append('/Users/buell/Current/Python/')
#
#from DABUtilities.dabfunctions.checkargs import checkargs
#from DABUtilities.dabfunctions.printoutput import printoutput
#from DABUtilities.dabfunctions.dabtimer import DABTimer

from z_checkargs import checkargs
from z_printoutput import printoutput

################################################################################
## Simple function to count freqs of words in the texts.
def dowordfreqs(label, thedict, logfile):

    stopfile = open('wstoplistNLTK.txt')
    stopwords = set()
    for line in stopfile:
        stopwords.add(line.strip())
    stopfile.close()

#    print(stopwords)

    freqs = defaultdict(int)
    outstring = '\nWORDFREQS %s' % (label)
    printoutput(outstring, logfile)
    for idstr, thistweet in thedict.items():
        thistext = thistweet['text']
#        outstring = 'DUMPTEXT    %s %s: %s' % (label, idstr, thistext)
#        printoutput(outstring, logfile)

        thistextsplit = thistext.split()
        for item in thistextsplit:
            item = item.replace('“', '"')
            item = item.replace('”', '"')
            item = item.replace('’', "'")


            item = item.lower()
            item = item.strip()
            if item.startswith('"'):
                item = item[1:]
            if item.endswith('"'):
                item = item[:-1]
            if item.endswith('.'):
                item = item[:-1]

            if item.endswith(','):
                item = item.replace(',', '')
            if item.endswith(':'):
                item = item.replace(':', '')
            if item in stopwords:
                continue
            freqs[item] += 1 

    # put into a list for sorting by freq
    freqlist = []
    for word, freq in sorted(freqs.items()):
        freqlist.append([freq, word])

    # and print the list sorted by freq
    for item in sorted(freqlist):
        freq = item[0]
        word = item[1]
        outstring = 'FREQ %6d %s' % (freq, word)
        printoutput(outstring, logfile)

################################################################################
## Simple function to output some statistics
def dostats(label, thedict, dupcount, logfile):
    outstring = '%s tweets and dups %8d %8d' % \
                 (label, len(thedict), dupcount)
    printoutput(outstring, logfile)

################################################################################
## Simple function to dump a tweet's keys and values, with a preceding label.
def dumptweet(label, thetweet, logfile):
    outstring = '\nDUMPONETWEET %s' % (label)
    printoutput(outstring, logfile)
    for key, value in thetweet.items():
        outstring = 'DUMPTWEET    %s %8s %s' % (label, key, value)
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
def readtweets(inputfile, outputfile, logfile):

    ############################################################################
    ## Build the list of input lines, making sure to deal with embedded newline 
    ## characters. the result of this is a list of [seqnum, key, value] triples.
    oneline = ''
    thelines = []
    for line in inputfile:
        if not line.strip().endswith('ZZXXZZ'):
#            print('FOUND MULTILINE %s' % (line))
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
        sequencekey = int(itemsplit[0]) # we are not going to use this
        tweetfieldkey = itemsplit[1].strip()
        tweetfieldvalue = itemsplit[2].strip()
#        print(' SEQ: %d' % (sequencekey))
#        print(' KEY: %s' % (tweetfieldkey))
#        print(' VAL: %s' % (tweetfieldvalue))

        # If there is no tweet for this sequence key, create one.
        if sequencekey not in thebigdictionary.keys():
            thistweet = defaultdict(str)
            thebigdictionary[sequencekey] = thistweet

        # Pull up the tweet for this sequence key.
        # Add the new line's value.
        # Put the tweet back
        thistweet = thebigdictionary[sequencekey]
        thistweet[tweetfieldkey] = tweetfieldvalue
        thebigdictionary[sequencekey] = thistweet

#    ############################################################################
#    ## Dump the dictionary of tweets to verify that we have them stored.
#    print('DUMP THE BIGDICTIONARY OF TWEETS')
#    for seqkey, thistweet in sorted(thebigdictionary.items()):
##        print('TWEETA %8s' % (seqkey))
#        label = 'TWEETB %8d:' % (seqkey)
#        dumptweet(label, thistweet, logfile)

    ############################################################################
    ## Look for duplicate
    thefiltereddict = defaultdict()
    dupcountold = 0
    for seqkey, thistweet in sorted(thebigdictionary.items()):
        thisidstr = thistweet['id_str']
        outstring = 'ID_STR %s' % (thisidstr)
        printoutput(outstring, outputfile)
        # If 'id_str' is not in our dict, save the tweet, else continue
        if thisidstr not in thefiltereddict.keys():
            thefiltereddict[thisidstr] = thistweet
        else:
            outstring = 'ID_STR DUPLICATE %s' % (thisidstr)
            printoutput(outstring, outputfile)
            dupcountold += 1
            continue

#    ############################################################################
#    ## Turn the 'user' string into a dict of its own.
#    thefiltereddict2 = defaultdict()
#    print("CONVERT 'user' TO DICT")
#    for idstr, thistweet in sorted(thefiltereddict.items()):
#        userstring = thistweet['user']
#        print('CONVERTUSERA %s' % (userstring))
#        userstring = userstring.strip()
#        if userstring.startswith('{'):
#            userstring = userstring[1:]
#        else:
#            print('ERRORA CONVERTUSER %s' % (userstring))
#            sys.exit()
#        if userstring.endswith('}'):
#            userstring = userstring[:len(userstring)-1]
#        else:
#            print('ERRORB CONVERTUSER %s' % (userstring))
#            sys.exit()
#        print('CONVERTUSERB %s' % (userstring))
#        userstringsplit = userstring.split(',')
#        print()
#        for item in userstringsplit:
#            print('    CONVERTUSERC %s' % (item))
#
##        userstring = userstring.replace("'", '"')
##        print('CONVERTUSERB %s' % (userstring))
##        userstring2 = StringIO(userstring)
##        userdict = json.load(userstring2)
##        userstringjson = json.dumps(thistweet['user'])
##        print('CONVERTUSERSTRINGJSON   %s' % (userstringjson))
##        userstringjsonio = StringIO(userstringjson)
##        print('CONVERTUSERSTRINGJSONIO %s' % (userstringjsonio))
##        userdict = json.load(userstringjsonio)
##        thistweet['user'] = userdict
##        print('USERDICT %s %s' % (type(userdict), userdict))
##        thefiltereddict2[idstr] = thistweet

#    ############################################################################
#    ## Dump the dictionary of tweets to verify that we have them stored.
#    print('DUMP THE DE-DUPED TWEETS')
#    for idstr, thistweet in sorted(thefiltereddict.items()):
##        print('TWEETC %8s' % (idstr))
#        label = 'TWEETD %s:' % (idstr)
#        dumptweet(label, thistweet, logfile)

    lenoriginal = len(thebigdictionary)
    lenfiltered = len(thefiltereddict)

    if lenfiltered == lenoriginal:
        outstring = 'NO DUPS  %d %d' % (lenoriginal, lenfiltered)
        printoutput(outstring, logfile)
    else:
        dupcountnew = lenoriginal - lenfiltered
        outstring = 'YES DUPS %d %d (%d) (%d)' % (lenoriginal, lenfiltered, dupcountold, dupcountnew)
        printoutput(outstring, logfile)

    thebigdictionary.clear() # save memory space, no need for this any more

    return thefiltereddict, dupcountold

################################################################################
##
def main(datainputfilename, dataoutputfilename, logfilename):

    datainputfile = open(datainputfilename, 'r')
    dataoutputfile = open(dataoutputfilename, 'w')
    logfile = open(logfilename, 'w')

    outstring = "MAIN: READ TWEETS FROM FILE '%s'" % (datainputfilename)
    printoutput(outstring, logfile)

    outstring = "MAIN: WRITE DICT TO FILE    '%s'" % (dataoutputfilename)
    printoutput(outstring, logfile)

    outstring = "MAIN: WRITE LOG TO FILE    '%s'" % (logfilename)
    printoutput(outstring, logfile)

    thedict, dupcount = readtweets(datainputfile, dataoutputfile, logfile)
    datainputfile.close()

    dowordfreqs('FREQS', thedict, dataoutputfile)

    dostats('STATS', thedict, dupcount, dataoutputfile)

    dataoutputfile.close()

################################################################################
##
checkargs(3, 'usage: a.out datainputfile dataoutputfile logfile')
datainputfilename = sys.argv[1]
dataoutputfilename = sys.argv[2]
logfilename = sys.argv[3]
main(datainputfilename, dataoutputfilename, logfilename)

