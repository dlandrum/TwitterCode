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
## Extract between two field identifiers.
def extract(startkey, uptokey, tag, thedict, thelist, outfile):
    testkey = "'" + startkey + "':"
    if thelist[0] == testkey:
        theplace = ' '.join(thelist[1:])
        thelist = theplace.split()
#        print("A" , thelist)
        fullvalue = ''
#        while uptokey not in thelist[0]:
        uptokeymunged = "'" + uptokey + "':"
        while uptokeymunged not in thelist[0]:
            fullvalue = fullvalue + ' ' + thelist[0]
            theplace = ' '.join(thelist[1:])
            thelist = theplace.split()
#            print("B" , thelist)

        fullvalue = fullvalue.replace("'", "")
        fullvalue = fullvalue.replace(",", "")
        thedict[tag+startkey] = fullvalue.strip()
#    print("C" , thelist)
    thelist = theplace.split()
#    print("D" , thelist)

    return thedict, thelist, theplace

################################################################################
## Function to get the non-None values by field key.
def hackkeys(label, thedict, logfile):

    # Run through the dictionary to get the names of the field keys in a set.
    setofkeys = set()
    for tweetkey, thetweet in thedict.items():
        for fieldkey, fieldvalue in thetweet.items():
            setofkeys.add(fieldkey)

    # Print the list of fieldkey names.
    outstring = '%s %s' % (label, sorted(setofkeys))
    printoutput(outstring, logfile)
    outstring = ''
    printoutput(outstring, logfile)

    # We're now going to get the values by field key
    # initialize a list for each field key
    valuelist = defaultdict()
    for fieldkey in setofkeys:
        valuelist[fieldkey] = []

    # Now get the non-None values.
    favoritecountdict = defaultdict(int)
    langdict = defaultdict(int)
    metadatadict = defaultdict(int)
    placedict = defaultdict(int)
    qstatusiddict = defaultdict(int)
    qstatusidstrdict = defaultdict(int)
    retweetcountdict = defaultdict(int)
    sourcedict = defaultdict(int)
    userdict = defaultdict(int)
    for tweetkey, thetweet in thedict.items():
        for fieldkey, fieldvalue in thetweet.items():

            if fieldkey == 'coordinates':
                if fieldvalue.strip() != 'None':
                    fieldvalue = parsecoordinates(fieldvalue, logfile)

            if fieldkey == 'favorite_count':
                if fieldvalue.strip() != 'None':
                    favoritecountdict[fieldvalue.strip()] += 1

            if fieldkey == 'geo':
                if fieldvalue.strip() != 'None':
                    fieldvalue = parsecoordinates(fieldvalue, logfile)

            if fieldkey == 'lang':
                if fieldvalue.strip() != 'None':
                    langdict[fieldvalue.strip()] += 1

            if fieldkey == 'metadata':
                if fieldvalue.strip() != 'None':
                    metadatadict = parsemetadata(metadatadict, fieldvalue, \
                                                 logfile)

            if fieldkey == 'place':
                if fieldvalue.strip() != 'None':
                    placedict = parseplace(placedict, fieldvalue, \
                                           logfile)

            if fieldkey == 'quoted_status_id':
                if fieldvalue.strip() != 'None':
                    qstatusiddict = parseqstatusid(qstatusiddict, \
                                                   fieldvalue, logfile)

            if fieldkey == 'quoted_status_id_str':
                if fieldvalue.strip() != 'None':
                    qstatusidstrdict = parseqstatusidstr(qstatusidstrdict, \
                                                         fieldvalue, logfile)

            if fieldkey == 'retweet_count':
                if fieldvalue.strip() != 'None':
                    retweetcountdict[fieldvalue.strip()] += 1

            if fieldkey == 'source':
                if fieldvalue.strip() != 'None':
                    sourcedict[fieldvalue.strip()] += 1

            if fieldkey == 'user':
                if fieldvalue.strip() != 'None':
                    userdict = parseuser(userdict, fieldvalue, logfile)

            if fieldvalue.strip() == 'None':
                continue
            if (fieldvalue.strip() == 'True') or \
               (fieldvalue.strip() == 'False'):
                if len(valuelist[fieldkey]) == 0:
                    valuelist[fieldkey] = [0,0]
                
                if (fieldvalue.strip() == 'True'):
                    counts = valuelist[fieldkey]
                    counts = [int(counts[0]+1), int(counts[1])]
                    valuelist[fieldkey] = counts
                else:
                    counts = valuelist[fieldkey]
                    counts = [int(counts[0]), int(counts[1])+1]
                    valuelist[fieldkey] = counts
                continue
            if fieldkey == 'favorite_count':
                valuelist[fieldkey] = favoritecountdict
            elif fieldkey == 'lang':
                valuelist[fieldkey] = langdict
            elif fieldkey == 'metadata':
                valuelist[fieldkey] = metadatadict
            elif fieldkey == 'place':
                valuelist[fieldkey] = placedict
            elif fieldkey == 'quoted_status_id':
                valuelist[fieldkey] = qstatusiddict
            elif fieldkey == 'quoted_status_id_str':
                valuelist[fieldkey] = qstatusidstrdict
            elif fieldkey == 'retweet_count':
                valuelist[fieldkey] = retweetcountdict
            elif fieldkey == 'source':
                valuelist[fieldkey] = sourcedict
            elif fieldkey == 'user':
                valuelist[fieldkey] = userdict
            else:
                valuelist[fieldkey].append(fieldvalue)

    writethefiles(label, valuelist, setofkeys)

################################################################################
## Simple function to dump a tweet's keys and values, with a preceding label.
def dumptweet(label, thetweet, logfile):
    outstring = '\nDUMPONETWEET %s' % (label)
    printoutput(outstring, logfile)
    for key, value in thetweet.items():
        outstring = 'DUMPTWEET    %s %8s %s' % (label, key, value)
        printoutput(outstring, logfile)

################################################################################
## Parse the 'coordinates' field.
def parsecoordinates(fieldcoordinates, outputfile):
    startindex = fieldcoordinates.find('[')
    endindex = fieldcoordinates.find(']')
    coords = fieldcoordinates[startindex+1:endindex]
#    outstring = 'COORDS %s' % (coords)
#    printoutput(outstring, outputfile)
    return coords

################################################################################
## Parse the 'metadata' field.
def parsemetadata(metadatadict, fieldmetadata, outputfile):
    field = fieldmetadata.strip()
    field = field.replace('{', '')
    field = field.replace('}', '')
    field = field.replace("'", '')
    field = field.replace(':', '')
    field = field.replace(',', '')
    fieldsplit = field.split()
    if len(fieldsplit) != 4:
        outstring = 'ERROR METADATA %s' % (fieldsplit)
        printoutput(outstring, outputfile)
        sys.exit()

    thekey = fieldsplit[1] + ' ' + fieldsplit[3]
    metadatadict[thekey] += 1

    return metadatadict

################################################################################
## Parse the 'place' field.
def parseplace(placedict, fieldplace, outputfile):
    localdict = defaultdict()
    fieldplace = fieldplace.strip()
    fieldplace = fieldplace[1:] # strip off the leading brace
    fieldlist = fieldplace.split()

    localdict, fieldlist, fieldplace = extract('id', 'url', \
                                               'a_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, fieldplace = extract('url', 'place_type', \
                                               'b_', \
                                               localdict, fieldlist, outputfile) 
    
    localdict, fieldlist, fieldplace = extract('place_type', 'name', \
                                               'c_', \
                                               localdict, fieldlist, outputfile) 
    
    localdict, fieldlist, fieldplace = extract('name', 'full_name', \
                                               'd_', \
                                               localdict, fieldlist, outputfile) 
    
    localdict, fieldlist, fieldplace = extract('full_name', 'country_code', \
                                               'e_', \
                                               localdict, fieldlist, outputfile) 
    
    localdict, fieldlist, fieldplace = extract('country_code', 'country', \
                                               'f_', \
                                               localdict, fieldlist, outputfile) 
    
    localdict, fieldlist, fieldplace = extract('country', 'contained_within', \
                                               'g_', \
                                               localdict, fieldlist, outputfile) 
    
    localdict, fieldlist, fieldplace = extract('contained_within', \
                                               'bounding_box', \
                                               'h_', \
                                               localdict, fieldlist, outputfile) 
    
    localdict, fieldlist, fieldplace = extract('bounding_box', 'attributes', \
                                               'i_', \
                                               localdict, fieldlist, outputfile) 

    lastfield = fieldlist[1] # dump the 'attributes' key and keep value
    lastfield = lastfield[:-1] # strip off trailing brace
    
    localdict['j_attributes'] = lastfield
#    localdict['xxxx'] = fieldplace

    idvalue = localdict['a_id']
    placedict[idvalue] = localdict

    return placedict

################################################################################
## Parse the 'quoted_status_id' field.
def parseqstatusid(qstatusiddict, fieldqstatusid, outputfile):
    field = fieldqstatusid.strip()
    field = field.replace("'", '')
    qstatusiddict[field] += 1

    return qstatusiddict

################################################################################
## Parse the 'quoted_status_id' field.
def parseqstatusidstr(qstatusidstrdict, fieldqstatusidstr, outputfile):
    field = fieldqstatusidstr.strip()
    field = field.replace("'", '')
    qstatusidstrdict[field] += 1

    return qstatusidstrdict

################################################################################
## Parse the 'user' field.
def parseuser(thedict, thefield, outputfile):
    localdict = defaultdict()
    thefield = thefield.strip()
    thefield = thefield[1:] # strip off the leading brace
    fieldlist = thefield.split()

    localdict, fieldlist, thefield = extract('id', 'id_str', \
                                               'aa_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('id_str', 'name', \
                                               'ab_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('name', 'screen_name', \
                                               'ac_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('screen_name', 'location', \
                                               'ad_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('location', 'description', \
                                               'ae_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('description', 'url', \
                                               'af_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('url', 'entities', \
                                               'ag_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('entities', 'protected', \
                                               'ah_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('protected', 'followers_count', \
                                               'ai_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('followers_count', 'friends_count',\
                                               'aj_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('friends_count', 'listed_count',\
                                               'ak_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('listed_count', 'created_at',\
                                               'al_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('created_at', 'favourites_count',\
                                               'am_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('favourites_count', 'utc_offset', \
                                               'an_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('utc_offset', 'time_zone', \
                                               'ao_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('time_zone', 'geo_enabled', \
                                               'ap_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('geo_enabled', 'verified', \
                                               'aq_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('verified', 'statuses_count', \
                                               'ar_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('statuses_count', 'lang', \
                                               'as_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('lang', 'contributors_enabled', \
                                               'at_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('contributors_enabled', \
                                              'is_translator', \
                                              'au_', \
                                              localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('is_translator', 'is_translation_enabled', \
                                               'av_', \
                                               localdict, fieldlist, outputfile) 
   
    localdict, fieldlist, thefield = extract('is_translation_enabled', 'profile_background_color', \
                                               'aw_', \
                                               localdict, fieldlist, outputfile) 

#    localdict, fieldlist, thefield = extract('profile_background_color', 'profile_background_image_url', \
#                                               'ax_', \
#                                               localdict, fieldlist, outputfile) 
#   
#    localdict, fieldlist, thefield = extract('profile_background_image_url', 'profile_background_url_https', \
#                                               'ay_', \
#                                               localdict, fieldlist, outputfile) 
   
    localdict['xxxx'] = thefield

    idvalue = localdict['aa_id']
    thedict[idvalue] = localdict

    return thedict

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
#        outstring = 'ID_STR %s' % (thisidstr)
#        printoutput(outstring, outputfile)
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
## Simple function to write the 'geo' and 'coordinates' lists.
def writecoords(label, which, thecoords, outfile):

    newlist = []
    for item in thecoords:
        itemsplit = item.split(',')
        if which == 'geo':
            lat = float(itemsplit[0])
            lon = float(itemsplit[1])
        else:
            lat = float(itemsplit[1])
            lon = float(itemsplit[0])
        newlist.append([lon, lat])

    for item in sorted(newlist):
        outstring = '%12.6f %12.6f\n' % (item[1], item[0])
        outfile.write(outstring)

################################################################################
## Simple function to write a dict sorted by key.
def writedictofdicts(label, thedict, outfile):
    for key, value in sorted(thedict.items()):
        localdict = value
        for localkey, localvalue in sorted(localdict.items()):
            outstring = '%-10s %s %s\n' % (key, localkey, localvalue)
            outfile.write(outstring)

################################################################################
## Simple function to write a freq dict sorted by key.
## This is used for things like 'favorite' where the 'favorited by' key
## value is more important than the number of times favorited.
def writefreqdict(label, thedict, outfile):
    thelist = []
    for key, freq in thedict.items():
        keystr = '%8s' % (key)
        thelist.append([keystr, freq])

    for item in sorted(thelist):
        key = item[0]
        freq = item[1]
        outstring = '%8d %s\n' % (freq, key)
        outfile.write(outstring)

################################################################################
## Simple function to write a freq dict sorted by freq.
def writefreqdictflipped(label, thedict, outfile):
    thelist = []
    for key, freq in thedict.items():
        thelist.append([freq, key])

    for item in sorted(thelist):
        freq = item[0]
        key = item[1]
        outstring = '%8d %s\n' % (freq, key)
        outfile.write(outstring)

################################################################################
## Simple function to write the output files by fieldkey.
def writethefiles(label, valuelist, setofkeys):
    for fieldkey in sorted(setofkeys):
        thefile = open('keysout/'+fieldkey, 'w')
        labelstring = '\n%s %s\n' % (label, fieldkey.upper())
        thefile.write(labelstring)

        if fieldkey == 'coordinates':
            writecoords(label, fieldkey, valuelist[fieldkey], thefile)
        elif fieldkey == 'favorite_count':
            writefreqdict(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'geo':
            writecoords(label, fieldkey, valuelist[fieldkey], thefile)
        elif fieldkey == 'lang':
            writefreqdictflipped(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'metadata':
            writefreqdictflipped(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'place':
            writedictofdicts(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'quoted_status_id':
            writefreqdictflipped(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'quoted_status_id_str':
            writefreqdictflipped(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'retweet_count':
            writefreqdictflipped(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'source':
            writefreqdictflipped(label, valuelist[fieldkey], thefile)
        elif fieldkey == 'user':
            writedictofdicts(label, valuelist[fieldkey], thefile)
        else:
            outstring = '%s %s\n' % (label, valuelist[fieldkey])
            thefile.write(outstring)
        thefile.close()

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

    hackkeys('KEYS', thedict, dataoutputfile)

    dataoutputfile.close()

################################################################################
##
checkargs(3, 'usage: a.out datainputfile dataoutputfile logfile')
datainputfilename = sys.argv[1]
dataoutputfilename = sys.argv[2]
logfilename = sys.argv[3]
main(datainputfilename, dataoutputfilename, logfilename)

