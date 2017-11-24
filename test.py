import tweepy, json
from tweepy import OAuthHandler

consumer_key = 'bqb2614rUsb9Ts7DhtyTaPuWI'
consumer_secret = 'Q9UMklSd41gMzUKIMzt5CzZQZNOS6E5ndHNGk58pUxBShvGO2r'
access_token = '925969087997542400-mqhVvH56uE8pA5sqprq6vCh7XVB00Wj'
access_secret = 'a9xyKdciNSoZdg6AapOA6774AVEYgnpId5eelD9DvXwRS'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)
outputJSON = open("outputJSON.txt", "a")
outputText = open("outputText.txt", "a")
outputUserID = open("outputUserID.txt", "a")

def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print("limit reached")
        #    time.sleep(15 * 60)
        except tweepy.TweepError:
        	print("limit reached")

for result in limit_handled(tweepy.Cursor(api.search, q=" mom ").items()):
	outputJSON.write(str(result))
	outputJSON.write("\n")
	outputText.write(result.text)
	outputText.write("\n")
	outputUserID.write(str(result.user.id))
	outputUserID.write("\n")

outputJSON.close()
outputText.close()
outputUserID.close()

#results = tweepy.Cursor(api.search, q="mom").items(10);
#results = api.search(q="mom").items(10)

#for result in results:
#	outputJSON.write(str(result))
#	outputJSON.write("\n")
#	outputText.write(result.text)
#	outputText.write("\n\n")
#	outputUserID.write(str(result.user.id))
#	outputUserID.write("\n\n")
#outputJSON.close()
#lines = open("outputJSON.txt", "r").read().splitlines()
#for line in lines:
#	print(line)
#	j = json.loads(line)
#	outputText.write(j.text)
#	outputText.write("\n\n")
#	outputUserID.write(str(j.user.id))
#	outputUserID.write("\n\n")
#outputText.close()
#outputUserID.close()

#	outputUserID.write(result.
#    count = count + 1
#    if (result.id not in idList):
#        idList.append(result.id)
#print(count)
#print(len(idList))
    # if (hasattr(result, 'place') and result.place is not None):
    #     print(result.place)

# print(api.rate_limit_status())

# count = 0
# query = '#winning'
# for searchResult in tweepy.Cursor(api.search, q=query).items(166):
#     count = count + 1
#     print(searchResult.text+"\n")
#
# print(count)
#
# for status in tweepy.Cursor(api.home_timeline).items(10):
#     # Process a single status
#     print(status.text)
