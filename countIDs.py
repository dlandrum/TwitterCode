import tweepy
from tweepy import OAuthHandler

consumer_key = 'bqb2614rUsb9Ts7DhtyTaPuWI'
consumer_secret = 'Q9UMklSd41gMzUKIMzt5CzZQZNOS6E5ndHNGk58pUxBShvGO2r'
access_token = '925969087997542400-mqhVvH56uE8pA5sqprq6vCh7XVB00Wj'
access_secret = 'a9xyKdciNSoZdg6AapOA6774AVEYgnpId5eelD9DvXwRS'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

outputUserID = open("outputUserID.txt", "r")
listIDs = []
IDcount = []
for id in outputUserID.read().splitlines():
	if id not in listIDs:
		listIDs.append(id)
		IDcount.append(0)
count = 0
for id in outputUserID.read().splitlines():
	position = count
	while (listIDs[position] != id):
		position = position - 1
	IDcount[position] = IDcount[position] + 1
	count = count + 1
max = -1
index = -1
counter = 0
for count in IDcount:
	if count > max:
		max = count
		index = counter
	counter = counter + 1
print(listIDs[index])

outputSelected = open(str(listIDs[index])+".txt", "a")

def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print("limit reached")
        #    time.sleep(15 * 60)
        except tweepy.TweepError:
        	print("limit reached")

count = 0
for result in limit_handled(tweepy.Cursor(api.user_timeline, user_id=listIDs[index]).items()):
#can do maxID and minID to specify when you want to search
	if (" mom " in result.text or " Mom " in result.text):
		count = count + 1
		print(result.text+"\n")
print(count)







