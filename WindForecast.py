# import urllib2
# import json
#f = urllib2.urlopen('http://api.wunderground.com/api/Your_Key/geolookup/conditions/q/IA/Cedar_Rapids.json')
#json_string = f.read()
#parsed_json = json.loads(json_string)
#location = parsed_json['location']['city']
#temp_f = parsed_json['current_observation']['temp_f']
#print("Current temperature in %s is: %s" % (location, temp_f))
#f.close()


#import urllib2, urllib, json

#yahoo_key = "dj0yJmk9elpFNkJlbTFXYm1uJmQ9WVdrOWJtUlhWMFpNTm5FbWNHbzlNQS0tJnM9Y29uc3VtZXJzZWNyZXQmeD02Mw--"

import urllib
import json

def download_file(url=None, params=None):
    import urllib.request
    import urllib.parse

    querystring = urllib.parse.urlencode(params)

    response = urllib.request.urlopen(url + "?" + querystring)
    data = response.read()  # a `bytes` object
    encoding = response.info().get_param("charset", "utf-8")
    text = data.decode(encoding)  # a `str`; this step can't be used if data is binary
    return text

def doyqlquery(query):
    import json
    baseurl = "https://query.yahooapis.com/v1/public/yql"

    result = download_file(baseurl, { "q": query, "format": "json"})
    return json.loads(result)


#data = doyqlquery("select * from weather.forecast where woeid in (select woeid from geo.places(1) where text=""nome, ak"")")



#data = doyqlquery("select wind from weather.forecast where woeid in (select woeid from geo.places(1) where text=\"chicago, il\")")

beijing_woeid = "2151330"

data = doyqlquery("select * from weather.forecast where woeid=" + beijing_woeid)

# Hourly forecast, including wind, for Beijing
#http://www.weather.com/weather/hourbyhour/l/CHXX0008:1:CH

print(data['query']['results'])
print(data)
