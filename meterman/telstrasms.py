import datetime
import json
import urllib.request

from meterman import app_base as base


def getAuthToken(url, now, key, secret, authfile):
    response = urllib.request.urlopen(url)
    newtokendict = json.loads(response.read())
    newtokendict["datetime"] = str(now)
    authtoken = newtokendict["access_token"]
    newjsontoken = json.JSONEncoder().encode(newtokendict)
    f = open(authfile, 'w')
    print("new token : " + newjsontoken)
    f.write(newjsontoken)
    f.close()
    return authtoken

def sendMessage(phNum, messageText):
    key = "JYQtaS4qqoDqf8l10V4gQDjQ6pGHP1TX"
    secret = "5E9G8wGhQURhBk02"
    url = "https://api.telstra.com/v1/oauth/token?client_id=" + key + "&client_secret=" + secret + "&grant_type=client_credentials&scope=SMS"
    now = datetime.datetime.today()
    authfile = base.TEMP_PATH + 'telstraauth'

    f = open(authfile, 'a+')
    filetoken = f.read()
    f.close()
    try:
        with open(authfile) as json_dict:
            dict_data = json.load(json_dict)
            json_dict.close()
        authtoken = dict_data["access_token"]
        dtobj = datetime.datetime.strptime(dict_data["datetime"], '%Y-%m-%d %H:%M:%S.%f')
        expires = dtobj + datetime.timedelta(0, int(dict_data["expires_in"]))
        if expires < now:
            print("expired")
            authtoken = getAuthToken(url, now, key, secret, authfile)
        else:
            print("not expired")
    except ValueError:
        print("Invalid JSON. Retrieving new token")
        authtoken = getAuthToken(url, now, key, secret, authfile)

    try:
        url = "https://api.telstra.com/v1/sms/messages"
        smsdata = {'to': phNum, 'body': messageText}
        headers = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + str(authtoken)}

        req = urllib.request.Request(url, headers=headers, data=json.dumps(smsdata).encode('utf-8'))
        msg = urllib.request.urlopen(req)

        return msg.read()

    except Exception as e:
        raise Exception('SMS send error: {0}.  Message received: {1}.'.format(str(e), msg.read()))
