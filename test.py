import requests
path = "http://master.cystreams.com:25461/live/epistrofi/epistrofi123/44.m3u8"
path2 = "https://antennalivesp-lh.akamaihd.net/i/live_1@715138/master.m3u8"


req = requests.request('GET', path, timeout=5)
print(path,req.status_code)

print('-------')
r = requests.head(path, timeout=5)
print(path,r.status_code)

print('-------')
print('-------')
print('-------')
print('-------')

req = requests.request('GET', path2, timeout=5)
print(path2,req.status_code)

print('-------')
r = requests.head(path2, timeout=5)
print(path2,r.status_code)
