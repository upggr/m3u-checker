import requests
path = "https://c98db5952cb54b358365984178fb898a.msvdn.net/live/S86713049/gonOwuUacAxM/playlist.m3u8"
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
