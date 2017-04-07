# pyrlite
基于rlite-py的封装


```
rlite = RliteDB("kkkkk.test")
print rlite.hmset('key', {"d": 3, "fff": 5})

print rlite.hmset("mhash", {"a": 1, "b": 2})
print rlite.hmget("mhash", "a", "b")

print rlite.get('key')

print rlite.rpush('mylist', '1', '2', '3')
print rlite.lrange('mylist', '0', '-1')
print rlite.exists("mhash")
```