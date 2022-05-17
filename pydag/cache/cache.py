from base64 import decode
import redis

class Cache:

    def set(k,v):
        try:
            r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)            
            r.set(k,v)
            r.expire(k,3600)
        except Exception as ex:
            print(str(ex))

    def get(k):
        try:        
            r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
            if r.exists(k) < 0:
                return None
            else:
                return r.get(k)
        except Exception as ex:
            print(str(ex))
            return None

    def delete(k):
        r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
        r.delete(k)

        





    