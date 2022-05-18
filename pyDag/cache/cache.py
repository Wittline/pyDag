from base64 import decode
import redis

class Cache:

    def set(k,v, e):
        try:
            r = redis.StrictRedis(
                host='localhost', 
                port=6379, 
                decode_responses=True)

            r.set(k,v)
            if e > 0:
                r.expire(k,e)           
        except Exception as ex:
            print(str(ex))

    def get(k):
        try:        
            r = redis.StrictRedis(
                host='localhost', 
                port=6379, 
                decode_responses=True)

            if r.exists(k) < 0:
                return None
            else:
                return r.get(k)
        except Exception as ex:
            print(str(ex))
            return None

    def delete(k):
        try:
            r = redis.StrictRedis(
                host='localhost', 
                port=6379, 
                decode_responses=True)

            if not r.exists(k) < 0:
                r.delete(k)
        except Exception as ex:
            print(str(ex))        

        





    