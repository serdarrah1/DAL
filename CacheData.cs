using System;
using System.Collections.Generic; 
using System.Runtime.Caching; 

namespace DataLayer {
    public static class CacheData {
        public static T GetCachedObj<T>(string key) where T : class {
            T currentData = MemoryCache.Default.Get(key) as T;
            return currentData;
        }
        public static void AddCacheData(object data, string key) {
            MemoryCache.Default.Add(key, data, new DateTimeOffset(DateTime.Now.AddHours(2)));
        }

        public static void AddCacheData(object data, string key, int plusminute) {
            MemoryCache.Default.Add(key, data, new DateTimeOffset(DateTime.Now.AddMinutes(plusminute)));
        }

        public static List<T> GetCachedList<T>(string key) where T : class {
            List<T> currentData = MemoryCache.Default.Get(key) as List<T>;
            return currentData;
        }
    }
}
