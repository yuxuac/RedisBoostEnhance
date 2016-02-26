using RedisBoost;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RedisBoost.Core.Serialization;
using System.Globalization;
using System.Runtime.Serialization;
using System.Collections.Generic;

namespace RedisDemo
{
    /// <summary>
    /// 本文件内容及实现基于:http://andrew-bn.github.io/RedisBoost/
    /// </summary>
    public static class RedisUtil
    {
        static RedisUtil()
        {
            RedisClient.DefaultSerializer = new JsonSerializer();
        }

        #region Async Actions

        /// <summary>
        /// 得到一个Redis客户端实例(异步)
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        /// <returns>Redis客户端实例</returns>
        public static async Task<IRedisClient> GetRedisClientAsync(string ip, int port)
        {
            var client = await RedisClient.ConnectAsync(ip, port);
            return client;
        }

        /// <summary>
        /// 将指定项目(数组)存储到Set(异步)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="redisClient">Redis客户端实例</param>
        /// <param name="key">Set键值</param>
        /// <param name="items">需要存储在此Set中的Item数组</param>
        /// <returns>存储成功的项目个数</returns>
        public static async Task<long> SaveSetAsync<T>(this IRedisClient redisClient, string key, T[] items)
        {
            long x = await redisClient.SAddAsync<T>(key, items);
            return x;
        }

        /// <summary>
        /// 得到指定Set中的项目(异步)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="redisClient">Redis客户端实例</param>
        /// <param name="setName">Set键值</param>
        /// <returns>此Set中的所有项目，以数组的形式返回</returns>
        public static async Task<T[]> GetSetAsync<T>(this IRedisClient redisClient, string key)
        {
            var task = redisClient.ExecuteAsync("smembers", key);

            var result = await task.ContinueWith<T[]>(res =>
            {
                List<T> items = new List<T>();
                if (res.Result.AsMultiBulk().Count() > 0)
                {
                    foreach (var item in res.Result.AsMultiBulk())
                    {
                        items.Add(item.As<T>());
                    }
                }
                return items.ToArray<T>();
            });

            return result;
        }

        #endregion

        #region Sync Actions
        
        /// <summary>
        /// 得到一个Redis客户端实例
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        /// <returns>Redis客户端实例</returns>
        public static IRedisClient GetRedisClient(string ip, int port)
        {
            var task = RedisClient.ConnectAsync(ip, port);
            task.Wait();
            return task.Result;
        }

        /// <summary>
        /// 将指定项目(数组)存储到Set
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="redisClient">Redis客户端实例</param>
        /// <param name="key">Set键值</param>
        /// <param name="items">需要存储在此Set中的Item数组</param>
        /// <returns>存储成功的项目个数</returns>
        public static long SaveSet<T>(this IRedisClient redisClient, string key, T[] items)
        {
            var task = redisClient.SAddAsync<T>(key, items);
            task.Wait();
            return task.Result;
        }

        /// <summary>
        /// 得到指定Set中的项目
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="redisClient">Redis客户端实例</param>
        /// <param name="setName">Set键值</param>
        /// <returns>此Set中的所有项目，以数组的形式返回</returns>
        public static T[] GetSet<T>(this IRedisClient redisClient, string key)
        {
            List<T> items = new List<T>();

            var t = redisClient.ExecuteAsync("smembers", key);
            t.Wait();

            if (t.Result.AsMultiBulk().Count() > 0)
            {
                foreach (var item in t.Result.AsMultiBulk())
                {
                    items.Add(item.As<T>());
                }
            }
            return items.ToArray<T>();
        }

        #endregion
    }

    public class JsonSerializer : BasicRedisSerializer
    {
        internal static readonly byte[] Null = new byte[] { 0 };
        internal const string DatetimeFormat = "yyyy-MM-dd'T'HH:mm:ss.fffffff";

        #region Serialization

        public override byte[] Serialize(object value)
        {
            if (value == null) return Null;

            var type = value.GetType();

            if (type == typeof(string))
                return SerializeString(value.ToString());
            if (type == typeof(byte[]))
                return value as byte[];
            if (type.IsEnum)
                return SerializeString(value.ToString());
            if (type == typeof(DateTime))
                return SerializeString((value as IFormattable).ToString(DatetimeFormat, CultureInfo.InvariantCulture));
            if (type == typeof(Guid))
                return SerializeString(value.ToString());
            if (type == typeof(int) || type == typeof(long) || type == typeof(byte) || type == typeof(short) ||
                type == typeof(uint) || type == typeof(ulong) || type == typeof(sbyte) || type == typeof(ushort) ||
                type == typeof(bool) || type == typeof(decimal) || type == typeof(double) || type == typeof(char))
                return SerializeString((value as IConvertible).ToString(CultureInfo.InvariantCulture));

            var result = SerializeComplexValue(value);

            if (result.SequenceEqual(Null))
                throw new SerializationException("Serializer returned unexpected result. byte[]{0} value is reserved for NULL");

            return result;
        }

        protected virtual byte[] SerializeComplexValue(object value)
        {
            string json = JsonConvert.SerializeObject(value);
            return Encoding.UTF8.GetBytes(json);
        }

        #endregion

        #region Deserialization

        public override object Deserialize(Type type, byte[] value)
        {
            if (value == null || value.SequenceEqual(Null)) return null;

            if (type == typeof(string))
                return DeserializeToString(value);
            if (type == typeof(byte[]))
                return value;
            if (type.IsEnum)
                return DeserializeToEnum(DeserializeToString(value), type);
            if (type == typeof(DateTime))
                return DateTime.ParseExact(DeserializeToString(value), DatetimeFormat, CultureInfo.InvariantCulture);
            if (type == typeof(Guid))
                return Guid.Parse(DeserializeToString(value));
            if (type == typeof(int))
                return int.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(long))
                return long.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(byte))
                return byte.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(short))
                return short.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(uint))
                return uint.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(ulong))
                return ulong.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(sbyte))
                return sbyte.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(ushort))
                return ushort.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(bool))
                return bool.Parse(DeserializeToString(value));
            if (type == typeof(decimal))
                return decimal.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(double))
                return double.Parse(DeserializeToString(value), CultureInfo.InvariantCulture);
            if (type == typeof(char))
                return DeserializeToString(value)[0];

            return DeserializeComplexValue(type, value);
        }

        protected override object DeserializeComplexValue(Type type, byte[] value)
        {
            string json = Encoding.UTF8.GetString(value);
            return JsonConvert.DeserializeObject(json, type);
        }

        #endregion

        private static object DeserializeToEnum(string value, Type enumType)
        {
            try
            {
                return Enum.Parse(enumType, value, true);
            }
            catch (Exception ex)
            {
                throw new SerializationException("Invalid enum value. Enum type: " + enumType.Name, ex);
            }
        }

        private static byte[] SerializeString(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        private static string DeserializeToString(byte[] value)
        {
            return Encoding.UTF8.GetString(value);
        }
    }
}
