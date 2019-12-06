package com.wang.dmp.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisPools {

    private lazy val jedisPool = new JedisPool(ConfigHandler.redisHost,ConfigHandler.redisPort)
    def getJedis: Jedis = {
        val jedis = jedisPool.getResource
        jedis.select(ConfigHandler.redisIndex)
        jedis //返回
    }

}
