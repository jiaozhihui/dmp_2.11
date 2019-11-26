package com.bjvca.dmp.commonutils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class JedisPools(host:String, password:String, db:Integer) extends Serializable {

  val config = new JedisPoolConfig()

  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, host, 6379, 10000, password, db)

  def getJedis(): Jedis = {
    pool.getResource
  }

}
