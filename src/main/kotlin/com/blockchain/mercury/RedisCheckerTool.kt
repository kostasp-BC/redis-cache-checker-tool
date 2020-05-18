package com.blockchain.mercury

import org.apache.http.client.utils.URIBuilder
import redis.clients.jedis.JedisPool
import redis.clients.jedis.util.SafeEncoder
import java.io.File

object RedisCheckerTool {
    @JvmStatic
    fun main(args: Array<String>) {
        val host = args[0]
        val port = args[1].toInt()
        val pwd = args[2]
        val file = args[3]

        val redisUri = URIBuilder()
            .setScheme("redis")
            .setHost(host)
            .setPort(port)
            .also { builder ->
                if (!pwd.isNullOrBlank()) {
                    builder.setUserInfo("usr", pwd)
                }
            }
            .build()

        val jedisPool = JedisPool(redisUri)
        jedisPool.resource!!.use { jedis ->
            File(file).forEachLine { line ->
                var lineTokens: List<String>
                var userId = ""
                var orderId = ""
                try {
                    lineTokens = line.split(",")
                    userId = lineTokens[0]
                    orderId = lineTokens[1]
                    val exists = jedis.hexists(SafeEncoder.encode(userId), SafeEncoder.encode(orderId))
                    if (exists) println("orderId=$orderId, user=$userId @ EXISTS")
                    else println("orderId=$orderId, user=$userId @ DOES NOT EXIST")
                } catch (e: Exception) {
                    println(
                        "Failed to check:\n" +
                            "line=$line\n" +
                            "orderId=$orderId\n" +
                            "userId=$userId\n" +
                            "exc=${e.message}"
                    )
                }
            }
        }
        jedisPool.close()
    }
}