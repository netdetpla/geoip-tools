package org.ndp.geoip_hanler

import me.liuwj.ktorm.database.Database
import me.liuwj.ktorm.dsl.*
import org.apache.http.Consts
import org.ndp.geoip_hanler.beans.Block
import org.ndp.geoip_hanler.beans.Location
import java.io.File
import java.util.*
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.ndp.geoip_hanler.beans.IP
import java.lang.Exception
import java.net.URI
import java.nio.charset.StandardCharsets

fun postTask(
        url: String,
        imageName: String,
        tag: String,
        taskName: String,
        priority: String,
        ips: String,
        ports: String
) {
    val params = ArrayList<NameValuePair>()
    params.add(BasicNameValuePair("image-name", imageName))
    params.add(BasicNameValuePair("tag", tag))
    params.add(BasicNameValuePair("task-name", taskName))
    params.add(BasicNameValuePair("priority", priority))
    params.add(BasicNameValuePair("params[]", ips))
    params.add(BasicNameValuePair("params[]", ports))
    val httpPost = HttpPost()
    httpPost.uri = URI.create(url)
    httpPost.entity = UrlEncodedFormEntity(params, Consts.UTF_8)

    val httpClient = HttpClients.createMinimal()
    try {
        val response = httpClient.execute(httpPost)
        val entity = response.entity
        val result = EntityUtils.toString(entity, StandardCharsets.UTF_8)
        println(result)
    } catch (e: Exception) {

    }
}


fun main(args: Array<String>) {
    Database.Companion.connect(
            "jdbc:mysql://10.0.21.120:3306/ndp?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
            "com.mysql.jdbc.Driver",
            "root",
            "password"
    )
    val save = File("./config.txt")
    val offset = save.readText().toInt()
//    val limit = args[0].toInt()
    val limit = 10
    val target = Block
            .select(Block.network, Block.geoNameID)
            .where {
                Block.geoNameID inList
                        Location.select(Location.geoNameID).where { Location.countryISOCode eq "CN" }
            }
            .limit(offset, limit)
    save.writeText((offset + limit).toString())
    for (t in target) {
        val iNet4 = t[Block.network]!!
        postTask(
                "http://10.0.21.120:8080/task",
                "scanservice",
                "1.0.0",
                "test$iNet4",
                "5",
                iNet4,
                "1-10000"
        )
    }
}