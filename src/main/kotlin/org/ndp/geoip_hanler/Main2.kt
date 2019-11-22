package org.ndp.geoip_hanler

import me.liuwj.ktorm.database.Database
import me.liuwj.ktorm.dsl.*
import org.ndp.geoip_hanler.beans.Block
import org.ndp.geoip_hanler.beans.Location
import java.util.*

fun iNetString2Number(ipStr: String): Long =
        Arrays.stream(ipStr.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
                .map { java.lang.Long.parseLong(it) }
                .reduce(0L) { x, y -> (x!! shl 8) + y!! }

fun parseIPEnd(ipStart: Long, mask: Int): Long = ipStart or (((1 shl 32 - mask) - 1).toLong())

fun main() {
    Database.Companion.connect(
            "jdbc:mysql://10.0.21.120:3306/ndp?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
            "com.mysql.jdbc.Driver",
            "root",
            "password"
    )
    Block.select(Block.network, Block.id)
            .where {
                Block.geoNameID inList
                        Location.select(Location.geoNameID).where { Location.countryISOCode eq "CN" }
            }
            .forEach { row ->
                val ipSet = row[Block.network]!!.split("/")
                val ipStart = iNetString2Number(ipSet[0])
                val mask = ipSet[1].toInt()
                val ipEnd = parseIPEnd(ipStart, mask)
                val id = row[Block.id]
                Block.update {
                    it.longIPStart to ipStart
                    it.longIPEnd to ipEnd

                    where {
                        (it.id greater 3080293) and (it.id.toInt() eq id!!.toInt())
                    }
                }
            }
}