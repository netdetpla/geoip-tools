package org.ndp.geoip_hanler.beans

import me.liuwj.ktorm.schema.Table
import me.liuwj.ktorm.schema.int
import me.liuwj.ktorm.schema.text
import me.liuwj.ktorm.schema.varchar

object IP : Table<Nothing>("ip") {
    val id by int("id").primaryKey()
    val ip by varchar("ip")
    val osVersion by text("os_version")
    val hardware by text("hardware")
    val lnglatID by int("lnglat_id")
}
