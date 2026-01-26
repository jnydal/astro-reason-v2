package com.astroreason.ingest

import java.io.File
import java.io.FileInputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.*

data class PersonRecord(
    val adbId: String?,
    val fullName: String?,
    val date: String?,
    val time: String?,
    val tz: String?,
    val place: String?,
    val lat: Double?,
    val lon: Double?,
    val rating: String?,
    val bioText: String?
)

class XmlParser {
    fun iterPeople(xmlPath: String): Sequence<PersonRecord> = sequence {
        val factory = XMLInputFactory.newInstance()
        factory.setProperty(XMLInputFactory.IS_COALESCING, true)
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
        
        FileInputStream(File(xmlPath)).use { stream ->
            val reader = factory.createXMLEventReader(stream)
            
            var currentPerson: MutableMap<String, String?>? = null
            var currentPersonType: String? = null
            var currentPath = mutableListOf<String>()
            var currentText = StringBuilder()
            var inPerson = false
            
            while (reader.hasNext()) {
                val event = reader.nextEvent()
                
                when {
                    event.isStartElement -> {
                        val element = event.asStartElement()
                        val name = element.name.localPart.lowercase()
                        currentPath.add(name)
                        
                        if (name == "person") {
                            inPerson = true
                            currentPersonType = "person"
                            currentPerson = mutableMapOf()
                            currentPerson!!["adb_id"] = element.getAttributeByName(
                                javax.xml.namespace.QName("id")
                            )?.value
                        }

                        if (name == "adb_entry") {
                            inPerson = true
                            currentPersonType = "adb_entry"
                            currentPerson = mutableMapOf()
                            currentPerson!!["adb_id"] = element.getAttributeByName(
                                javax.xml.namespace.QName("adb_id")
                            )?.value
                        }

                        if (inPerson && currentPersonType == "adb_entry" && name == "place") {
                            val latRaw = element.getAttributeByName(
                                javax.xml.namespace.QName("slati")
                            )?.value
                            val lonRaw = element.getAttributeByName(
                                javax.xml.namespace.QName("slong")
                            )?.value
                            parseCoord(latRaw)?.let { currentPerson!!["lat"] = it.toString() }
                            parseCoord(lonRaw)?.let { currentPerson!!["lon"] = it.toString() }
                        }
                        
                        currentText.clear()
                    }
                    
                    event.isCharacters -> {
                        if (inPerson) {
                            currentText.append(event.asCharacters().data)
                        }
                    }
                    
                    event.isEndElement -> {
                        val element = event.asEndElement()
                        val name = element.name.localPart.lowercase()
                        
                        if (inPerson && currentPerson != null) {
                            val text = currentText.toString().trim()
                            val path = currentPath.joinToString("/")
                            
                            when (currentPersonType) {
                                "person" -> when {
                                    path.endsWith("/id") && name == "id" -> {
                                        currentPerson!!["adb_id"] = text.ifEmpty { currentPerson!!["adb_id"] }
                                    }
                                    path.endsWith("/name") && name == "name" -> {
                                        currentPerson!!["full_name"] = text
                                    }
                                    path.endsWith("/birth/date") && name == "date" -> {
                                        currentPerson!!["date"] = text
                                    }
                                    path.endsWith("/birth/time") && name == "time" -> {
                                        currentPerson!!["time"] = text
                                    }
                                    path.endsWith("/birth/tz") && name == "tz" -> {
                                        currentPerson!!["tz"] = text
                                    }
                                    path.endsWith("/birth/place/name") && name == "name" -> {
                                        currentPerson!!["place"] = text
                                    }
                                    path.endsWith("/birth/place/lat") && name == "lat" -> {
                                        currentPerson!!["lat"] = text
                                    }
                                    path.endsWith("/birth/place/lon") && name == "lon" -> {
                                        currentPerson!!["lon"] = text
                                    }
                                    (path.endsWith("/birth/rodden_rating") || path.endsWith("/birth/rating")) &&
                                        (name == "rodden_rating" || name == "rating") -> {
                                        currentPerson!!["rating"] = text
                                    }
                                    (path.endsWith("/bio") || path.endsWith("/biography")) &&
                                        (name == "bio" || name == "biography") -> {
                                        currentPerson!!["bio_text"] = text
                                    }
                                }
                                "adb_entry" -> when {
                                    path.endsWith("/public_data/name") && name == "name" -> {
                                        currentPerson!!["full_name"] = text
                                    }
                                    path.endsWith("/public_data/bdata/sbdate") && name == "sbdate" -> {
                                        currentPerson!!["date"] = text.replace("/", "-")
                                    }
                                    path.endsWith("/public_data/bdata/sbtime") && name == "sbtime" -> {
                                        currentPerson!!["time"] = text
                                    }
                                    path.endsWith("/public_data/bdata/place") && name == "place" -> {
                                        currentPerson!!["place"] = text
                                    }
                                    path.endsWith("/public_data/roddenrating") && name == "roddenrating" -> {
                                        currentPerson!!["rating"] = text
                                    }
                                    path.endsWith("/text_data/shortbiography") && name == "shortbiography" -> {
                                        currentPerson!!["bio_text"] = text
                                    }
                                }
                            }
                        }
                        
                        if ((name == "person" || name == "adb_entry") && inPerson && currentPerson != null) {
                            val adbId = currentPerson!!["adb_id"]
                            val fullName = currentPerson!!["full_name"]
                            
                            if (!adbId.isNullOrBlank() && !fullName.isNullOrBlank()) {
                                yield(PersonRecord(
                                    adbId = adbId,
                                    fullName = fullName,
                                    date = currentPerson!!["date"],
                                    time = currentPerson!!["time"],
                                    tz = currentPerson!!["tz"],
                                    place = currentPerson!!["place"],
                                    lat = currentPerson!!["lat"]?.toDoubleOrNull(),
                                    lon = currentPerson!!["lon"]?.toDoubleOrNull(),
                                    rating = currentPerson!!["rating"],
                                    bioText = currentPerson!!["bio_text"]
                                ))
                            }
                            
                            inPerson = false
                            currentPersonType = null
                            currentPerson = null
                        }
                        
                        if (currentPath.isNotEmpty()) {
                            currentPath.removeAt(currentPath.size - 1)
                        }
                        currentText.clear()
                    }
                }
            }
        }
    }
}

fun tzToMinutes(tz: String?): Int? {
    if (tz.isNullOrBlank()) return null
    val sign = if (tz.contains("-") || tz.contains("−")) -1 else 1
    val cleaned = tz.replace("+", "").replace("-", "").replace("−", "")
    val parts = cleaned.split(":")
    val h = parts[0].toIntOrNull() ?: 0
    val m = if (parts.size > 1) parts[1].toIntOrNull() ?: 0 else 0
    return sign * (h * 60 + m)
}

private fun parseCoord(raw: String?): Double? {
    if (raw.isNullOrBlank()) return null
    val cleaned = raw.trim().lowercase()
    val directionIndex = cleaned.indexOfFirst { it == 'n' || it == 's' || it == 'e' || it == 'w' }
    if (directionIndex <= 0) return cleaned.toDoubleOrNull()
    val degPart = cleaned.substring(0, directionIndex).toIntOrNull() ?: return null
    val dir = cleaned[directionIndex]
    val minPart = cleaned.substring(directionIndex + 1).toIntOrNull() ?: 0
    val sign = if (dir == 's' || dir == 'w') -1 else 1
    return sign * (degPart + (minPart / 60.0))
}

fun sha256(text: String): String {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(text.toByteArray())
    return hash.joinToString("") { "%02x".format(it) }
}
