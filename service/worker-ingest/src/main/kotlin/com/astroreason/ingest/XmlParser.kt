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
                            currentPerson = mutableMapOf()
                            currentPerson!!["adb_id"] = element.getAttributeByName(
                                javax.xml.namespace.QName("id")
                            )?.value
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
                            
                            when {
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
                        }
                        
                        if (name == "person" && inPerson && currentPerson != null) {
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

fun sha256(text: String): String {
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(text.toByteArray())
    return hash.joinToString("") { "%02x".format(it) }
}
