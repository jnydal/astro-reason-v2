package com.astroreason.astro

import com.astroreason.core.Config
import com.astroreason.core.DatabaseManager
import com.astroreason.core.logProvenanceEvent
import com.astroreason.core.schema.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.*
import java.util.*
import kotlin.math.*

@Serializable
data class PlanetLongitudes(
    val sun: Double,
    val moon: Double,
    val mercury: Double,
    val venus: Double,
    val mars: Double,
    val jupiter: Double,
    val saturn: Double,
    val uranus: Double,
    val neptune: Double,
    val pluto: Double
)

@Serializable
data class Aspect(
    val a: String,
    val b: String,
    val aspect: String,
    val angle: Double,
    val deviation: Double,
    val strength: Double
)

@Serializable
data class ElementRatios(
    val fire: Double,
    val earth: Double,
    val air: Double,
    val water: Double
)

@Serializable
data class ModalityRatios(
    val cardinal: Double,
    val fixed: Double,
    val mutable: Double
)

@Serializable
data class AstroFeaturesData(
    val system: String,
    val jdUtc: Double,
    val unknownTime: Boolean,
    val longs: PlanetLongitudes,
    val houses: Map<String, Double>? = null,
    val aspects: List<Aspect>,
    val elemRatios: ElementRatios,
    val modalityRatios: ModalityRatios,
    val featureVec: Map<String, Double>
)

object AstroConstants {
    val PLANETS = listOf(
        "sun", "moon", "mercury", "venus", "mars",
        "jupiter", "saturn", "uranus", "neptune", "pluto"
    )
    
    val ASPECTS = mapOf(
        "conjunction" to 0.0,
        "opposition" to 180.0,
        "trine" to 120.0,
        "square" to 90.0,
        "sextile" to 60.0
    )
    
    val SIGNS = listOf(
        "aries", "taurus", "gemini", "cancer", "leo", "virgo",
        "libra", "scorpio", "sagittarius", "capricorn", "aquarius", "pisces"
    )
    
    val SIGN_ELEMENTS = mapOf(
        "aries" to "fire", "leo" to "fire", "sagittarius" to "fire",
        "taurus" to "earth", "virgo" to "earth", "capricorn" to "earth",
        "gemini" to "air", "libra" to "air", "aquarius" to "air",
        "cancer" to "water", "scorpio" to "water", "pisces" to "water"
    )
    
    val SIGN_MODALITIES = mapOf(
        "aries" to "cardinal", "cancer" to "cardinal", "libra" to "cardinal", "capricorn" to "cardinal",
        "taurus" to "fixed", "leo" to "fixed", "scorpio" to "fixed", "aquarius" to "fixed",
        "gemini" to "mutable", "virgo" to "mutable", "sagittarius" to "mutable", "pisces" to "mutable"
    )
}

fun wrap360(x: Double): Double = x % 360.0

fun angDistance(a: Double, b: Double): Double {
    val d = abs(wrap360(a) - wrap360(b))
    return if (d <= 180) d else 360 - d
}

fun signFromLongitude(lon: Double): Pair<String, Double> {
    val wrapped = wrap360(lon)
    val idx = (wrapped / 30).toInt()
    val degInSign = wrapped - idx * 30
    return AstroConstants.SIGNS[idx] to degInSign
}

fun toJulianDay(date: LocalDate, time: LocalTime?, tzOffsetMinutes: Int?): Pair<Double, Boolean> {
    val unknownTime = time == null
    val localTime = time ?: LocalTime.of(12, 0)
    
    val offset = tzOffsetMinutes ?: 0
    val dtLocal = LocalDateTime.of(date, localTime)
    val dtUtc = dtLocal.minusMinutes(offset.toLong())
    
    // Simplified Julian Day calculation
    val year = dtUtc.year
    val month = dtUtc.monthValue
    val day = dtUtc.dayOfMonth
    val hour = dtUtc.hour
    val minute = dtUtc.minute
    val second = dtUtc.second
    
    val a = (14 - month) / 12
    val y = year + 4800 - a
    val m = month + 12 * a - 3
    val jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045
    val frac = (hour - 12) / 24.0 + minute / 1440.0 + second / 86400.0
    val jd = jdn + frac
    
    return jd to unknownTime
}

/**
 * Swiss Ephemeris backend using the official Java wrapper.
 *
 * This uses high‑precision planetary positions from the Swiss Ephemeris
 * and optional house cusps when latitude/longitude are available.
 */
class SwissEphemerisBackend(ephePath: String? = null) {
    private val swe: Any
    private val calcMethod: java.lang.reflect.Method
    private val housesMethod: java.lang.reflect.Method
    private val flags: Int
    private val bodyMap: Map<String, Int>

    init {
        val sweClass = Class.forName("swisseph.SwissEph")
        val sweConst = Class.forName("swisseph.SweConst")

        swe = if (ephePath != null) {
            sweClass.getConstructor(String::class.java).newInstance(ephePath)
        } else {
            sweClass.getConstructor().newInstance()
        }

        calcMethod = sweClass.getMethod(
            "swe_calc_ut",
            Double::class.javaPrimitiveType,
            Int::class.javaPrimitiveType,
            Int::class.javaPrimitiveType,
            DoubleArray::class.java,
            StringBuffer::class.java
        )
        housesMethod = sweClass.getMethod(
            "swe_houses",
            Double::class.javaPrimitiveType,
            Double::class.javaPrimitiveType,
            Double::class.javaPrimitiveType,
            Int::class.javaPrimitiveType,
            DoubleArray::class.java,
            DoubleArray::class.java
        )

        val flagsSwieph = sweConst.getField("SEFLG_SWIEPH").getInt(null)
        val flagsSpeed = sweConst.getField("SEFLG_SPEED").getInt(null)
        flags = flagsSwieph or flagsSpeed

        bodyMap = mapOf(
            "sun" to sweConst.getField("SE_SUN").getInt(null),
            "moon" to sweConst.getField("SE_MOON").getInt(null),
            "mercury" to sweConst.getField("SE_MERCURY").getInt(null),
            "venus" to sweConst.getField("SE_VENUS").getInt(null),
            "mars" to sweConst.getField("SE_MARS").getInt(null),
            "jupiter" to sweConst.getField("SE_JUPITER").getInt(null),
            "saturn" to sweConst.getField("SE_SATURN").getInt(null),
            "uranus" to sweConst.getField("SE_URANUS").getInt(null),
            "neptune" to sweConst.getField("SE_NEPTUNE").getInt(null),
            "pluto" to sweConst.getField("SE_PLUTO").getInt(null)
        )
    }

    fun getPlanetLongitudes(jdUt: Double): PlanetLongitudes {
        val serr = StringBuffer()
        val res = DoubleArray(6)

        fun calc(bodyKey: String): Double {
            val body = bodyMap[bodyKey] ?: error("Unknown SwissEph body: $bodyKey")
            val rc = (calcMethod.invoke(swe, jdUt, body, flags, res, serr) as Int)
            if (rc < 0) {
                throw RuntimeException("Swiss Ephemeris error for body=$bodyKey: $serr")
            }
            // res[0] = ecliptic longitude in degrees
            return wrap360(res[0])
        }

        return PlanetLongitudes(
            sun = calc("sun"),
            moon = calc("moon"),
            mercury = calc("mercury"),
            venus = calc("venus"),
            mars = calc("mars"),
            jupiter = calc("jupiter"),
            saturn = calc("saturn"),
            uranus = calc("uranus"),
            neptune = calc("neptune"),
            pluto = calc("pluto")
        )
    }

    /**
     * Compute Placidus house cusps using Swiss Ephemeris.
     */
    fun getHouses(jdUt: Double, lat: Double, lon: Double, system: String = "P"): Map<String, Double>? {
        val cusps = DoubleArray(13)
        val ascmc = DoubleArray(10)
        // SwissEph expects a single‑char int code for the house system
        housesMethod.invoke(swe, jdUt, lat, lon, system.first().code, cusps, ascmc)

        val result = mutableMapOf<String, Double>()
        for (i in 1..12) {
            result["house_$i"] = wrap360(cusps[i])
        }
        // Optionally expose ASC/MC later via ascmc
        return result
    }
}

// Fallback using basic calculations (simplified, low‑precision)
class FallbackBackend {
    fun getPlanetLongitudes(jdUt: Double): PlanetLongitudes {
        // Simplified calculation - in production, use proper ephemeris
        // This is a placeholder that returns approximate values
        val baseLon = (jdUt * 0.9856) % 360.0 // Approximate Sun position
        
        return PlanetLongitudes(
            sun = baseLon,
            moon = (baseLon * 13.1764) % 360.0,
            mercury = (baseLon * 4.0923) % 360.0,
            venus = (baseLon * 1.6021) % 360.0,
            mars = (baseLon * 0.5240) % 360.0,
            jupiter = (baseLon * 0.0831) % 360.0,
            saturn = (baseLon * 0.0335) % 360.0,
            uranus = (baseLon * 0.0118) % 360.0,
            neptune = (baseLon * 0.0060) % 360.0,
            pluto = (baseLon * 0.0040) % 360.0
        )
    }
}

fun computeAspects(longs: PlanetLongitudes): List<Aspect> {
    val planets = listOf(
        "sun" to longs.sun, "moon" to longs.moon, "mercury" to longs.mercury,
        "venus" to longs.venus, "mars" to longs.mars, "jupiter" to longs.jupiter,
        "saturn" to longs.saturn, "uranus" to longs.uranus, "neptune" to longs.neptune,
        "pluto" to longs.pluto
    )
    
    val aspects = mutableListOf<Aspect>()
    
    for (i in planets.indices) {
        for (j in (i + 1) until planets.size) {
            val (nameA, lonA) = planets[i]
            val (nameB, lonB) = planets[j]
            val dist = angDistance(lonA, lonB)
            
            var bestAspect: Pair<String, Double>? = null
            var bestDev = 999.0
            
            for ((aspName, aspAngle) in AstroConstants.ASPECTS) {
                val dev = abs(dist - aspAngle)
                val orb = 8.0 // Simplified orb
                if (dev <= orb && dev < bestDev) {
                    bestAspect = aspName to aspAngle
                    bestDev = dev
                }
            }
            
            if (bestAspect != null) {
                val (aspName, aspAngle) = bestAspect
                val strength = max(0.0, min(1.0, (8.0 - bestDev) / 8.0))
                aspects.add(Aspect(
                    a = nameA,
                    b = nameB,
                    aspect = aspName,
                    angle = dist,
                    deviation = bestDev,
                    strength = strength
                ))
            }
        }
    }
    
    return aspects
}

fun elemModalityTallies(longs: PlanetLongitudes): Pair<ElementRatios, ModalityRatios> {
    val elems = mutableMapOf("fire" to 0.0, "earth" to 0.0, "air" to 0.0, "water" to 0.0)
    val mods = mutableMapOf("cardinal" to 0.0, "fixed" to 0.0, "mutable" to 0.0)
    
    val planetLongs = listOf(
        longs.sun, longs.moon, longs.mercury, longs.venus, longs.mars,
        longs.jupiter, longs.saturn, longs.uranus, longs.neptune, longs.pluto
    )
    
    for (lon in planetLongs) {
        val (sign, _) = signFromLongitude(lon)
        elems[AstroConstants.SIGN_ELEMENTS[sign]!!] = elems[AstroConstants.SIGN_ELEMENTS[sign]!!]!! + 1.0
        mods[AstroConstants.SIGN_MODALITIES[sign]!!] = mods[AstroConstants.SIGN_MODALITIES[sign]!!]!! + 1.0
    }
    
    val se = elems.values.sum()
    val sm = mods.values.sum()
    
    return ElementRatios(
        fire = (elems["fire"]!! / se).roundTo(6),
        earth = (elems["earth"]!! / se).roundTo(6),
        air = (elems["air"]!! / se).roundTo(6),
        water = (elems["water"]!! / se).roundTo(6)
    ) to ModalityRatios(
        cardinal = (mods["cardinal"]!! / sm).roundTo(6),
        fixed = (mods["fixed"]!! / sm).roundTo(6),
        mutable = (mods["mutable"]!! / sm).roundTo(6)
    )
}

fun Double.roundTo(decimals: Int): Double {
    var multiplier = 1.0
    repeat(decimals) { multiplier *= 10 }
    return round(this * multiplier) / multiplier
}

fun flattenFeatureVec(
    longs: PlanetLongitudes,
    aspects: List<Aspect>,
    elems: ElementRatios,
    mods: ModalityRatios
): Map<String, Double> {
    val fv = mutableMapOf<String, Double>()
    
    // Planetary longitudes (sin/cos encoding)
    val planetLongs = listOf(
        "sun" to longs.sun, "moon" to longs.moon, "mercury" to longs.mercury,
        "venus" to longs.venus, "mars" to longs.mars, "jupiter" to longs.jupiter,
        "saturn" to longs.saturn, "uranus" to longs.uranus, "neptune" to longs.neptune,
        "pluto" to longs.pluto
    )
    
    for ((name, lon) in planetLongs) {
        val rad = Math.toRadians(lon)
        fv["lon_${name}_sin"] = sin(rad).roundTo(6)
        fv["lon_${name}_cos"] = cos(rad).roundTo(6)
    }
    
    // Aspects: summed strengths per aspect type
    for (aspName in AstroConstants.ASPECTS.keys) {
        fv["aspect_strength_$aspName"] = aspects
            .filter { it.aspect == aspName }
            .sumOf { it.strength }
            .roundTo(6)
    }
    
    // Elements/modalities
    fv["elem_fire"] = elems.fire
    fv["elem_earth"] = elems.earth
    fv["elem_air"] = elems.air
    fv["elem_water"] = elems.water
    fv["mod_cardinal"] = mods.cardinal
    fv["mod_fixed"] = mods.fixed
    fv["mod_mutable"] = mods.mutable
    
    return fv
}

fun computeFeaturesForPerson(
    personId: UUID,
    date: LocalDate,
    time: LocalTime?,
    tzOffsetMinutes: Int?,
    lat: Double?,
    lon: Double?,
    backend: String = "fallback",
    ephePath: String? = null
): AstroFeaturesData {
    val (jd, unknownTime) = toJulianDay(date, time, tzOffsetMinutes)

    var longs: PlanetLongitudes
    var houses: Map<String, Double>?
    var systemName = backend

    if (backend == "swisseph") {
        try {
            val sweBackend = SwissEphemerisBackend(ephePath)
            longs = sweBackend.getPlanetLongitudes(jd)
            houses = if (lat != null && lon != null) {
                try {
                    sweBackend.getHouses(jd, lat, lon, "P")
                } catch (e: Exception) {
                    // Graceful degradation: continue without houses
                    null
                }
            } else {
                null
            }
        } catch (e: Throwable) {
            println("⚠️ Swiss Ephemeris unavailable, falling back: ${e.message}")
            systemName = "fallback"
            val fb = FallbackBackend()
            longs = fb.getPlanetLongitudes(jd)
            houses = null
        }
    } else {
        val fb = FallbackBackend()
        longs = fb.getPlanetLongitudes(jd)
        houses = null
    }
    
    val aspects = computeAspects(longs)
    val (elemRatios, modalityRatios) = elemModalityTallies(longs)
    val featureVec = flattenFeatureVec(longs, aspects, elemRatios, modalityRatios)
    
    return AstroFeaturesData(
        system = systemName,
        jdUtc = jd,
        unknownTime = unknownTime,
        longs = longs,
        houses = houses,
        aspects = aspects,
        elemRatios = elemRatios,
        modalityRatios = modalityRatios,
        featureVec = featureVec
    )
}

fun run(batchSize: Int = 128) {
    Config.initialize()
    val settings = Config.settings
    val backend = settings.astroBackend.lowercase()
    val effectiveBackend = if (backend == "swisseph") "swisseph" else "fallback"
    val startedAt = System.nanoTime()

    transaction(DatabaseManager.getDatabase()) {
        val rows = Birth
            .leftJoin(AstroFeatures, { Birth.id }, { AstroFeatures.id })
            .slice(Birth.id, Birth.date, Birth.time, Birth.tzOffsetMinutes, Birth.lat, Birth.lon)
            .select {
                AstroFeatures.id.isNull()
            }
            .limit(batchSize)
        
        var wrote = 0
        
        for (row in rows) {
            try {
                val personId = row[Birth.id].value
                val date = row[Birth.date] ?: continue
                val time = row[Birth.time]
                val tzOffset = row[Birth.tzOffsetMinutes]
                val lat = row[Birth.lat]
                val lon = row[Birth.lon]

                val feats = computeFeaturesForPerson(
                    personId = personId,
                    date = date,
                    time = time,
                    tzOffsetMinutes = tzOffset,
                    lat = lat,
                    lon = lon,
                    backend = effectiveBackend,
                    ephePath = settings.swephEphePath
                )
                
                val json = Json { ignoreUnknownKeys = true }
                
                AstroFeatures.insert {
                    it[AstroFeatures.id] = personId
                    it[AstroFeatures.system] = feats.system
                    it[AstroFeatures.jdUtc] = feats.jdUtc
                    it[AstroFeatures.unknownTime] = feats.unknownTime
                    it[AstroFeatures.longs] = json.encodeToString(feats.longs)
                    it[AstroFeatures.houses] = feats.houses?.let { json.encodeToString(it) }
                    it[AstroFeatures.aspects] = json.encodeToString(feats.aspects)
                    it[AstroFeatures.elemRatios] = json.encodeToString(feats.elemRatios)
                    it[AstroFeatures.modalityRatios] = json.encodeToString(feats.modalityRatios)
                    it[AstroFeatures.featureVec] = json.encodeToString(feats.featureVec)
                }
                
                wrote++
            } catch (e: Exception) {
                println("[astro_features] Error person_id=${row[Birth.id]}: ${e.message}")
                e.printStackTrace()
            }
        }
        
        commit()
        println("✅ astro_features: wrote $wrote rows using backend=$effectiveBackend")
        logProvenanceEvent(
            stage = "astro",
            status = "ok",
            count = wrote,
            durationMs = (System.nanoTime() - startedAt) / 1_000_000,
            meta = mapOf("backend" to effectiveBackend, "batch_size" to batchSize.toString())
        )
    }
}

fun main() {
    run()
}
