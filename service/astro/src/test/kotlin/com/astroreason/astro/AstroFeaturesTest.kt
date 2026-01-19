package com.astroreason.astro

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import java.time.LocalDate
import java.time.LocalTime
import java.util.*

class AstroFeaturesTest {
    
    @Test
    fun testComputeFeaturesMinimal() {
        val personId = UUID.randomUUID()
        val date = LocalDate.of(1984, 6, 12)
        val time = LocalTime.of(6, 30, 0)
        val tzOffsetMinutes = 60 // UTC+1
        val lat = 59.91 // Oslo-ish
        val lon = 10.75
        
        val feats = computeFeaturesForPerson(
            personId = personId,
            date = date,
            time = time,
            tzOffsetMinutes = tzOffsetMinutes,
            lat = lat,
            lon = lon,
            backend = "fallback"
        )
        
        // Basic shape checks
        assertEquals("fallback", feats.system)
        assertNotNull(feats.jdUtc)
        assertEquals(false, feats.unknownTime)
        
        // Longitudes should be in valid range
        assert(feats.longs.sun >= 0.0 && feats.longs.sun < 360.0)
        assert(feats.longs.moon >= 0.0 && feats.longs.moon < 360.0)
        assert(feats.longs.mercury >= 0.0 && feats.longs.mercury < 360.0)
        
        // Houses are null in fallback
        assertEquals(null, feats.houses)
        
        // Aspects should be a list
        assertNotNull(feats.aspects)
        
        // Element/modality ratios should sum to ~1.0
        val elemSum = feats.elemRatios.fire + feats.elemRatios.earth + 
                     feats.elemRatios.air + feats.elemRatios.water
        assert(kotlin.math.abs(elemSum - 1.0) < 1e-6)
        
        val modSum = feats.modalityRatios.cardinal + feats.modalityRatios.fixed + 
                     feats.modalityRatios.mutable
        assert(kotlin.math.abs(modSum - 1.0) < 1e-6)
        
        // Feature vector must include sin/cos entries for each planet
        for (planet in AstroConstants.PLANETS) {
            assert(feats.featureVec.containsKey("lon_${planet}_sin"))
            assert(feats.featureVec.containsKey("lon_${planet}_cos"))
        }
    }
    
    @Test
    fun testAngDistance() {
        assertEquals(10.0, angDistance(0.0, 10.0), 0.001)
        assertEquals(10.0, angDistance(350.0, 0.0), 0.001)
        assertEquals(180.0, angDistance(0.0, 180.0), 0.001)
    }
    
    @Test
    fun testSignFromLongitude() {
        val (sign, deg) = signFromLongitude(15.0)
        assertEquals("aries", sign)
        assert(deg >= 0.0 && deg < 30.0)
        
        val (sign2, _) = signFromLongitude(45.0)
        assertEquals("taurus", sign2)
    }
}
