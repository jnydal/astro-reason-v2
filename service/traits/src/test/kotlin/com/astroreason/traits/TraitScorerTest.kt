package com.astroreason.traits

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TraitScorerTest {
    private val scorer = TraitScorer(baseUrl = "http://example.com", model = "test-model")

    @Test
    fun `normalizes fenced json`() {
        val raw = """
            ```json
            {"vectors":{"sound":4,"visual":4,"oral":4,"anal":4,"urethral":4,"skin":4,"muscular":4,"olfactory":4},"dominant":["sound","visual"],"rationale":{"sound":"insufficient evidence","visual":"insufficient evidence","oral":"insufficient evidence","anal":"insufficient evidence","urethral":"insufficient evidence","skin":"insufficient evidence","muscular":"insufficient evidence","olfactory":"insufficient evidence"},"confidence":0.2}
            ```
        """.trimIndent()

        val normalized = scorer.normalizeJsonResponse(raw)

        assertEquals(
            """{"vectors":{"sound":4,"visual":4,"oral":4,"anal":4,"urethral":4,"skin":4,"muscular":4,"olfactory":4},"dominant":["sound","visual"],"rationale":{"sound":"insufficient evidence","visual":"insufficient evidence","oral":"insufficient evidence","anal":"insufficient evidence","urethral":"insufficient evidence","skin":"insufficient evidence","muscular":"insufficient evidence","olfactory":"insufficient evidence"},"confidence":0.2}""",
            normalized
        )
    }

    @Test
    fun `keeps plain json intact after trim`() {
        val raw = "  \n{\"vectors\":{},\"dominant\":[],\"rationale\":{},\"confidence\":0.0}\n  "

        val normalized = scorer.normalizeJsonResponse(raw)

        assertEquals("""{"vectors":{},"dominant":[],"rationale":{},"confidence":0.0}""", normalized)
    }
}
