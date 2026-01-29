package com.astroreason.api.stats

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldContainExactly

class StatsServiceTest : StringSpec({
    "parsePgVector handles bracketed arrays" {
        parsePgVector("[1, 2,3]").shouldContainExactly(1.0, 2.0, 3.0)
    }

    "parsePgVector handles parenthesized arrays" {
        parsePgVector("(1.25, -2.5, 3)").shouldContainExactly(1.25, -2.5, 3.0)
    }

    "parsePgVector handles empty values" {
        parsePgVector("[]").shouldContainExactly()
    }
})
