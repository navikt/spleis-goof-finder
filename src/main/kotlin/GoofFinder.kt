import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.LocalDate

private val objectMapper = ObjectMapper()
var personerMedEkstraVedtaksperioder = 0

fun main() {
    val user = System.getenv("DB_USERNAME")
    val pass = System.getenv("DB_PASSWORD")
    DriverManager.getConnection(
        "jdbc:postgresql://a01dbfl039.adeo.no:5432/spleis2",
        user, pass
    ).use { connection ->
        val statement = connection.prepareStatement(
            """
select distinct fnr from person, json_array_elements(data -> 'arbeidsgivere') arbeidsgivere, json_array_elements(arbeidsgivere -> 'vedtaksperioder') vedtaksperioder
WHERE skjema_versjon > 5
  AND opprettet > date '2020-04-29'
  AND opprettet < date '2020-05-05'
  AND json_array_length(vedtaksperioder -> 'sykdomshistorikk' -> 0 -> 'nyHendelseSykdomstidslinje' -> 'dager') = 0;
        """
        )

        val goofedFnrs = statement.executeQuery().map { it.getString("fnr") }

        val lastNonGoofedStatement = connection.prepareStatement(
            """
            SELECT id, opprettet, data FROM person 
            WHERE fnr=? 
                AND skjema_versjon < 6 
            ORDER BY ID DESC LIMIT 1;
            """
        )
        val goofedStatement =
            connection.prepareStatement("SELECT id, opprettet, data FROM person WHERE fnr=? AND id > ?;")

        goofedFnrs.map { fnr ->
            System.err.println("Henter siste kjente ok vedtaksperiode for $fnr")
            lastNonGoofedStatement.setString(1, fnr)
            val nonGoofed = lastNonGoofedStatement.executeQuery()
                .map {
                    NonGoofed(
                        fnr = fnr,
                        id = it.getLong("id"),
                        opprettet = it.getDate("opprettet").toLocalDate(),
                        vedtaksperioder = vedtaksperiodeIder(it.getString("data"))
                    )
                }
                .firstOrNull()
            goofedStatement.setString(1, fnr)
            goofedStatement.setLong(2, nonGoofed?.id ?: 0)
            val goofed = goofedStatement
                .executeQuery()
                .map { it.getString("data") }
            require(goofed.none{ data -> harUtbetalingerEtterGoof(data, nonGoofed?.vedtaksperioder ?: listOf()) }) {
                "Fant en periode som har utbetalinger etter goofed perioder, $fnr"
            }
            nonGoofed ?: IngenTidligereData(fnr)
        }.forEach(::println)

        System.err.println("Antall personer: ${goofedFnrs.size}")
        System.err.println("Antall personer med ekstra vedtaksperioder: $personerMedEkstraVedtaksperioder")
    }
}

fun vedtaksperiodeIder(data: String): List<String> {
    val json = objectMapper.readValue(data, JsonNode::class.java)
    return json["arbeidsgivere"].flatMap { arbeidsgivere ->
        arbeidsgivere["vedtaksperioder"]
    }.map { it["id"].asText() }
}

fun harUtbetalingerEtterGoof(data: String, okVedtaksperioder: List<String>): Boolean {
    val json = objectMapper.readValue(data, JsonNode::class.java)
    val vedtaksperioder = json["arbeidsgivere"]
        .flatMap { arbeidsgivere ->
            arbeidsgivere["vedtaksperioder"]
        }
        .filter {
            it["id"].asText() !in okVedtaksperioder
        }

    if (vedtaksperioder.isNotEmpty()) {
        personerMedEkstraVedtaksperioder ++
        System.err.println("Fant person med ny vedtaksperiode etter tom tidslinje")
        return true
    }
    return false
}

data class IngenTidligereData(
    val fnr: String
)

data class Goofed(
    val data: String
)

data class NonGoofed(
    val vedtaksperioder: List<String>,
    val fnr: String,
    val id: Long,
    val opprettet: LocalDate
)

fun <T> ResultSet.map(mapper: (rs: ResultSet) -> T): List<T> = use {
    mutableListOf<T>().apply {
        while (next()) {
            add(mapper(this@map))
        }
    }
}