import java.sql.DriverManager
import java.sql.ResultSet
import java.time.LocalDate

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

        val lastNonGoofedMessage = connection.prepareStatement(
            """
            SELECT fnr, id, opprettet FROM person 
            WHERE fnr=? 
                AND skjema_versjon < 6 
            ORDER BY ID DESC LIMIT 1;
            """
        )

        goofedFnrs.map { fnr ->
            lastNonGoofedMessage.setString(1, fnr)
            lastNonGoofedMessage.executeQuery()
                .map {
                    NonGoofed(
                        fnr = fnr,
                        id = it.getLong("id"),
                        opprettet = it.getDate("opprettet").toLocalDate()
                    )
                }
                .firstOrNull()
        }.forEach(::println)
    }
}


data class NonGoofed(
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