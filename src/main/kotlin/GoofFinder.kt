import java.sql.DriverManager
import java.sql.ResultSet

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

        val fnr = statement.executeQuery().map { it.getString("fnr") }
        fnr.forEach(::println)
    }
}

fun <T> ResultSet.map(mapper: (rs: ResultSet) -> T): List<T> = mutableListOf<T>().apply {
    while (next()) {
        add(mapper(this@map))
    }
}