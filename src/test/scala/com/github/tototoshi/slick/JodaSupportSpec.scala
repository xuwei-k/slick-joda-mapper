/*
 * Copyright 2013 Toshiyuki Takahashi
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.tototoshi.slick

import org.scalatest.BeforeAndAfterEach
import org.joda.time._

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import slick.jdbc.{ GetResult, H2Profile, JdbcProfile, MySQLProfile, PostgresProfile }
import java.util.{ Locale, TimeZone }

import com.dimafeng.testcontainers.{ Container, ForAllTestContainer, JdbcDatabaseContainer, MySQLContainer, PostgreSQLContainer }
import org.testcontainers.utility.DockerImageName
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

abstract class JodaSupportSpec extends AnyFunSpec
  with Matchers
  with BeforeAndAfterEach {

  val driver: JdbcProfile
  val jodaSupport: GenericJodaSupport
  def jdbcUrl: String
  def jdbcDriver: String
  def jdbcUser: String
  def jdbcPassword: String

  import driver.api._
  import jodaSupport._

  case class Jodas(
      dateTimeZone: DateTimeZone,
      localDate: LocalDate,
      dateTime: DateTime,
      instant: Instant,
      localDateTime: LocalDateTime,
      localTime: LocalTime,
      optDateTimeZone: Option[DateTimeZone],
      optLocalDate: Option[LocalDate],
      optDateTime: Option[DateTime],
      optInstant: Option[Instant],
      optLocalDateTime: Option[LocalDateTime],
      optLocalTime: Option[LocalTime])

  class JodaTest(tag: Tag) extends Table[Jodas](tag, "joda_test") {
    def dateTimeZone = column[DateTimeZone]("date_time_zone")
    def localDate = column[LocalDate]("local_date")
    def dateTime = column[DateTime]("date_time")
    def instant = column[Instant]("instant")
    def localDateTime = column[LocalDateTime]("local_date_time")
    def localTime = column[LocalTime]("local_time")
    def optDateTimeZone = column[Option[DateTimeZone]]("opt_date_time_zone")
    def optLocalDate = column[Option[LocalDate]]("opt_local_date")
    def optDateTime = column[Option[DateTime]]("opt_date_time")
    def optInstant = column[Option[Instant]]("opt_instant")
    def optLocalDateTime = column[Option[LocalDateTime]]("opt_local_date_time")
    def optLocalTime = column[Option[LocalTime]]("opt_local_time")
    def * = (dateTimeZone, localDate, dateTime, instant, localDateTime, localTime, optDateTimeZone, optLocalDate, optDateTime, optInstant, optLocalDateTime, optLocalTime) <> (Jodas.tupled, Jodas.unapply _)
  }

  lazy val db = Database.forURL(url = jdbcUrl, user = jdbcUser, password = jdbcPassword, driver = jdbcDriver)

  val jodaTest = TableQuery[JodaTest]

  private[this] val timeout = 10.seconds

  private[this] implicit class FutureOps[A](future: Future[A]) {
    def await(): A = Await.result(future, timeout)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    Locale.setDefault(Locale.JAPAN)
    val tz = TimeZone.getTimeZone("Asia/Tokyo")
    TimeZone.setDefault(tz)
    DateTimeZone.setDefault(DateTimeZone.forID(tz.getID))

    db.run(DBIO.seq(jodaTest.schema.create)).await()
  }

  override def afterEach(): Unit = {
    db.run(DBIO.seq(jodaTest.schema.drop)).await()
    super.afterEach()
  }

  val testData = List(
    Jodas(
      DateTimeZone.forID("Asia/Tokyo"),
      new LocalDate(2012, 12, 4),
      new DateTime(2012, 12, 4, 0, 0, 0, 0),
      new DateTime(2012, 12, 4, 0, 0, 0, 0).toInstant,
      new LocalDateTime(2012, 12, 4, 0, 0, 0, 0),
      new LocalTime(0),
      Some(DateTimeZone.forID("Asia/Tokyo")),
      Some(new LocalDate(2012, 12, 4)),
      Some(new DateTime(2012, 12, 4, 0, 0, 0, 0)),
      Some(new DateTime(2012, 12, 4, 0, 0, 0, 0).toInstant),
      Some(new LocalDateTime(2012, 12, 4, 0, 0, 0, 0)),
      Some(new LocalTime(0))
    ),
    Jodas(
      DateTimeZone.forID("Europe/London"),
      new LocalDate(2012, 12, 5),
      new DateTime(2012, 12, 5, 0, 0, 0, 0),
      new DateTime(2012, 12, 5, 0, 0, 0, 0).toInstant,
      new LocalDateTime(2012, 12, 5, 0, 0, 0, 0),
      new LocalTime(0),
      Some(DateTimeZone.forID("Europe/London")),
      Some(new LocalDate(2012, 12, 5)),
      None,
      None,
      None,
      Some(new LocalTime(0))
    ),
    Jodas(
      DateTimeZone.forID("America/New_York"),
      new LocalDate(2012, 12, 6),
      new DateTime(2012, 12, 6, 0, 0, 0, 0),
      new DateTime(2012, 12, 6, 0, 0, 0, 0).toInstant,
      new LocalDateTime(2012, 12, 6, 0, 0, 0, 0),
      new LocalTime(0),
      Some(DateTimeZone.forID("America/New_York")),
      Some(new LocalDate(2012, 12, 6)),
      None,
      None,
      None,
      Some(new LocalTime(0))
    )
  )

  def insertTestData(): DBIOAction[_, NoStream, Effect.Write] = {
    jodaTest ++= testData
  }

  describe("JodaSupport") {
    it("should enable us to use joda-time with slick") {
      db.run(insertTestData()).await()
      db.run(jodaTest.result).await() should equal(testData)
    }
  }

}

class H2JodaSupportSpec extends JodaSupportSpec {
  override val driver = H2Profile
  override val jodaSupport = H2JodaSupport
  override def jdbcUrl = "jdbc:h2:mem:testh2;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"
  override def jdbcDriver = "org.h2.Driver"
  override def jdbcUser = "sa"
  override def jdbcPassword = null
}

abstract class TestContainerSpec extends JodaSupportSpec with ForAllTestContainer {
  override def container: JdbcDatabaseContainer with Container
  override def jdbcUrl = container.jdbcUrl
  override def jdbcUser = container.username
  override def jdbcPassword = container.password
}

class MySQLJodaSupportSpec extends TestContainerSpec {
  // TODO update 5.7 or later
  // `.schema.create` does not work due to timestamp default value issue
  override val container = MySQLContainer(mysqlImageVersion = DockerImageName.parse("mysql:5.6.50"))
  override def jdbcDriver = "com.mysql.cj.jdbc.Driver"
  override val driver = MySQLProfile
  override val jodaSupport = MySQLJodaSupport
}

class PostgresJodaSupportSpec extends TestContainerSpec {
  override val container = PostgreSQLContainer()
  override def jdbcDriver = "org.postgresql.Driver"
  override val driver = PostgresProfile
  override val jodaSupport = PostgresJodaSupport
}
