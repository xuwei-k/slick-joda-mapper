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
import java.util.TimeZone

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
      dateTimeWithZone: DateTime,
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
    def dateTimeWithZone = column[DateTime]("date_time_with_zone")
    def instant = column[Instant]("instant")
    def localDateTime = column[LocalDateTime]("local_date_time")
    def localTime = column[LocalTime]("local_time")
    def optDateTimeZone = column[Option[DateTimeZone]]("opt_date_time_zone")
    def optLocalDate = column[Option[LocalDate]]("opt_local_date")
    def optDateTime = column[Option[DateTime]]("opt_date_time")
    def optInstant = column[Option[Instant]]("opt_instant")
    def optLocalDateTime = column[Option[LocalDateTime]]("opt_local_date_time")
    def optLocalTime = column[Option[LocalTime]]("opt_local_time")
    def * = (dateTimeZone, localDate, dateTime, dateTimeWithZone, instant, localDateTime, localTime, optDateTimeZone, optLocalDate, optDateTime, optInstant, optLocalDateTime, optLocalTime) <> (Jodas.tupled, Jodas.unapply _)
  }

  lazy val db = Database.forURL(url = jdbcUrl, user = jdbcUser, password = jdbcPassword, driver = jdbcDriver)

  val jodaTest = TableQuery[JodaTest]

  private[this] val timeout = 10.seconds

  private[this] implicit class FutureOps[A](future: Future[A]) {
    def await(): A = Await.result(future, timeout)
  }

  def withTimezone[A](zone: DateTimeZone)(f: => A): A = {
    val tz = TimeZone.getTimeZone(zone.getID)
    TimeZone.setDefault(tz)
    DateTimeZone.setDefault(DateTimeZone.forID(tz.getID))
    try {
      db.run(DBIO.seq(jodaTest.schema.create)).await()
      f
    } finally {
      db.run(DBIO.seq(jodaTest.schema.drop)).await()
    }
  }

  val Tokyo = DateTimeZone.forID("Asia/Tokyo")
  val London = DateTimeZone.forID("Europe/London")
  val NewYork = DateTimeZone.forID("America/New_York")

  val timezones = List(Tokyo, London, NewYork)

  def test[A](f: => A): Unit = {
    timezones.foreach(zone => withTimezone(zone) {
      db.run(insertTestData()).await()
      f
    })
  }

  def insertTestData(): DBIOAction[Unit, NoStream, Effect.Write] = {
    DBIO.seq(
      jodaTest += Jodas(
        Tokyo,
        new LocalDate(2012, 12, 4),
        new DateTime(2012, 12, 4, 0, 0, 0, 0),
        new DateTime(2012, 12, 4, 0, 0, 0, 0).withZone(Tokyo),
        new DateTime(2012, 12, 4, 0, 0, 0, 0).toInstant,
        new LocalDateTime(2012, 12, 4, 0, 0, 0, 0),
        new LocalTime(0),
        Some(Tokyo),
        Some(new LocalDate(2012, 12, 4)),
        Some(new DateTime(2012, 12, 4, 0, 0, 0, 0)),
        Some(new DateTime(2012, 12, 4, 0, 0, 0, 0).toInstant),
        Some(new LocalDateTime(2012, 12, 4, 0, 0, 0, 0)),
        Some(new LocalTime(0))
      ),
      jodaTest +=
        Jodas(
          London,
          new LocalDate(2012, 12, 5),
          new DateTime(2012, 12, 5, 0, 0, 0, 0),
          new DateTime(2012, 12, 5, 0, 0, 0, 0).withZone(London),
          new DateTime(2012, 12, 5, 0, 0, 0, 0).toInstant,
          new LocalDateTime(2012, 12, 5, 0, 0, 0, 0),
          new LocalTime(0),
          Some(London),
          Some(new LocalDate(2012, 12, 5)),
          None,
          None,
          None,
          Some(new LocalTime(0))
        ),
      jodaTest +=
        Jodas(
          NewYork,
          new LocalDate(2012, 12, 6),
          new DateTime(2012, 12, 6, 0, 0, 0, 0),
          new DateTime(2012, 12, 6, 0, 0, 0, 0).withZone(NewYork),
          new DateTime(2012, 12, 6, 0, 0, 0, 0).toInstant,
          new LocalDateTime(2012, 12, 6, 0, 0, 0, 0),
          new LocalTime(0),
          Some(NewYork),
          Some(new LocalDate(2012, 12, 6)),
          None,
          None,
          None,
          Some(new LocalTime(0))
        )
    )
  }

  describe("JodaSupport") {

    it("should enable us to use joda-time with slick") {
      test {
        db.run(jodaTest.result).await() should have size 3
      }
    }

    it("date_time_with_zone") {
      test {
        db.run(jodaTest.result).await().map(_.dateTimeWithZone).map(_.getMillis).sorted should equal(List(
          new DateTime(2012, 12, 4, 0, 0, 0, 0).withZone(Tokyo),
          new DateTime(2012, 12, 5, 0, 0, 0, 0).withZone(London),
          new DateTime(2012, 12, 6, 0, 0, 0, 0).withZone(NewYork),
        ).map(_.getMillis).sorted)
      }
    }

    it("should enable us to use joda-time with string interpolation API") {
      test {
        db.run(sql"SELECT opt_date_time_zone FROM joda_test WHERE date_time_zone = 'Asia/Tokyo'"
          .as[Option[DateTimeZone]].head).await() should be(Some(DateTimeZone.forID("Asia/Tokyo")))
        db.run(sql"SELECT opt_local_date FROM joda_test WHERE local_date = ${new LocalDate(2012, 12, 4)}"
          .as[Option[LocalDate]].head).await() should be(Some(new LocalDate(2012, 12, 4)))
        db.run(sql"SELECT opt_date_time FROM joda_test WHERE date_time = ${new DateTime(2012, 12, 4, 0, 0, 0, 0)}"
          .as[Option[DateTime]].head).await() should be(Some(new DateTime(2012, 12, 4, 0, 0, 0, 0)))
        db.run(sql"SELECT opt_instant FROM joda_test WHERE instant = ${new DateTime(2012, 12, 4, 0, 0, 0, 0)}"
          .as[Option[Instant]].head).await() should be(Some(new DateTime(2012, 12, 4, 0, 0, 0, 0).toInstant))
        db.run(sql"SELECT opt_local_date_time FROM joda_test WHERE local_date_time = ${new LocalDateTime(2012, 12, 4, 0, 0, 0, 0)}"
          .as[Option[LocalDateTime]].head).await() should be(Some(new LocalDateTime(2012, 12, 4, 0, 0, 0, 0)))
        db.run(sql"SELECT opt_local_time FROM joda_test WHERE local_time = ${new LocalTime(0)}"
          .as[Option[LocalTime]].head).await() should be(Some(new LocalTime(0)))
        db.run(sql"SELECT local_date FROM joda_test WHERE opt_local_date = ${Some(new LocalDate(2012, 12, 5))}"
          .as[LocalDate].head).await() should be(new LocalDate(2012, 12, 5))
        db.run(sql"SELECT date_time FROM joda_test WHERE opt_date_time = ${Some(new DateTime(2012, 12, 4, 0, 0, 0, 0))}"
          .as[DateTime].head).await() should be(new DateTime(2012, 12, 4, 0, 0, 0, 0))
        db.run(sql"SELECT instant FROM joda_test WHERE opt_instant = ${Some(new DateTime(2012, 12, 4, 0, 0, 0, 0).toInstant)}"
          .as[Instant].head).await() should be(new DateTime(2012, 12, 4, 0, 0, 0, 0).toInstant)
        db.run(sql"SELECT local_date_time FROM joda_test WHERE opt_local_date_time = ${Some(new LocalDateTime(2012, 12, 4, 0, 0, 0, 0))}"
          .as[LocalDateTime].head).await() should be(new LocalDateTime(2012, 12, 4, 0, 0, 0, 0))
        db.run(sql"SELECT local_time FROM joda_test WHERE opt_local_time = ${Some(new LocalTime(0))}"
          .as[LocalTime].head).await() should be(new LocalTime(0))

        implicit val getResult: GetResult[(DateTimeZone, LocalDate, DateTime, DateTime, Instant, LocalDateTime, LocalTime)] = GetResult(r => (r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
        implicit val getResult2: GetResult[Jodas] = GetResult(r => Jodas(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

        db.run(sql"SELECT date_time_zone, local_date, date_time, date_time_with_zone, instant, local_date_time, local_time FROM joda_test".as[(DateTimeZone, LocalDate, DateTime, DateTime, Instant, LocalDateTime, LocalTime)]).await() should have size 3
        db.run(sql"SELECT date_time_zone, local_date, date_time, date_time_with_zone, instant, local_date_time, local_time, opt_date_time_zone, opt_local_date, opt_date_time, opt_instant, opt_local_date_time, opt_local_time FROM joda_test".as[Jodas]).await() should have size 3
      }
    }
  }

  it("can be used with comparative operators") {
    test {
      val q1 = jodaTest.filter(_.localDate > new LocalDate(2012, 12, 5))
      db.run(q1.result).await() should have size 1
    }
  }

  it("should be able to filter with the specified date") {
    test {
      val q1 = for {
        jt <- jodaTest
        if jt.localDate === new LocalDate(2012, 12, 5)
      } yield jt

      val res1 = db.run(q1.result).await()
      res1 should have size 1
      res1.headOption.map(_.localDate) should be(Some(new LocalDate(2012, 12, 5)))

      val q2 = for {
        jt <- jodaTest
        if jt.localDate =!= new LocalDate(2012, 12, 5)
      } yield jt
      val res2 = db.run(q2.result).await()
      res2 should have size 2
      res2.lift(1).map(_.localDate) should not be Some(new LocalDate(2012, 12, 5))
      res2.lift(2).map(_.localDate) should not be Some(new LocalDate(2012, 12, 5))
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
  override val container = MySQLContainer(
    mysqlImageVersion = DockerImageName.parse("mysql:5.6.50"),
    configurationOverride = "somepath"
  )
  override def jdbcDriver = "com.mysql.jdbc.Driver"
  override val driver = MySQLProfile
  override val jodaSupport = MySQLJodaSupport
}

class PostgresJodaSupportSpec extends TestContainerSpec {
  override val container = PostgreSQLContainer()
  override def jdbcDriver = "org.postgresql.Driver"
  override val driver = PostgresProfile
  override val jodaSupport = PostgresJodaSupport
}
