import de.heikoseeberger.sbtheader.license._

name := "pool"

organization := "io.github.davidgregory084"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "0.9.0",
  "io.monix" %% "monix" % "2.3.0",
  "io.monix" %% "monix-cats" % "2.3.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

headers := Map("scala" -> Apache2_0("2017", "David Gregory"))

createHeaders.in(Compile) := { createHeaders.in(Compile).triggeredBy(compile.in(Compile)).value }
