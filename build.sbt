name := "pool"

organization := "io.github.davidgregory084"

version := "0.1.0-SNAPSHOT"

releaseCrossBuild := true

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "0.9.0",
  "io.monix" %% "monix" % "2.3.0",
  "io.monix" %% "monix-cats" % "2.3.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
)

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

headers := {
  import de.heikoseeberger.sbtheader.license._
  Map("scala" -> Apache2_0("2017", "David Gregory"))
}

createHeaders.in(Compile) := {
  createHeaders.in(Compile).triggeredBy(compile.in(Compile)).value
}

coursierVerbosity := {
  val travisBuild = isTravisBuild.in(Global).value

  if (travisBuild)
    0
  else
    coursierVerbosity.value
}
