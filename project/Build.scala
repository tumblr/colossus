import sbt._
import Keys._
import spray.revolver.RevolverPlugin._
import com.typesafe.sbt.pgp.PgpKeys._
import scoverage.ScoverageSbtPlugin.ScoverageKeys._

object ColossusBuild extends Build {

  val AKKA_VERSION            = "2.3.9"
  val SCALATEST_VERSION       = "2.2.0"

  lazy val testAll = TaskKey[Unit]("test-all")

  val GeneralSettings = Seq[Setting[_]](


    compile <<= (compile in Compile) dependsOn (compile in Test) dependsOn (compile in IntegrationTest),
    (testAll) <<= (test in Test) dependsOn (test in IntegrationTest),
    organization := "com.tumblr",
    scalaVersion  := "2.11.7",
    crossScalaVersions := Seq("2.10.6", "2.11.7"),
    version                   := "0.8.4-SNAPSHOT",
    parallelExecution in Test := false,
    scalacOptions <<= scalaVersion map { v: String =>
      val default = List(
        "-feature",
        "-language:implicitConversions",
        "-language:postfixOps",
        "-unchecked",
        "-deprecation"
      )
      if (v.startsWith("2.10.")) default else "-Ywarn-unused-import" :: default
    },
    scalacOptions in (Compile, console) := Seq(),
    libraryDependencies ++= Seq (
      "com.typesafe.akka" %% "akka-actor"   % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-agent"   % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION,
      "org.scalatest"     %% "scalatest" % SCALATEST_VERSION % "test, it",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test",
      "com.github.nscala-time" %% "nscala-time" % "1.2.0"
    ),
    coverageExcludedPackages := "colossus\\.examples\\..*;.*\\.testkit\\.*"
  ) ++ Defaults.itSettings

  val ColossusSettings = GeneralSettings ++ Publish.settings

  val noPubSettings = GeneralSettings ++ Seq(
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
    )

  val testkitDependencies = libraryDependencies ++= Seq(
    "org.scalatest"     %% "scalatest" % SCALATEST_VERSION
  )

  val MetricSettings = ColossusSettings

  val ExamplesSettings = Seq (
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-jackson" % "3.3.0"
    )
  )

  lazy val RootProject = Project(id="root", base=file("."))
      .settings(noPubSettings:_*)
      .configs(IntegrationTest)
      .dependsOn(ColossusProject)
      .aggregate(ColossusProject, ColossusTestkitProject, ColossusMetricsProject, ColossusExamplesProject)

  lazy val ColossusProject: Project = Project(id="colossus", base=file("colossus"))
      .settings(ColossusSettings:_*)
      .configs(IntegrationTest)
      .aggregate(ColossusTestsProject)
      .dependsOn(ColossusMetricsProject)

  lazy val ColossusExamplesProject = Project(id="colossus-examples", base=file("colossus-examples"))
      .settings(noPubSettings:_*)
      .settings(Revolver.settings:_*)
      .configs(IntegrationTest)
      .settings(ExamplesSettings:_*)
      .dependsOn(ColossusProject)

  lazy val ColossusMetricsProject = Project(id="colossus-metrics", base=file("colossus-metrics"))
      .settings(MetricSettings:_*)
      .settings(Revolver.settings:_*)
      .configs(IntegrationTest)

  lazy val ColossusTestkitProject = Project(id="colossus-testkit", base = file("colossus-testkit"))
      .settings(ColossusSettings:_*)
      .settings(testkitDependencies)
      .configs(IntegrationTest)
      .dependsOn(ColossusProject)

  lazy val ColossusTestsProject = Project(
    id="colossus-tests",
    base = file("colossus-tests"),
    dependencies = Seq(ColossusTestkitProject % "compile;test->test")
  ).settings(noPubSettings:_*).configs(IntegrationTest)



}
