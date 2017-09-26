import sbt._
import Keys._
import com.lightbend.paradox.sbt.ParadoxPlugin
import com.lightbend.paradox.sbt.ParadoxPlugin.autoImport._
import ProjectImplicits._

val AKKA_VERSION      = "2.5.4"
val SCALATEST_VERSION = "3.0.1"
val MIMA_PREVIOUS_VERSIONS = Seq("0.9.1")

lazy val testAll = TaskKey[Unit]("test-all")

lazy val GeneralSettings = Seq[Setting[_]](
  compile := (compile in Compile).dependsOn(compile in Test).dependsOn(compile in IntegrationTest).value,
  testAll := (test in Test).dependsOn(test in IntegrationTest).value,
  organization := "com.tumblr",
  scalaVersion := "2.12.2",
  crossScalaVersions := Seq("2.11.8", "2.12.2"),
  apiURL := Some(url("https://www.javadoc.io/doc/")),
  parallelExecution in Test := false,
  scalacOptions := scalaVersion.map { v: String =>
    val default = List(
      "-feature",
      "-language:implicitConversions",
      "-language:postfixOps",
      "-unchecked",
      "-deprecation",
      "-target:jvm-1.8"
    )
    if (v.startsWith("2.10.")) {
      default
    } else {
      "-Ywarn-unused-import" :: default
    }
  }.value,
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
  scalacOptions in (Compile, console) := Seq(),
  libraryDependencies ++= Seq(
    "com.typesafe.akka"      %% "akka-actor"                  % AKKA_VERSION,
    "com.typesafe.akka"      %% "akka-testkit"                % AKKA_VERSION,
    "org.scalatest"          %% "scalatest"                   % SCALATEST_VERSION % "test, it",
    "org.scalamock"          %% "scalamock-scalatest-support" % "3.6.0" % "test",
    "org.mockito"            % "mockito-all"                  % "1.9.5" % "test",
    "com.github.nscala-time" %% "nscala-time"                 % "2.16.0",
    "org.slf4j"              % "slf4j-api"                    % "1.7.6"
  ),
  coverageExcludedPackages := "colossus\\.examples\\..*;.*\\.testkit\\.*",
  credentials += Credentials("Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    sys.env.getOrElse("SONATYPE_USERNAME", ""),
    sys.env.getOrElse("SONATYPE_PASSWORD", ""))
) ++ Defaults.itSettings

lazy val publishSettings: Seq[Setting[_]] = Seq(
  publishMavenStyle := true,
  publishTo := Some(
    if (isSnapshot.value) {
      Opts.resolver.sonatypeSnapshots
    } else {
      Opts.resolver.sonatypeStaging
    }
  ),
  publishArtifact in Test := false,
  pomIncludeRepository := Function.const(false),
  useGpg := false,
  pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray),
  pgpSecretRing := file("secring.gpg"),
  pgpPublicRing := file("pubring.gpg"),
  licenses := Seq("Apache-2.0" -> new URL("http://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://github.com/tumblr/colossus")),
  scmInfo := Some(ScmInfo(url("https://github.com/tumblr/colossus"), "scm:git:git@github.com/tumblr/colossus.git")),
  developers := List(
    Developer(id = "danSimon", name = "", email = "", url = url("http://macrodan.tumblr.com"))
  )
)

lazy val ColossusSettings = GeneralSettings ++ publishSettings

lazy val noPubSettings = GeneralSettings ++ Seq(
  publishArtifact := false,
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
)

lazy val testkitDependencies = libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % SCALATEST_VERSION
)

lazy val MetricSettings = ColossusSettings

lazy val ExamplesSettings = Seq(
  libraryDependencies ++= Seq(
    "org.json4s"     %% "json4s-jackson" % "3.5.3",
    "ch.qos.logback" % "logback-classic" % "1.2.2"
  )
)

lazy val RootProject = Project(id = "root", base = file("."))
  .settings(noPubSettings: _*)
  .configs(IntegrationTest)
  .dependsOn(ColossusProject)
  .aggregate(ColossusProject, ColossusTestkitProject, ColossusMetricsProject, ColossusExamplesProject, ColossusDocs)

lazy val ColossusProject: Project = Project(id = "colossus", base = file("colossus"))
  .settings(ColossusSettings: _*)
  .withMima(MIMA_PREVIOUS_VERSIONS : _*)
  .configs(IntegrationTest)
  .aggregate(ColossusTestsProject)
  .dependsOn(ColossusMetricsProject)

lazy val ColossusExamplesProject = Project(id = "colossus-examples", base = file("colossus-examples"))
  .settings(noPubSettings: _*)
  .configs(IntegrationTest)
  .settings(ExamplesSettings: _*)
  .dependsOn(ColossusProject)

lazy val ColossusMetricsProject = Project(id = "colossus-metrics", base = file("colossus-metrics"))
  .settings(MetricSettings: _*)
  .withMima(MIMA_PREVIOUS_VERSIONS : _*)
  .configs(IntegrationTest)

lazy val ColossusTestkitProject = Project(id = "colossus-testkit", base = file("colossus-testkit"))
  .settings(ColossusSettings: _*)
  .settings(testkitDependencies)
  .withMima(MIMA_PREVIOUS_VERSIONS : _*)
  .configs(IntegrationTest)
  .dependsOn(ColossusProject)

lazy val ColossusDocs = Project(id = "colossus-docs", base = file("colossus-docs"))
  .settings(ColossusSettings: _*)
  .settings(ExamplesSettings: _*)
  .settings(noPubSettings: _*)
  .enablePlugins(ParadoxPlugin)
  .settings(
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    paradoxProperties ++= Map(
      "extref.docs.base_url" -> s"https://static.javadoc.io/${organization.value}/colossus_2.11/${version.value}/index.html#%s",
      "extref.docs-metrics.base_url" -> s"https://static.javadoc.io/${organization.value}/colossus-metrics_2.11/${version.value}/index.html#%s",
      "extref.docs-testkit.base_url" -> s"https://static.javadoc.io/${organization.value}/colossus-testkit_2.11/${version.value}/index.html#%s",
      "snip.examples.base_dir" -> "../scala" 
    )
  )
  .configs(IntegrationTest)
  .dependsOn(ColossusProject, ColossusTestkitProject)

lazy val ColossusTestsProject = Project(
  id = "colossus-tests",
  base = file("colossus-tests"),
  dependencies = Seq(ColossusTestkitProject % "compile;test->test")
).settings(noPubSettings: _*).configs(IntegrationTest)
