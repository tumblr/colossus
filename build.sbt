import sbt._
import Keys._
import com.lightbend.paradox.sbt.ParadoxPlugin
import com.lightbend.paradox.sbt.ParadoxPlugin.autoImport._
import ProjectImplicits._

val AkkaVersion      = "2.5.23"
val ScalatestVersion = "3.0.8"

val MIMAPreviousVersions = Seq("0.11.1-RC1")

lazy val testAll = TaskKey[Unit]("test-all")

lazy val GeneralSettings = Seq[Setting[_]](
  compile := (compile in Compile).dependsOn(compile in Test).dependsOn(compile in IntegrationTest).value,
  testAll := (test in Test).dependsOn(test in IntegrationTest).value,
  organization := "com.tumblr",
  scalaVersion := "2.12.8",
  crossScalaVersions := Seq("2.11.12", "2.12.8"),
  apiURL := Some(url("https://www.javadoc.io/doc/")),
  parallelExecution in Test := false,
  scalacOptions := Seq(
    "-feature",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-unchecked",
    "-deprecation",
    "-target:jvm-1.8",
    "-Ywarn-unused-import"
  ),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
  scalacOptions in (Compile, console) := Seq(),
  libraryDependencies ++= Seq(
    "com.typesafe.akka"      %% "akka-actor"                  % AkkaVersion,
    "com.typesafe.akka"      %% "akka-testkit"                % AkkaVersion % "test",
    "org.scalatest"          %% "scalatest"                   % ScalatestVersion % "test, it",
    "org.scalamock"          %% "scalamock-scalatest-support" % "3.6.0" % "test",
    "org.mockito"            %  "mockito-all"                 % "1.10.19" % "test",
    "org.slf4j"              %  "slf4j-api"                   % "1.7.26"
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
  "org.scalatest"     %% "scalatest"    % ScalatestVersion,
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion
)

lazy val ExamplesSettings = Seq(
  libraryDependencies ++= Seq(
    "org.json4s"                   %% "json4s-jackson"       % "3.6.6",
    "ch.qos.logback"               %  "logback-classic"      % "1.2.3",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
    "org.mockito"                  %  "mockito-all"          % "1.10.19"
  )
)

lazy val RootProject = Project(id = "colossus-root", base = file("."))
  .settings(noPubSettings: _*)
  .configs(IntegrationTest)
  .dependsOn(ColossusProject)
  .aggregate(ColossusProject, ColossusTestsProject, ColossusTestkitProject, ColossusMetricsProject, ColossusExamplesProject, ColossusDocs)

lazy val ColossusProject: Project = Project(id = "colossus", base = file("colossus"))
  .settings(ColossusSettings: _*)
  .withMima(MIMAPreviousVersions : _*)
  .configs(IntegrationTest)
  .dependsOn(ColossusMetricsProject)

lazy val ColossusExamplesProject = Project(id = "colossus-examples", base = file("colossus-examples"))
  .settings(noPubSettings: _*)
  .configs(IntegrationTest)
  .settings(ExamplesSettings: _*)
  .dependsOn(ColossusProject)

lazy val ColossusMetricsProject = Project(id = "colossus-metrics", base = file("colossus-metrics"))
  .settings(ColossusSettings: _*)
  .withMima(MIMAPreviousVersions : _*)
  .configs(IntegrationTest)

lazy val ColossusTestkitProject = Project(id = "colossus-testkit", base = file("colossus-testkit"))
  .settings(ColossusSettings: _*)
  .settings(testkitDependencies)
  .withMima(MIMAPreviousVersions : _*)
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
      "github.base_url" -> s"https://github.com/tumblr/colossus/blob/${version.value}",
      "extref.docs.base_url" -> s"https://static.javadoc.io/${organization.value}/colossus_2.11/${version.value}/index.html#%s",
      "extref.docs-metrics.base_url" -> s"https://static.javadoc.io/${organization.value}/colossus-metrics_2.11/${version.value}/index.html#%s",
      "extref.docs-testkit.base_url" -> s"https://static.javadoc.io/${organization.value}/colossus-testkit_2.11/${version.value}/index.html#%s",
      "snip.examples.base_dir" -> "../scala" 
    )
  )
  .configs(IntegrationTest)
  .dependsOn(ColossusProject, ColossusTestkitProject)

lazy val ColossusTestsProject = Project(id = "colossus-tests", base = file("colossus-tests"))
  .dependsOn(ColossusTestkitProject % "compile;test->test")
  .settings(noPubSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" %  "logback-classic" % "1.2.3"
    )
  ).configs(IntegrationTest)

