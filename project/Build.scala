import sbt._
import Keys._
import spray.revolver.RevolverPlugin._

object ColossusBuild extends Build {

  val AKKA_VERSION            = "2.3.6"
  val SCALATEST_VERSION       = "2.2.0"

  val GeneralSettings = Seq[Setting[_]](

    publishMavenStyle := true,

    compile <<= (compile in Compile) dependsOn (compile in Test),
    
    publishTo <<= (version) {version: String =>
      // we want to publish to tumblr/snapshots for snapshot releases, tumblr/releases otherwise
      val basePublishUrl = "http://repo.ewr01.tumblr.net:8081/nexus/content/repositories/"
      val snapshotDeployRepo = "snapshots"
      val releaseDeployRepo = "releases"
      if(version.trim.endsWith("SNAPSHOT"))
        Some("Tumblr Publish Snapshot" at (basePublishUrl + snapshotDeployRepo))
      else
        Some("Tumblr Publish Release" at (basePublishUrl + releaseDeployRepo))
    },
    
    credentials += Credentials(Path.userHome / ".nexus_credentials"),
    
    organization := "com.tumblr",
    scalaVersion  := "2.11.2",
    crossScalaVersions := Seq("2.10.4", "2.11.2"),
    version                   := "0.5.0-RC5-SNAPSHOT",
    parallelExecution in Test := false,
    scalacOptions             ++= Seq("-feature", "-language:implicitConversions", "-language:postfixOps", "-unchecked", "-deprecation"),


    externalResolvers ++= Seq (
      "Tumblr Nexus Repo"   at "http://repo.ewr01.tumblr.net:8081/nexus/content/groups/public",
      "akka snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
    ),

    libraryDependencies ++= Seq (
      "com.typesafe.akka" %% "akka-actor"   % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-agent"   % AKKA_VERSION,
      "com.typesafe.akka" %% "akka-testkit" % AKKA_VERSION,
      "org.scalatest"     %% "scalatest" % SCALATEST_VERSION % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test",
      "com.github.nscala-time" %% "nscala-time" % "1.2.0"
    )

  )

  val ColossusSettings = GeneralSettings

  val testkitDependencies = libraryDependencies ++= Seq(
    "org.scalatest"     %% "scalatest" % SCALATEST_VERSION
  )

  val MetricSettings = ColossusSettings ++ Seq(
    libraryDependencies ++= Seq(
      "net.liftweb"       %% "lift-json"     % "2.6-RC1")
  )

  lazy val RootProject = Project(id="root", base=file("."))
      .settings(GeneralSettings:_*)
      .dependsOn(ColossusProject)
      .aggregate(ColossusProject, ColossusTestkitProject, ColossusMetricsProject)

  lazy val ColossusProject: Project = Project(id="colossus", base=file("colossus"))
      .settings(ColossusSettings:_*)
      .aggregate(ColossusTestsProject)
      .dependsOn(ColossusMetricsProject)

  lazy val ColossusExamplesProject = Project(id="colossus-examples", base=file("colossus-examples"))
      .settings(GeneralSettings:_*)
      .settings(Revolver.settings:_*)
      .dependsOn(ColossusProject)

  lazy val ColossusMetricsProject = Project(id="colossus-metrics", base=file("colossus-metrics"))
      .settings(MetricSettings:_*)
      .settings(Revolver.settings:_*)

  lazy val ColossusTestkitProject = Project(id="colossus-testkit", base = file("colossus-testkit"))
      .settings(GeneralSettings:_*)
      .settings(testkitDependencies)
      .dependsOn(ColossusProject)

  lazy val ColossusTestsProject = Project(
    id="colossus-tests", 
    base = file("colossus-tests"),
    dependencies = Seq(ColossusTestkitProject % "compile;test->test")
  ).settings(GeneralSettings:_*)



}
