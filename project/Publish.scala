import sbt._
import sbt.Keys._

import java.net.URL

import scala.xml.NodeSeq

object Publish {

  private val nexus = "https://oss.sonatype.org/"
  private val snapshots : Option[Resolver] = Some("snapshots" at nexus + "content/repositories/snapshots")
  private val releases : Option[Resolver] = Some("releases" at nexus + "service/local/staging/deploy/maven2")

  lazy val settings = Seq(
    publishMavenStyle := true,
    
    publishTo := (if(isSnapshot.value) snapshots else releases),

    publishArtifact in Test := false,

    pomIncludeRepository := { _ => false },

    //credentials could also be just embedded in ~/.sbt/0.13/sonatype.sbt
    credentials += Credentials(Path.userHome / ".sonatype_credentials"),

    pomExtra := pomExtraGen,

    licenses := Seq("Apache License, Version 2.0"-> new URL("http://www.apache.org/licenses/LICENSE-2.0.html")),

    homepage :=  Some(url("https://github.com/tumblr/colossus"))
  )

  private def pomExtraGen = {
    <inceptionYear>2014</inceptionYear>
     <scm>
       <url>git@github.com/tumblr/colossus.git</url>
       <connection>scm:git:git@github.com/tumblr/colossus.git</connection>
     </scm> ++ pomDevelopersGen(Seq(("dsimon", "Dan Simon"), ("sauron", "Nick Sauro")))
  }

  private def pomDevelopersGen(developers : Seq[(String, String)]) : NodeSeq = {
    <developers>
      {
        developers.map{case (uid, name) => <developer><id>{uid}</id><name>{name}</name></developer>}
      }
    </developers>

  }

}
