addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.9.0")

addSbtPlugin("io.spray" % "sbt-revolver" % "0.8.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.2.13")

addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.10")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.18")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
