addSbtPlugin("org.scoverage"             % "sbt-scoverage"  % "2.0.5")
addSbtPlugin("com.github.sbt"            % "sbt-ci-release" % "1.5.10")
addSbtPlugin("ch.epfl.scala"             % "sbt-scalafix"   % "0.10.4")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"   % "0.4.2")
addSbtPlugin("org.scalameta"             % "sbt-mdoc"       % "2.3.6")
addSbtPlugin("com.47deg"                 % "sbt-microsites" % "1.3.4")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
