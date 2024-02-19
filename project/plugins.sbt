addSbtPlugin("org.scoverage"  % "sbt-scoverage"  % "2.0.10")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.12")
addSbtPlugin("ch.epfl.scala"  % "sbt-scalafix"   % "0.11.1")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"   % "0.5.0")
addSbtPlugin("org.scalameta"  % "sbt-mdoc"       % "2.5.2")
addSbtPlugin("com.47deg"      % "sbt-microsites" % "1.4.4")
addSbtPlugin("com.github.sbt" % "sbt-git"        % "2.0.1")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
