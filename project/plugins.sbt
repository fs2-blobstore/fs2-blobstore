addSbtPlugin("org.scoverage"  % "sbt-scoverage"  % "2.2.2")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.9.2")
addSbtPlugin("ch.epfl.scala"  % "sbt-scalafix"   % "0.13.0")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"   % "0.5.2")
addSbtPlugin("org.scalameta"  % "sbt-mdoc"       % "2.6.2")
addSbtPlugin("com.47deg"      % "sbt-microsites" % "1.4.4")
addSbtPlugin("com.github.sbt" % "sbt-git"        % "2.1.0")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.3.0"
