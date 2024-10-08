addSbtPlugin("org.scoverage"  % "sbt-scoverage"  % "2.2.0")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.7.0")
addSbtPlugin("ch.epfl.scala"  % "sbt-scalafix"   % "0.12.1")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"   % "0.5.2")
addSbtPlugin("org.scalameta"  % "sbt-mdoc"       % "2.6.1")
addSbtPlugin("com.47deg"      % "sbt-microsites" % "1.4.4")
addSbtPlugin("com.github.sbt" % "sbt-git"        % "2.0.1")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.3.0"
