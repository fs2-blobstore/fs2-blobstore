addSbtPlugin("org.scoverage"  % "sbt-scoverage"  % "2.4.2")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.11.2")
addSbtPlugin("ch.epfl.scala"  % "sbt-scalafix"   % "0.14.4")
addSbtPlugin("org.typelevel"  % "sbt-tpolecat"   % "0.5.2")
addSbtPlugin("org.scalameta"  % "sbt-mdoc"       % "2.7.2")
addSbtPlugin("com.47deg"      % "sbt-microsites" % "1.4.4")
addSbtPlugin("com.github.sbt" % "sbt-git"        % "2.1.0")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.4.0"
