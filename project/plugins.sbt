addSbtPlugin("org.scoverage"             % "sbt-scoverage"  % "2.0.6")
addSbtPlugin("com.github.sbt"            % "sbt-ci-release" % "1.5.11")
addSbtPlugin("ch.epfl.scala"             % "sbt-scalafix"   % "0.10.4")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"   % "0.4.1")
addSbtPlugin("org.scalameta"             % "sbt-mdoc"       % "2.3.6")
addSbtPlugin("com.47deg"                 % "sbt-microsites" % "1.4.1")
addSbtPlugin("com.github.sbt"            % "sbt-git"        % "2.0.1")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
