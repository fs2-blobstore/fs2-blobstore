addSbtPlugin("org.scoverage"             % "sbt-scoverage"  % "2.0.7")
addSbtPlugin("com.github.sbt"            % "sbt-ci-release" % "1.5.12")
addSbtPlugin("ch.epfl.scala"             % "sbt-scalafix"   % "0.11.0")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat"   % "0.4.2")
addSbtPlugin("org.scalameta"             % "sbt-mdoc"       % "2.3.7")
addSbtPlugin("com.47deg"                 % "sbt-microsites" % "1.4.3")
addSbtPlugin("com.github.sbt"            % "sbt-git"        % "2.0.1")

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
