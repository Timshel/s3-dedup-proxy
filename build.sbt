val scala3Version = "3.8.3"
val Http4sVersion = "0.23.34"

enablePlugins(JavaAppPackaging)

lazy val root = project
  .in(file("."))
  .settings(
    name         := "s3-dedup-proxy",
    version      := "0.0.3",
    scalaVersion := scala3Version,
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:higherKinds",
      // "-language:strictEquality",
      // "-explain",
    ),
    javaOptions += "-Duser.timezone=UTC",
    Compile / mainClass := Some("timshel.s3dedupproxy.Application"),
    libraryDependencies ++= Seq(
      // Java
      "io.minio"                         % "minio"                      % "8.6.0",
      "org.apache.jclouds"               % "jclouds-allblobstore"       % "2.7.0",
      "org.apache.jclouds.api"           % "filesystem"                 % "2.7.0",
      "org.apache.jclouds.driver"        % "jclouds-slf4j"              % "2.7.0",
      "org.flywaydb"                     % "flyway-database-postgresql" % "12.3.0",
      "org.gaul"                         % "s3proxy"                    % "3.1.0",
      "org.postgresql"                   % "postgresql"                 % "42.7.10",
      "org.quartz-scheduler"             % "quartz"                     % "2.5.2",
      "org.slf4j"                        % "slf4j-api"                  % "2.0.17",
      "org.slf4j"                        % "slf4j-simple"               % "2.0.17",

      // Leftovers
      "com.squareup.okhttp3"             % "okhttp"                     % "5.3.2",
      "com.squareup.okhttp3"             % "okhttp-brotli"              % "5.3.2",

      // Scala
      "com.github.pureconfig"           %% "pureconfig-generic-scala3"  % "0.17.10",
      "com.typesafe.scala-logging"      %% "scala-logging"              % "3.9.5",
      "org.scalikejdbc"                 %% "scalikejdbc"                % "4.3.5",
      "org.tpolecat"                    %% "skunk-core"                 % "0.6.5",
      "org.typelevel"                   %% "cats-effect"                % "3.7.0",
      "org.http4s"                      %% "http4s-ember-server"        % Http4sVersion,
      "org.http4s"                      %% "http4s-ember-client"        % Http4sVersion,
      "org.http4s"                      %% "http4s-dsl"                 % Http4sVersion,
      "org.http4s"                      %% "http4s-jawn"                % Http4sVersion,

      "org.scalameta"                   %% "munit"                      % "1.3.0" % Test,
      "org.typelevel"                   %% "munit-cats-effect"          % "2.2.0" % Test,
    ),
    // wartremoverWarnings ++= Warts.all,
  )
