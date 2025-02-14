val scala3Version = "3.5.2"

enablePlugins(JavaAppPackaging)

lazy val root = project
  .in(file("."))
  .settings(
    name         := "s3-dedup-proxy",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:higherKinds"
    ),
    javaOptions += "-Duser.timezone=UTC",
    Compile / mainClass := Some("timshel.s3dedupproxy.Application"),
    unmanagedSources / excludeFilter := HiddenFileFilter
      || "EncryptedBlobStore.java"
      || "S3ProxyExtension.java"
      || "S3ProxyRule.java"
      || "Main.java",
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml"     % "2.15.2",
      "com.fasterxml.woodstox"           % "woodstox-core"              % "6.5.1",
      "com.google.code.findbugs"         % "findbugs-annotations"       % "3.0.1",
      "com.google.code.findbugs"         % "jsr305"                     % "3.0.2",
      "com.squareup.okhttp3"             % "okhttp"                     % "4.11.0",
      "com.squareup.okhttp3"             % "okhttp-brotli"              % "4.11.0",
      "com.zaxxer"                       % "HikariCP"                   % "5.0.1",
      "commons-fileupload"               % "commons-fileupload"         % "1.5",
      "org.apache.jclouds"               % "jclouds-blobstore"          % "2.5.0",
      "org.apache.jclouds.api"           % "filesystem"                 % "2.5.0",
      "org.apache.jclouds.driver"        % "jclouds-slf4j"              % "2.5.0",
      "org.apache.jclouds.provider"      % "aws-s3"                     % "2.5.0",
      "org.apache.jclouds.provider"      % "b2"                         % "2.5.0",
      "org.eclipse.jetty"                % "jetty-server"               % "11.0.16",
      "org.flywaydb"                     % "flyway-database-postgresql" % "11.0.1",
      "org.postgresql"                   % "postgresql"                 % "42.7.4",
      "org.slf4j"                        % "slf4j-api"                  % "1.7.36",
      "org.slf4j"                        % "slf4j-simple"               % "1.7.36",
      "com.github.pureconfig"           %% "pureconfig-generic-scala3"  % "0.17.8",
      "com.typesafe.scala-logging"      %% "scala-logging"              % "3.9.5",
      "org.tpolecat"                    %% "skunk-core"                 % "0.6.4",
      "org.typelevel"                   %% "cats-effect"                % "3.5.7",
      "org.scalikejdbc"                 %% "scalikejdbc"                % "4.3.+",
      "org.scalameta"                   %% "munit"                      % "1.1.0" % Test,
      "org.typelevel"                   %% "munit-cats-effect"          % "2.0.0" % Test,
    )
  )
