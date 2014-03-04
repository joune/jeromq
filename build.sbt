organization := "ap.test"

name := "jeromq"

libraryDependencies += "org.zeromq" % "jeromq" % "0.3.1"

//not in maven central libraryDependencies += "org.filemq" % "filemq" % "0.1.0-SNAPSHOT"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0"

testOptions in Test += Tests.Setup( () => System.setProperty("debug", "true") )

osgiSettings

OsgiKeys.privatePackage := Seq("ap.test.jeromq.*")

