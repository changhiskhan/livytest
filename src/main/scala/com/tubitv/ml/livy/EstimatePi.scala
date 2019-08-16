package com.tubitv.ml.livy

import java.io.{File, FileNotFoundException}
import java.net.URI

import org.apache.livy.LivyClientBuilder
import org.apache.livy.scalaapi._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 *  A WordCount example using Scala-API which reads text from a stream and saves
 *  it as data frames. The word with maximum count is the result.
 */
object EstimatePi {

  var scalaClient: LivyScalaClient = _

  /**
   *  Initializes the Scala client with the given url.
   *  @param url The Livy server url.
   */
  def init(url: String): Unit = {
    scalaClient = new LivyClientBuilder(false).setURI(new URI(url)).build().asScalaClient
  }

  /**
   *  Uploads the Scala-API Jar and the examples Jar from the target directory.
   *  @throws FileNotFoundException If either of Scala-API Jar or examples Jar is not found.
   */
  @throws(classOf[FileNotFoundException])
  def uploadRelevantJarsForJobExecution(): Unit = {
    val exampleAppJarPath = getSourcePath(this)
    val scalaApiJarPath = getSourcePath(scalaClient)
    uploadJar(exampleAppJarPath)
    uploadJar(scalaApiJarPath)
  }

  @throws(classOf[FileNotFoundException])
  private def getSourcePath(obj: Object): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(s"Jar containing ${obj.getClass.getName} not found.")
    }
  }

  private def uploadJar(path: String): Unit = {
    val file = new File(path)
    val uploadJarFuture = scalaClient.uploadJar(file)
    Await.result(uploadJarFuture, 40 second) match {
      case null => println("Successfully uploaded " + file.getName)
    }
  }

  /**
   * Submits a spark sql job to the livy server.
   *
   * The sql context job reads data frames from the given json path and executes
   * a sql query to get the word with max count on the temp table created with data frames.
   */
  def estimatePi(num_samples: Int): Long = {
    val start = System.currentTimeMillis()
    val handle = scalaClient.submit { context =>
      val count = context.sc.parallelize(1 to num_samples).map { i =>
        val x = Math.random()
        val y = Math.random()
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
      4.0 * count / num_samples
    }
    Await.result(handle, 1 second)
    System.currentTimeMillis() - start
  }

  def doNothing(): Long = {
    val start = System.currentTimeMillis()
    val handle = scalaClient.submit { context =>

    }
    Await.result(handle, 1 second)
    System.currentTimeMillis() - start
  }

  private def stopClient(): Unit = {
    if (scalaClient != null) {
      scalaClient.stop(true)
      scalaClient = null;
    }
  }

  /**
   * λ ~/code/livytest/ sbt run
   * [info] Loading settings for project global-plugins from idea.sbt ...
   * [info] Loading global plugins from /home/chang/.sbt/1.0/plugins
   * [info] Loading project definition from /home/chang/code/livytest/project
   * [info] Loading settings for project livytest from build.sbt ...
   * [info] Set current project to livytest (in build file:/home/chang/code/livytest/)
   * [info] Running com.tubitv.ml.livy.EstimatePi
   * log4j:WARN No appenders could be found for logger (org.apache.livy.shaded.apache.http.client.protocol.RequestAddCookies).
   * log4j:WARN Please initialize the log4j system properly.
   * log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
   * Successfully uploaded livytest_2.11-0.1.jar
   * Successfully uploaded livy-scala-api_2.11-0.6.0-incubating.jar
   *
   * Warmed up nothing for 161 milliseconds
   * 1000 nothings averaged 109 milliseconds
   * Warmed up pi estimate for 717 milliseconds
   * 1000 pi's averaged 143 milliseconds
   */
  def main(args: Array[String]): Unit = {
    try {
      init("http://localhost:8900/")
      uploadRelevantJarsForJobExecution()
      println(s"Warmed up nothing for ${doNothing()} milliseconds")
      println(s"1000 nothings averaged ${(1 to 1000).map(_ => doNothing()).sum / 1000} milliseconds")
      println(s"Warmed up pi estimate for ${estimatePi(1000000)} milliseconds")
      println(s"1000 pi's averaged ${(1 to 1000).map(_ => estimatePi(1000000)).sum / 1000} milliseconds")
    } finally {
      stopClient()
    }
  }
}