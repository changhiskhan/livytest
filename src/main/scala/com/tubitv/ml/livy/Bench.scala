package com.tubitv.ml.livy

// Must not be in default package
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Fork, Mode, OutputTimeUnit, Scope, Setup, State, TearDown, Warmup}

/* Default settings for benchmarks in this class */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.All))
@Warmup(iterations=3)
@Fork(1)
@State(Scope.Benchmark)
class Bench {

  @Setup
  def doSetup(): Unit = {
    EstimatePi.init("http://localhost:8900/")
    EstimatePi.uploadRelevantJarsForJobExecution()
  }

  @TearDown
  def doTearDown(): Unit = {
    EstimatePi.stopClient()
  }

  @Benchmark
  def noop(): Unit = {
    EstimatePi.doNothing()
  }

  @Benchmark
  def estimatePi(): Unit = {
    EstimatePi.estimatePi(randomInt)
  }

  def randomInt: Int = {
    scala.util.Random.nextInt(10000) + 995000
  }
}
