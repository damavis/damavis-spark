package com.damavis.spark.pipeline2

object PipelineStage {
  def apply(processor: Processor): PipelineStage =
    new PipelineStage(processor)
}

class PipelineStage(private val processor: Processor) {

  private var next: PipelineStage = NoStage
  private var dependsOn: Seq[PipelineStage] = Nil
  private var _toRun: Boolean = true

  protected val sockets: SocketSet =
    SocketSet(new StageSocket(this), new StageSocket(this))
  private var deliverySocket: StageSocket = _

  protected def toRun: Boolean = _toRun

  def left: StageSocket = sockets.left
  def right: StageSocket = sockets.right

  protected def addDependency(stage: PipelineStage): Unit = {
    dependsOn = stage +: dependsOn
  }

  def compute(): Unit = {
    val dependenciesSatisfied = !dependsOn.exists(_.toRun)

    if (_toRun && dependenciesSatisfied) {
      val newData = processor.compute(sockets)

      _toRun = false

      deliverySocket.set(newData)
      next.compute()
    }

  }

  def ->(socket: StageSocket)(
      implicit definition: PipelineDefinition): PipelineStage = {
    //TODO: disallow assignment to stages already connected

    if (this == socket.stage)
      throw new RuntimeException("Loops not allowed in the pipeline")

    deliverySocket = socket

    next = socket.stage
    next.addDependency(this)

    next
  }

}
