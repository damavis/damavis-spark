package com.damavis.spark.utils

import com.holdenkarau.spark.testing.{HDFSCluster => MockHDFS}

object HDFSCluster {
  //This mock cluster is global withing a JVM, and lasts until the process finishes
  //This saves a lot of time in test startup/cleanup

  protected var hdfsCluster: MockHDFS = _

  def uri: String = {
    this.synchronized {
      if (hdfsCluster == null) {
        hdfsCluster = new MockHDFS
        hdfsCluster.startHDFS()

        sys.addShutdownHook {
          hdfsCluster.shutdownHDFS()
        }

      }
    }

    hdfsCluster.getNameNodeURI()
  }

}
