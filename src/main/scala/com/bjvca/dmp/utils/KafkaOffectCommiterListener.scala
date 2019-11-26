package com.bjvca.dmp.utils

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

class KafkaOffectCommiterListener extends StreamingListener with Logging{
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    super.onBatchCompleted(batchCompleted)
    logWarning("调用批完成方法")
  }
}
