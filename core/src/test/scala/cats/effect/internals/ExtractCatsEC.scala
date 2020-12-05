package cats.effect.internals

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext

// Extractor for ec driving ContextShift and scheduler driving Timer
object ExtractCatsEC {
  def executionContext: ExecutionContext                 = PoolUtils.ioAppGlobal
  def scheduledExecutorService: ScheduledExecutorService = IOTimer.scheduler
}
