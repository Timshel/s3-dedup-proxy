package timshel.s3dedupproxy

import cats.effect._
import cats.effect.std.Dispatcher
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder.cronSchedule;
import org.quartz.JobBuilder.newJob;
import org.quartz.TriggerBuilder.newTrigger;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.SchedulerMetaData;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

case class Cleanup(
    db: Database,
    client: ObjectStoreClient,
    dispatcher: Dispatcher[IO]
) {
  import Cleanup.log

  val sched = new StdSchedulerFactory().getScheduler();

  def purge(): IO[Int] = {
    log.debug("Running purge job")
    for {
      hashes <- db.getDangling(1000)
      _      <- client.deleteKeys(hashes)
      count  <- db.delMetadatas(hashes)
      _ = log.debug(s"Purged $count files")
    } yield count
  }

}

object Cleanup {
  val log = com.typesafe.scalalogging.Logger(classOf[Application])

  class CleanupJob() extends org.quartz.Job {
    def execute(context: org.quartz.JobExecutionContext) = {
      val cleanup = context.getMergedJobDataMap().get("cleanup").asInstanceOf[Cleanup]
      cleanup.dispatcher.unsafeRunSync(cleanup.purge())
    }
  }

  def scheduled(
      config: GlobalConfig,
      db: Database,
      dispatcher: Dispatcher[IO]
  ): Resource[IO, Cleanup] = Resource.make(IO.blocking {
    import scala.jdk.CollectionConverters._

    val client  = ObjectStoreClient(config.backend)
    val cleanup = Cleanup(db, client, dispatcher)

    val job = newJob(classOf[Cleanup.CleanupJob])
      .withIdentity("cleaner", "cleaners")
      .usingJobData(new org.quartz.JobDataMap(Map("cleanup" -> cleanup).asJava))
      .build()

    val trigger = newTrigger().withIdentity("cron", "cleaners").withSchedule(cronSchedule(config.proxy.purge)).build();

    cleanup.sched.scheduleJob(job, trigger)
    cleanup.sched.getContext().put("cleanup", cleanup)
    cleanup.sched.start();
    log.info(s"Cleanup sheduler running with cron ${config.proxy.purge}")

    cleanup
  })(cleanup =>
    IO.blocking {
      log.info("Stopping cleanup scheduler")
      cleanup.sched.shutdown(true);
    }
  )

}
