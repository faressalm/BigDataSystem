package packages.Scheduler;

import com.clearspring.analytics.util.Pair;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;
import packages.SpeedLayerProcessing.SpeedLayerProcessing;
import packages.batchProcessing.DayAggregation;
import packages.batchProcessing.DayMinutes;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

@Configuration
@EnableScheduling
public class ScheduledConfiguration implements SchedulingConfigurer {
    volatile public ScheduledConfiguration scheduledConfiguration;
    TaskScheduler taskScheduler;
    private ScheduledFuture < ? > batchJobMin;
    private ScheduledFuture < ? > batchJobDay;
    private ScheduledFuture < ? > sparkMainJob;
    private ScheduledFuture < ? > sparkTempJob;
    private ScheduledFuture < ? > terminateJobsJob;
    private volatile Pair < Boolean, Boolean > batchStart = new Pair < > (false, false);
    private volatile String sparkQueryPath = "";
    private volatile String sparkWritePath = "";
    private String inputPath = "./../../";
    private  volatile  int currentDay = 0;
    private DayAggregation dayAggregation = new DayAggregation();
    private DayMinutes dayMinutes = new DayMinutes();
    private SpeedLayerProcessing speedLayerProcessing = new SpeedLayerProcessing();
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        scheduledConfiguration = this;
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(10); // Set the pool of threads
        threadPoolTaskScheduler.setThreadNamePrefix("scheduler-thread");
        threadPoolTaskScheduler.initialize();
        scheduleBatchJobMin(threadPoolTaskScheduler);// Assign the batch days job to the scheduler
        scheduleBatchJobDay(threadPoolTaskScheduler);// Assign the batch min to job to the scheduler
        scheduleSparkMainJob(threadPoolTaskScheduler); // Assign spark main job
        this.taskScheduler = threadPoolTaskScheduler; // this will be used in later part of the article during refreshing the cron expression dynamically
        taskRegistrar.setTaskScheduler(threadPoolTaskScheduler);

    }

    private void scheduleBatchJobMin(TaskScheduler scheduler) {
        batchJobMin = scheduler.schedule(() -> {
            try {
                dayMinutes.runTask(inputPath+currentDay+".json");
                batchStart = new Pair < > (true, batchStart.right);
                terminateJobs(scheduler);
                System.out.println("Sssss");
            } catch (Exception e) {

                System.out.println(e.toString());
            }
        }, triggerContext -> {
            String cronExp = "0 0 8 * * ?"; // run every day at 8 am
            return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
        });
    }

    private void scheduleBatchJobDay(TaskScheduler scheduler) {
        batchJobDay = scheduler.schedule(() -> {
            try {
                dayAggregation.runTask(inputPath+currentDay+".json");
                batchStart = new Pair < > (batchStart.left, true);
            } catch (Exception e) {

                System.out.println(e.toString());
            }
        }, triggerContext -> {
            String cronExp = "0 0 8 * * ?"; // run every day at 8 am
            return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
        });
    }
    private void scheduleSparkMainJob(TaskScheduler scheduler) {
        sparkMainJob = scheduler.schedule(() -> {
            try {
                speedLayerProcessing.runTask(inputPath+(currentDay+1)+".json");
            } catch (Exception e) {

                System.out.println(e.toString());
            }
        }, triggerContext -> {
            String cronExp = "0/3 * * * * ?"; //Can be pulled from a db . This will run every minute
            return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
        });
    }
    private void scheduleSparkTempJob(TaskScheduler scheduler) {
        sparkTempJob = scheduler.schedule(() -> {
            try {
                speedLayerProcessing.runTask(inputPath+(currentDay+1)+".json");
            } catch (Exception e) {
                System.out.println(e);
            }
        }, triggerContext -> {
            String cronExp = "0 0 8 * * ?"; // run every day at 8 am
            return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
        });
    }

    private void terminateJobs(TaskScheduler scheduler) {
        terminateJobsJob = scheduler.schedule(new Runnable() {
            @Override
            public void run() {

                synchronized(this) {
                    if (batchStart.left && batchStart.right) {
                        sparkMainJob.cancel(true);
                        //TODO::
                        if (terminateJobsJob != null) {
                            terminateJobsJob.cancel(true);
                            terminateJobsJob = null;
                        }
                        batchStart = new Pair < > (false, false);
                        return;
                    }
                }

            }
        }, triggerContext -> {
            String cronExp = "0/1 * * * * ?"; //run every sec
            return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
        });
    }
}