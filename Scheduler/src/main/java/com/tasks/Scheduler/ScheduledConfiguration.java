package com.tasks.Scheduler;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

@Configuration
@EnableScheduling
public class ScheduledConfiguration implements SchedulingConfigurer {
    static volatile public ScheduledConfiguration scheduledConfiguration;
    TaskScheduler taskScheduler;
    private ScheduledFuture<?> job1;
    private ScheduledFuture<?> job2;

    public ScheduledConfiguration ScheduledConfiguration() {
        if(scheduledConfiguration==null){
            scheduledConfiguration =this;
        }
        return scheduledConfiguration;
    }

    private volatile boolean job1Flag=false;
    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        scheduledConfiguration =this;
        ThreadPoolTaskScheduler threadPoolTaskScheduler =new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(10);// Set the pool of threads
        threadPoolTaskScheduler.setThreadNamePrefix("scheduler-thread");
        threadPoolTaskScheduler.initialize();
        job1(threadPoolTaskScheduler);// Assign the job1 to the scheduler
        job2(threadPoolTaskScheduler);// Assign the job1 to the scheduler
        this.taskScheduler=threadPoolTaskScheduler;// this will be used in later part of the article during refreshing the cron expression dynamically
        taskRegistrar.setTaskScheduler(threadPoolTaskScheduler);

    }

    private void job1(TaskScheduler scheduler) {
        job1 = scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + " The Task1 executed at " + new Date());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    System.out.println(e.toString());
                }
            }
        }, new Trigger() {
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                String cronExp = "0/10 * * * * ?";// Can be pulled from a db .
                return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
            }
        });
    }

    private void job2(TaskScheduler scheduler){
        job2=scheduler.schedule(new Runnable(){
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName()+" The Task2 executed at "+ new Date());
            }
        }, new Trigger(){
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                String cronExp="0/3 * * * * ?";//Can be pulled from a db . This will run every minute
                return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
            }
        });
    }
    public void refreshCronSchedule(){
        System.out.println("went through refresh");
        if(job1!=null){
            job1.cancel(true);
            scheduleJob1(taskScheduler);
        }

        if(job2!=null){
            job2.cancel(true);
            scheduleJob2(taskScheduler);
        }
    }

    private void scheduleJob2(TaskScheduler scheduler) {
        job2=scheduler.schedule(new Runnable(){

            @Override
            public void run() {
                synchronized(this){
                    while(!job1Flag){
                        System.out.println(Thread.currentThread().getName()+" waiting for job1 to complete to execute "+ new Date());
                        try {
                            wait(1000);// add any number of seconds to wait
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                System.out.println(Thread.currentThread().getName()+" The Task222222 executed at "+ new Date());
                job1Flag=false;
            }
        }, new Trigger(){
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                String cronExp="0/5 * * * * ?";//Can be pulled from a db . This will run every minute
                return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
            }
        });
    }
    private void scheduleJob1(TaskScheduler scheduler) {
        job1 = scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + " The Task111111 executed at " + new Date());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                job1Flag=true;// setting the flag true to mark it complete
            }
        }, new Trigger() {
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                String cronExp = "0/5 * * * * ?";// Can be pulled from a db
                return new CronTrigger(cronExp).nextExecutionTime(triggerContext);
            }
        });

    }
}
