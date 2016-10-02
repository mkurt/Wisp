package com.coreoz.wisp.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.Test;

import com.coreoz.wisp.schedule.AfterInitialDelaySchedule;
import com.coreoz.wisp.schedule.Schedule;
import com.coreoz.wisp.schedule.Schedules;

public class AfterInitialDelayScheduleTest {

	@Test
	public void first_execution_should_depends_only_on_the_first_delay() {
		AfterInitialDelaySchedule after1msDelay = new AfterInitialDelaySchedule(null, Duration.ofMillis(1));

		assertThat(after1msDelay.nextExecutionInMillis(0, 0)).isEqualTo(1);
	}

	@Test
	public void second_execution_should_depends_only_on_the_first_delay() {
		Schedule every5ms = Schedules.fixedDelaySchedule(Duration.ofMillis(5));
		AfterInitialDelaySchedule afterUnusedDelay = new AfterInitialDelaySchedule(every5ms, null);

		assertThat(afterUnusedDelay.nextExecutionInMillis(1, 0)).isEqualTo(5);
	}

}