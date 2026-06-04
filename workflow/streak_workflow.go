package workflow

import (
	"fmt"
	"time"
	_ "time/tzdata"

	"go.temporal.io/sdk/workflow"
)

func StreakWorkflow(ctx workflow.Context, state StreakState) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("StreakWorkflow started", "userId", state.UserID, "timezone", state.Timezone, "demoMode", state.DayDurationSeconds > 0)

	if state.Timezone == "" {
		state.Timezone = "America/Chicago"
	}
	if state.AnchorTimezone == "" {
		state.AnchorTimezone = state.Timezone
	}
	if state.LastDayStartTime.IsZero() {
		state.LastDayStartTime = workflow.Now(ctx)
	}

	// Query handler: returns streak state with stale detection and countdown
	err := workflow.SetQueryHandler(ctx, GetStreakQuery, func() (StreakState, error) {
		result := state
		// Clear the computed field before populating
		result.SecondsUntilMidnight = 0

		// Stale detection
		if !state.LastActivityTime.IsZero() && isStreakStale(ctx, state) {
			result.CurrentStreak = 0
			result.RecordedToday = false
		}

		// Countdown to next "midnight"
		if state.DayDurationSeconds > 0 {
			elapsed := workflow.Now(ctx).Sub(state.LastDayStartTime)
			remaining := time.Duration(state.DayDurationSeconds)*time.Second - elapsed
			if remaining < 0 {
				remaining = 0
			}
			result.SecondsUntilMidnight = int(remaining.Seconds())
		} else {
			result.SecondsUntilMidnight = int(calcDurationUntilMidnight(ctx, state.Timezone).Seconds())
		}

		return result, nil
	})
	if err != nil {
		return fmt.Errorf("failed to set query handler: %w", err)
	}

	recordCh := workflow.GetSignalChannel(ctx, RecordActivitySignal)
	timezoneCh := workflow.GetSignalChannel(ctx, ChangeTimezoneSignal)

	iteration := 0
	for {
		// Calculate timer duration
		var timerDuration time.Duration
		if state.DayDurationSeconds > 0 {
			timerDuration = time.Duration(state.DayDurationSeconds) * time.Second
		} else {
			timerDuration = calcDurationUntilMidnight(ctx, state.Timezone)
		}

		timerCtx, timerCancel := workflow.WithCancel(ctx)
		timer := workflow.NewTimer(timerCtx, timerDuration)
		timerFired := false

		sel := workflow.NewSelector(ctx)

		sel.AddFuture(timer, func(f workflow.Future) {
			if f.Get(timerCtx, nil) == nil {
				timerFired = true
			}
		})

		sel.AddReceive(recordCh, func(ch workflow.ReceiveChannel, more bool) {
			var payload RecordActivityPayload
			ch.Receive(ctx, &payload)
			tzChanged := payload.Timezone != "" && payload.Timezone != state.Timezone
			handleRecordActivity(ctx, &state, payload.Timezone)
			if tzChanged {
				timerCancel()
			}
			logger.Info("Activity recorded", "userId", state.UserID, "streak", state.CurrentStreak, "day", state.CurrentDay)
		})

		sel.AddReceive(timezoneCh, func(ch workflow.ReceiveChannel, more bool) {
			var payload ChangeTimezonePayload
			ch.Receive(ctx, &payload)
			if _, err := time.LoadLocation(payload.NewTimezone); err == nil {
				state.Timezone = payload.NewTimezone
				timerCancel()
				logger.Info("Timezone simulated", "userId", state.UserID, "newTimezone", payload.NewTimezone)
			}
		})

		sel.Select(ctx)

		if timerFired {
			// "Midnight" passed — new day begins
			if state.DayDurationSeconds > 0 {
				state.CurrentDay++
				state.LastDayStartTime = workflow.Now(ctx)
				logger.Info("Demo day advanced", "userId", state.UserID, "day", state.CurrentDay)
			}
			state.RecordedToday = false
		}

		// Drain pending signals
		for {
			var payload RecordActivityPayload
			if !recordCh.ReceiveAsync(&payload) {
				break
			}
			handleRecordActivity(ctx, &state, payload.Timezone)
		}
		for {
			var payload ChangeTimezonePayload
			if !timezoneCh.ReceiveAsync(&payload) {
				break
			}
			if _, err := time.LoadLocation(payload.NewTimezone); err == nil {
				state.Timezone = payload.NewTimezone
			}
		}

		iteration++
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			logger.Info("Continuing as new", "userId", state.UserID, "iteration", iteration)
			return workflow.NewContinueAsNewError(ctx, StreakWorkflow, state)
		}
	}
}

func handleRecordActivity(ctx workflow.Context, state *StreakState, incomingTz string) {
	now := workflow.Now(ctx)

	if incomingTz == "" {
		incomingTz = state.Timezone
	}

	if state.DayDurationSeconds > 0 {
		handleRecordActivityDemo(ctx, state, incomingTz, now)
	} else {
		handleRecordActivityReal(ctx, state, incomingTz, now)
	}

	state.LastActivityTime = now
	state.RecordedToday = true
	state.AnchorTimezone = incomingTz
	state.Timezone = incomingTz

	if state.CurrentStreak > state.LongestStreak {
		state.LongestStreak = state.CurrentStreak
	}
}

func handleRecordActivityDemo(ctx workflow.Context, state *StreakState, incomingTz string, now time.Time) {
	if state.LastActivityTime.IsZero() {
		// First ever recording
		state.CurrentStreak = 1
		state.TotalDays++
		state.LastRecordedDay = state.CurrentDay
		return
	}

	if state.LastRecordedDay == state.CurrentDay {
		// Already recorded this virtual day
		return
	}

	if state.LastRecordedDay == state.CurrentDay-1 {
		// Consecutive virtual day — extend streak
		state.CurrentStreak++
		state.TotalDays++
		state.LastRecordedDay = state.CurrentDay
		return
	}

	// Missed virtual day(s) — check timezone grace
	hoursLost := calcHoursLost(ctx, state.AnchorTimezone, incomingTz)
	// Every 12 hours lost = 1 virtual day of grace
	graceVirtualDays := hoursLost / 12
	gap := state.CurrentDay - state.LastRecordedDay

	if gap <= 1+graceVirtualDays {
		// Grace covers the gap — streak preserved
		state.CurrentStreak++
		state.TotalDays++
	} else {
		// Gap too large — streak broken
		state.CurrentStreak = 1
		state.TotalDays++
	}
	state.LastRecordedDay = state.CurrentDay
}

func handleRecordActivityReal(ctx workflow.Context, state *StreakState, incomingTz string, now time.Time) {
	// Compute exact hours lost to eastward travel
	hoursLost := calcHoursLost(ctx, state.AnchorTimezone, incomingTz)

	// Wind clock back by hours lost
	effectiveNow := now.Add(-time.Duration(hoursLost) * time.Hour)

	// Compare dates in anchor timezone
	anchorLoc, err := time.LoadLocation(state.AnchorTimezone)
	if err != nil || anchorLoc == nil {
		anchorLoc = time.UTC
	}

	effectiveDate := effectiveNow.In(anchorLoc).Format("2006-01-02")
	lastDate := state.LastActivityTime.In(anchorLoc).Format("2006-01-02")
	yesterdayEffective := effectiveNow.In(anchorLoc).AddDate(0, 0, -1).Format("2006-01-02")

	if state.LastActivityTime.IsZero() {
		state.CurrentStreak = 1
		state.TotalDays++
	} else if lastDate == effectiveDate {
		// Already recorded this effective day
	} else if lastDate == yesterdayEffective {
		state.CurrentStreak++
		state.TotalDays++
	} else {
		state.CurrentStreak = 1
		state.TotalDays++
	}
}

func calcHoursLost(ctx workflow.Context, anchorTz, newTz string) int {
	if anchorTz == "" || anchorTz == newTz {
		return 0
	}
	now := workflow.Now(ctx)
	anchorLoc, err := time.LoadLocation(anchorTz)
	if err != nil {
		return 0
	}
	newLoc, err := time.LoadLocation(newTz)
	if err != nil {
		return 0
	}

	_, anchorOffset := now.In(anchorLoc).Zone()
	_, newOffset := now.In(newLoc).Zone()

	diff := newOffset - anchorOffset
	if diff > 0 {
		return diff / 3600
	}
	return 0
}

func isStreakStale(ctx workflow.Context, state StreakState) bool {
	if state.DayDurationSeconds > 0 {
		// Demo mode: stale if we've moved past the recorded day + 1
		return state.CurrentDay > state.LastRecordedDay+1
	}

	// Real mode: stale if last activity is older than yesterday
	now := workflow.Now(ctx)
	loc, err := time.LoadLocation(state.Timezone)
	if err != nil {
		loc = time.UTC
	}

	today := now.In(loc).Format("2006-01-02")
	yesterday := now.In(loc).AddDate(0, 0, -1).Format("2006-01-02")
	lastDate := state.LastActivityTime.In(loc).Format("2006-01-02")

	return lastDate != today && lastDate != yesterday
}

func calcDurationUntilMidnight(ctx workflow.Context, timezone string) time.Duration {
	now := workflow.Now(ctx)
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		loc = time.UTC
	}
	userNow := now.In(loc)
	nextMidnight := time.Date(userNow.Year(), userNow.Month(), userNow.Day()+1, 0, 0, 0, 0, loc)
	duration := nextMidnight.Sub(now)
	if duration <= 0 {
		duration = time.Second
	}
	return duration
}
