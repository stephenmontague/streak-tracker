package workflow

import "time"

const (
	TaskQueue            = "streak-tracker"
	RecordActivitySignal = "record-activity"
	ChangeTimezoneSignal = "change-timezone"
	GetStreakQuery        = "get-streak"
)

type StreakState struct {
	UserID           string    `json:"userId"`
	CurrentStreak    int       `json:"currentStreak"`
	LongestStreak    int       `json:"longestStreak"`
	LastActivityTime time.Time `json:"lastActivityTime"`
	Timezone         string    `json:"timezone"`
	AnchorTimezone   string    `json:"anchorTimezone"`
	RecordedToday    bool      `json:"recordedToday"`
	TotalDays        int       `json:"totalDays"`

	// Demo mode fields
	DayDurationSeconds  int       `json:"dayDurationSeconds"`  // 0 = real days, >0 = seconds per "day"
	CurrentDay          int       `json:"currentDay"`          // Virtual day counter (demo mode)
	LastRecordedDay     int       `json:"lastRecordedDay"`     // Day number of last recording (demo mode)
	LastDayStartTime    time.Time `json:"lastDayStartTime"`    // When current virtual day started

	// Computed by query handler, not persisted
	SecondsUntilMidnight int `json:"secondsUntilMidnight"`
}

type RecordActivityPayload struct {
	Timezone string `json:"timezone"`
}

type ChangeTimezonePayload struct {
	NewTimezone string `json:"newTimezone"`
}
