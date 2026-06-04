package activity

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

func LogStreakMilestone(ctx context.Context, userID string, streak int) error {
	logger := activity.GetLogger(ctx)
	message := fmt.Sprintf("User %s hit a %d-day streak!", userID, streak)
	logger.Info(message)
	return nil
}
