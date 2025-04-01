// Copyright 2025 Open3FS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/open3fs/m3fs/pkg/errors"
	"github.com/sirupsen/logrus"
)

// ProgressInfo tracks the progress information of a task execution
type ProgressInfo struct {
	TaskID         string    `json:"taskId"`
	Name           string    `json:"name"`
	Completed      bool      `json:"completed"`
	TotalSteps     int       `json:"totalSteps"`
	CompletedSteps int       `json:"completedSteps"`
	StartTime      time.Time `json:"startTime"`
	EndTime        time.Time `json:"endTime,omitempty"`
}

// DeploymentProgress stores the overall deployment progress
type DeploymentProgress struct {
	StartTime      time.Time               `json:"startTime"`
	EndTime        time.Time               `json:"endTime,omitempty"`
	TotalTasks     int                     `json:"totalTasks"`
	CompletedTasks int                     `json:"completedTasks"`
	CurrentTask    string                  `json:"currentTask"`
	TaskProgress   map[string]ProgressInfo `json:"taskProgress"`
}

// NewDeploymentProgress creates a new deployment progress tracker
func NewDeploymentProgress() *DeploymentProgress {
	return &DeploymentProgress{
		StartTime:    time.Now(),
		TaskProgress: make(map[string]ProgressInfo),
	}
}

// SaveProgressToFile saves the progress information to a file
func (dp *DeploymentProgress) SaveProgressToFile(filePath string) error {
	// Ensure the directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Trace(err)
	}

	data, err := json.MarshalIndent(dp, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}

	return os.WriteFile(filePath, data, 0644)
}

// LoadProgressFromFile loads progress information from a file
func LoadProgressFromFile(filePath string) (*DeploymentProgress, error) {
	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return NewDeploymentProgress(), nil // File doesn't exist, return a new progress
	}

	// Read and parse the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var progress DeploymentProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, errors.Trace(err)
	}

	return &progress, nil
}

// DisplayProgress displays the progress information
func (dp *DeploymentProgress) DisplayProgress(
	taskIndex int,
	taskName string,
	progressStyle string,
	colorAttr color.Attribute,
) {
	// Check if color should be used
	useColor := int(colorAttr) >= 0

	// Calculate progress percentage
	percentage := 0.0
	if dp.TotalTasks > 0 {
		percentage = float64(taskIndex) / float64(dp.TotalTasks) * 100
	}

	var message string
	switch progressStyle {
	case "bar":
		// Display progress bar
		// Example: [==========>     ] 60% (6/10) Current: Installing Meta Service
		const width = 30
		completedWidth := int(float64(width) * percentage / 100)

		// Build the progress bar string
		bar := strings.Builder{}
		bar.WriteString("[")
		for i := 0; i < width; i++ {
			if i < completedWidth {
				bar.WriteString("=")
			} else if i == completedWidth && completedWidth < width {
				bar.WriteString(">")
			} else {
				bar.WriteString(" ")
			}
		}
		bar.WriteString("]")

		message = fmt.Sprintf("%s %.1f%% (%d/%d) Current: %s",
			bar.String(), percentage, taskIndex+1, dp.TotalTasks, taskName)
	case "percentage":
		// Only display percentage
		message = fmt.Sprintf("Deployment progress: %.1f%% (%d/%d) - Running task: %s",
			percentage, taskIndex+1, dp.TotalTasks, taskName)
	default:
		// Simple display
		message = fmt.Sprintf("Running task %s (%d/%d)", taskName, taskIndex+1, dp.TotalTasks)
	}

	// Apply color - integrates with existing taskInfoColor configuration
	if useColor {
		taskHighlight := color.New(colorAttr).SprintFunc()
		message = taskHighlight(message)
	}

	logrus.Info(message)
}

// DisplayDeploymentComplete displays the deployment completion information
func (dp *DeploymentProgress) DisplayDeploymentComplete(colorAttr color.Attribute) {
	// Check if color should be used
	useColor := int(colorAttr) >= 0

	message := "Deployment completed successfully!"

	if useColor {
		completeHighlight := color.New(colorAttr, color.Bold).SprintFunc()
		message = completeHighlight(message)
	}

	logrus.Info(message)

	// Calculate total time
	if !dp.EndTime.IsZero() {
		duration := dp.EndTime.Sub(dp.StartTime)
		logrus.Infof("Total deployment time: %s", formatDuration(duration))
	}
}

// formatDuration formats a duration to be more readable
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	parts := []string{}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d hours", hours))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d minutes", minutes))
	}
	if seconds > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%d seconds", seconds))
	}

	return strings.Join(parts, " ")
}
