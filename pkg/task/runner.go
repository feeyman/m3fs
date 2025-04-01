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
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"

	"github.com/open3fs/m3fs/pkg/config"
	"github.com/open3fs/m3fs/pkg/errors"
	"github.com/open3fs/m3fs/pkg/external"
	"github.com/open3fs/m3fs/pkg/log"
	"github.com/open3fs/m3fs/pkg/utils"
)

// defines keys of runtime cache.
const (
	RuntimeArtifactTmpDirKey    = "artifact/tmp_dir"
	RuntimeArtifactPathKey      = "artifact/path"
	RuntimeArtifactGzipKey      = "artifact/gzip"
	RuntimeArtifactSha256sumKey = "artifact/sha256sum"
	RuntimeArtifactFilePathsKey = "artifact/file_paths"

	RuntimeClickhouseTmpDirKey      = "clickhouse/tmp_dir"
	RuntimeMonitorTmpDirKey         = "monitor/tmp_dir"
	RuntimeFdbClusterFileContentKey = "fdb/cluster_file_content"
	RuntimeMgmtdServerAddressesKey  = "mgmtd/server_addresses"
	RuntimeUserTokenKey             = "user_token"
	RuntimeAdminCliTomlKey          = "admin_cli_toml"
)

// Runtime contains task run info
type Runtime struct {
	sync.Map
	Cfg       *config.Config
	Nodes     map[string]config.Node
	Services  *config.Services
	WorkDir   string
	LocalEm   *external.Manager
	LocalNode *config.Node

	// Fields related to progress tracking
	Progress *DeploymentProgress
}

// LoadString load string value form sync map
func (r *Runtime) LoadString(key any) (string, bool) {
	valI, ok := r.Load(key)
	if !ok {
		return "", false
	}

	return valI.(string), true
}

// LoadBool load bool value form sync map
func (r *Runtime) LoadBool(key any) (bool, bool) {
	valI, ok := r.Load(key)
	if !ok {
		return false, false
	}

	return valI.(bool), true
}

// LoadInt load int value form sync map
func (r *Runtime) LoadInt(key any) (int, bool) {
	valI, ok := r.Load(key)
	if !ok {
		return 0, false
	}

	return valI.(int), true
}

// Runner is a task runner.
type Runner struct {
	Runtime      *Runtime
	tasks        []Interface
	cfg          *config.Config
	localNode    *config.Node
	init         bool
	progressFile string // Progress file path
}

// Init initializes all tasks.
func (r *Runner) Init() {
	r.Runtime = &Runtime{Cfg: r.cfg, WorkDir: r.cfg.WorkDir, LocalNode: r.localNode}
	r.Runtime.Nodes = make(map[string]config.Node, len(r.cfg.Nodes))
	for _, node := range r.cfg.Nodes {
		r.Runtime.Nodes[node.Name] = node
	}
	r.Runtime.Services = &r.cfg.Services
	logger := log.Logger.Subscribe(log.FieldKeyNode, "<LOCAL>")
	runnerCfg := &external.LocalRunnerCfg{
		Logger:         logger,
		MaxExitTimeout: r.cfg.CmdMaxExitTimeout,
	}
	if r.localNode != nil {
		runnerCfg.User = r.localNode.Username
		if r.localNode.Password != nil {
			runnerCfg.Password = *r.localNode.Password
		}
	}
	em := external.NewManager(external.NewLocalRunner(runnerCfg), logger)
	r.Runtime.LocalEm = em

	// 初始化进度跟踪
	r.initProgressTracking()

	for _, task := range r.tasks {
		task.Init(r.Runtime, log.Logger.Subscribe(log.FieldKeyTask, task.Name()))
	}
	r.init = true
}

// Store sets the value for a key.
func (r *Runner) Store(key, value any) error {
	if r.Runtime == nil {
		return errors.Errorf("Runtime hasn't been initialized")
	}
	r.Runtime.Store(key, value)
	return nil
}

// Register registers tasks.
func (r *Runner) Register(task ...Interface) error {
	if r.init {
		return errors.New("runner has been initialized")
	}
	r.tasks = append(r.tasks, task...)
	return nil
}

// getColorAttribute returns the corresponding color.Attribute based on the color name in configuration
// Returns -1 if the color name is "none" or not recognized
func getColorAttribute(colorName string) color.Attribute {
	if strings.ToLower(colorName) == "none" {
		return color.Attribute(-1) // Special value to indicate no color
	}

	colorMap := map[string]color.Attribute{
		"green":   color.FgHiGreen,
		"cyan":    color.FgHiCyan,
		"yellow":  color.FgHiYellow,
		"blue":    color.FgHiBlue,
		"magenta": color.FgHiMagenta,
		"red":     color.FgHiRed,
		"white":   color.FgHiWhite,
	}

	if attr, ok := colorMap[strings.ToLower(colorName)]; ok {
		return attr
	}

	// Return invalid attribute to indicate no color
	return color.Attribute(-1)
}

// initProgressTracking initializes progress tracking
func (r *Runner) initProgressTracking() {
	if r.cfg.Deployment.ProgressFilePath != "" {
		r.progressFile = r.cfg.Deployment.ProgressFilePath
	} else {
		r.progressFile = filepath.Join(r.cfg.WorkDir, "deployment_progress.json")
	}

	if r.cfg.Deployment.ResumeEnabled {
		progress, err := LoadProgressFromFile(r.progressFile)
		if err != nil {
			logrus.Warnf("Failed to load progress file: %v, starting fresh deployment", err)
			r.Runtime.Progress = NewDeploymentProgress()
		} else {
			r.Runtime.Progress = progress
			logrus.Infof("Resuming deployment with %d/%d completed tasks",
				progress.CompletedTasks, progress.TotalTasks)
		}
	} else {
		r.Runtime.Progress = NewDeploymentProgress()
	}

	r.Runtime.Progress.TotalTasks = len(r.tasks)
}

// saveProgress saves current progress to file
func (r *Runner) saveProgress() error {
	if !r.cfg.Deployment.ResumeEnabled {
		return nil
	}

	return r.Runtime.Progress.SaveProgressToFile(r.progressFile)
}

// Run runs all tasks.
func (r *Runner) Run(ctx context.Context) error {
	useColor := false
	var highlightColor color.Attribute

	if r.cfg != nil && r.cfg.UI.TaskInfoColor != "" {
		highlightColor = getColorAttribute(r.cfg.UI.TaskInfoColor)
		useColor = int(highlightColor) >= 0
	}

	for i, task := range r.tasks {
		taskID := task.Name()

		if r.cfg.Deployment.ResumeEnabled {
			if info, exists := r.Runtime.Progress.TaskProgress[taskID]; exists && info.Completed {
				logrus.Infof("Skipping completed task %s (%d/%d)", task.Name(), i+1, r.Runtime.Progress.TotalTasks)
				continue
			}
		}

		r.Runtime.Progress.CurrentTask = task.Name()

		if r.cfg.UI.ShowProgress {
			r.Runtime.Progress.DisplayProgress(i, task.Name(), r.cfg.UI.ProgressStyle, highlightColor)
		} else {
			var message string
			if useColor {
				taskHighlight := color.New(highlightColor, color.Bold).SprintFunc()
				message = taskHighlight(fmt.Sprintf("Running task %s", task.Name()))
			} else {
				message = fmt.Sprintf("Running task %s", task.Name())
			}

			logrus.Info(message)
		}

		r.Runtime.Progress.TaskProgress[taskID] = ProgressInfo{
			TaskID:    taskID,
			Name:      task.Name(),
			StartTime: time.Now(),
		}

		if err := r.saveProgress(); err != nil {
			logrus.Warnf("Failed to save progress: %v", err)
		}

		if err := task.Run(ctx); err != nil {
			return errors.Annotatef(err, "run task %s", task.Name())
		}

		info := r.Runtime.Progress.TaskProgress[taskID]
		info.Completed = true
		info.EndTime = time.Now()
		r.Runtime.Progress.TaskProgress[taskID] = info
		r.Runtime.Progress.CompletedTasks++

		if err := r.saveProgress(); err != nil {
			logrus.Warnf("Failed to save progress: %v", err)
		}
	}

	r.Runtime.Progress.EndTime = time.Now()

	if r.cfg.UI.ShowProgress {
		r.Runtime.Progress.DisplayDeploymentComplete(highlightColor)
	}

	if err := r.saveProgress(); err != nil {
		logrus.Warnf("Failed to save final progress: %v", err)
	}

	return nil
}

// NewRunner creates a new task runner.
func NewRunner(cfg *config.Config, tasks ...Interface) (*Runner, error) {
	localIPs, err := utils.GetLocalIPs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var localNode *config.Node
	for i, node := range cfg.Nodes {
		if isLocal, err := utils.IsLocalHost(node.Host, localIPs); err != nil {
			return nil, errors.Trace(err)
		} else if isLocal {
			localNode = &cfg.Nodes[i]
			break
		}
	}
	return &Runner{
		tasks:     tasks,
		localNode: localNode,
		cfg:       cfg,
	}, nil
}
