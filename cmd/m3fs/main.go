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

package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/open3fs/m3fs/pkg/common"
	"github.com/open3fs/m3fs/pkg/errors"
	mlog "github.com/open3fs/m3fs/pkg/log"
)

var (
	debug            bool
	configFilePath   string
	artifactPath     string
	artifactGzip     bool
	outputPath       string
	tmpDir           string
	workDir          string
	registry         string
	clusterDeleteAll bool
)

// CheckGlobalFlagsPlacement checks if global flags are placed after subcommands
// and provides correction suggestions if needed
func checkGlobalFlagsPlacement(args []string) {
	if len(args) <= 2 {
		return
	}
	
	globalFlags := map[string]bool{
		"--debug": true, "--help": true, "-h": true,
		"--version": true, "-v": true,
	}
	
	subcommands := map[string]bool{
		"artifact": true, "a": true, "cluster": true, "c": true,
		"config": true, "cfg": true, "os": true, "tmpl": true,
	}
	
	commandIndex := -1
	for i := 1; i < len(args); i++ {
		if subcommands[args[i]] {
			commandIndex = i
			break
		}
	}
	
	if commandIndex == -1 {
		return
	}
	
	var invalidFlags []string
	var validArgs []string
	validArgs = append(validArgs, args[0])
	
	for i := 1; i < len(args); i++ {
		arg := args[i]
		
		if i == commandIndex+1 && isSubSubcommand(arg) {
			validArgs = append(validArgs, arg)
			continue
		}
		
		if i > commandIndex && isGlobalFlag(arg, globalFlags) {
			invalidFlags = append(invalidFlags, arg)
		} else {
			validArgs = append(validArgs, arg)
		}
	}
	
	if len(invalidFlags) > 0 {
		suggestion := []string{args[0]}
		suggestion = append(suggestion, invalidFlags...)
		suggestion = append(suggestion, validArgs[1:]...)
		
		fmt.Println("Error: Global flags must be placed before subcommands")
		fmt.Println("Correct usage: m3fs [global options] command [command options] [arguments...]")
		fmt.Println("Suggested command:", strings.Join(suggestion, " "))
		os.Exit(1)
	}
}

// isGlobalFlag checks if an argument is a global flag
func isGlobalFlag(arg string, flagMap map[string]bool) bool {
	if flagMap[arg] {
		return true
	}
	
	if strings.HasPrefix(arg, "--") {
		parts := strings.SplitN(arg, "=", 2)
		return flagMap[parts[0]]
	}
	
	return false
}

// isSubSubcommand checks if a string is a secondary command
func isSubSubcommand(cmd string) bool {
	subSubcmds := map[string]bool{
		"create": true, "delete": true, "destroy": true,
		"prepare": true, "export": true, "download": true,
		"d": true, "e": true, "init": true,
	}
	return subSubcmds[cmd]
}

func main() {
	checkGlobalFlagsPlacement(os.Args)
	
	app := &cli.App{
		Name:  "m3fs",
		Usage: "3FS Deploy Tool",
		Before: func(ctx *cli.Context) error {
			level := logrus.InfoLevel
			if debug {
				level = logrus.DebugLevel
			}
			mlog.InitLogger(level)
			return nil
		},
		Commands: []*cli.Command{
			artifactCmd,
			clusterCmd,
			configCmd,
			osCmd,
			tmplCmd,
		},
		Action: func(ctx *cli.Context) error {
			return cli.ShowAppHelp(ctx)
		},
		ExitErrHandler: func(cCtx *cli.Context, err error) {
			if err != nil {
				logrus.Debugf("Command failed stacktrace: %s", errors.StackTrace(err))
			}
			cli.HandleExitCoder(err)
		},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "debug",
				Usage:       "Enable debug mode",
				Destination: &debug,
			},
		},
		Version: fmt.Sprintf(`%s
Git SHA: %s
Build At: %s
Go Version: %s
Go OS/Arch: %s/%s`,
			common.Version,
			getGitShaPrefix(),
			common.BuildTime,
			runtime.Version(),
			runtime.GOOS,
			runtime.GOARCH),
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

// getGitShaPrefix safely retrieves the GitSha prefix, avoiding crashes due to empty strings
func getGitShaPrefix() string {
	if len(common.GitSha) >= 7 {
		return common.GitSha[:7]
	} else if len(common.GitSha) > 0 {
		return common.GitSha
	}
	return "unknown"
}

