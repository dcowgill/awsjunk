package main

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	profile string
	region  string
)

func main() {
	var rootCmd = &cobra.Command{Use: "awsjunk"}
	f := rootCmd.PersistentFlags() // shorthand
	f.StringVarP(&profile, "profile", "p", fromEnv("AWS_PROFILE", "default"), "AWS profile")
	f.StringVarP(&region, "region", "r", fromEnv("AWS_REGION", "eu-west-1"), "AWS region")
	rootCmd.AddCommand(cmdDownloadRDSLogFiles)
	rootCmd.Execute()
}

// Read a variable from the environment. If empty, return defval.
func fromEnv(name, defval string) string {
	s := os.Getenv(name)
	if s == "" {
		return defval
	}
	return s
}
