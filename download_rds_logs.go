package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/spf13/cobra"
)

var cmdDownloadRDSLogFiles = &cobra.Command{
	Use:   "downloadRDSLogFiles -db=<instanceName> [flags]",
	Short: "Download RDS log files",
}

func init() {
	f := cmdDownloadRDSLogFiles.Flags()
	dbInstance := f.String("db", "", "DB instance name (required)")
	since := f.String("since", "", "fetch files modified after this date")
	filenameContains := f.String("contains", "", "limit to filenames containing this string")
	outputDir := f.StringP("output", "o", "", "write log files to this directory")

	cmdDownloadRDSLogFiles.Run = func(cmd *cobra.Command, args []string) {
		if *dbInstance == "" {
			fmt.Println("error: --db is required")
			os.Exit(1)
		}

		var fileLastWritten *int64
		if *since != "" {
			t, err := time.Parse(time.RFC3339, *since)
			if err != nil {
				log.Fatalf("usage error: %s", err)
			}
			n := t.Unix() * 1000
			fileLastWritten = &n
		}

		// Create an AWS session.
		sess, err := session.NewSessionWithOptions(session.Options{
			Config:  aws.Config{Region: aws.String(region)},
			Profile: profile,
		})
		if err != nil {
			log.Fatalf("session.NewSession: %s", err)
		}

		// Create an RDS client.
		rc := rds.New(sess)
		out, err := rc.DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{
			DBInstanceIdentifier: dbInstance,
			FilenameContains:     filenameContains,
			FileLastWritten:      fileLastWritten,
		})
		if err != nil {
			log.Fatalf("DescribeDBLogFiles: %s", err)
		}
		for _, inst := range out.DescribeDBLogFiles {
			logFileName := aws.StringValue(inst.LogFileName)
			dst := path.Join(*outputDir, logFileName)
			if dir, _ := path.Split(dst); dir != "" {
				if err := os.MkdirAll(dir, 0755); err != nil {
					log.Fatalf("os.MkdirAll(%q): %s", dir, err)
				}
			}
			fp, err := os.Create(dst)
			if err != nil {
				log.Fatalf("os.Open(%q): %s", dst, err)
			}
			n, err := downloadDBLogFile(fp, rc, *dbInstance, logFileName)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("wrote %s (%d bytes)", dst, n)
		}
	}
}

func downloadDBLogFile(w io.Writer, rc *rds.RDS, dbInstanceID, logFileName string) (bytesWritten int, reterr error) {
	req := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(dbInstanceID),
		LogFileName:          aws.String(logFileName),
		Marker:               aws.String("0"),
		NumberOfLines:        aws.Int64(10000),
	}
	for {
		// Download the next portion of the file.
		res, err := rc.DownloadDBLogFilePortion(req)
		if err != nil {
			reterr = err
			return
		}
		// Write the log data to w, if any.
		if res.LogFileData != nil && *res.LogFileData != "" {
			n, err := w.Write([]byte(aws.StringValue(res.LogFileData)))
			if err != nil {
				reterr = err
				return
			}
			bytesWritten += n
		}
		// Return if we've reached the end of the file.
		if !*res.AdditionalDataPending {
			return
		}
		// Advance to the next marker.
		req.Marker = aws.String(*res.Marker)
	}
}
