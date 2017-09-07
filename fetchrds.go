package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/urfave/cli"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	AUTHOR             = "katsew"
	APP_VERSION        = "0.1.0"
	DEFAULT_AWS_REGION = "ap-northeast-1"
)

var service *rds.RDS
var outPath string

func worker(targetDbName string, files []*rds.DescribeDBLogFilesDetails) <-chan bool {

	receiver := make(chan bool, 5)

	var wg sync.WaitGroup

	go func() {

		for _, logFileInfo := range files {

			wg.Add(1)

			filename := *logFileInfo.LogFileName

			fmt.Printf("Log filename: %s\n", filename)

			time.Sleep(500 * time.Millisecond)
			go func(logFileName string) {

				out, err := service.DownloadDBLogFilePortion(&rds.DownloadDBLogFilePortionInput{
					DBInstanceIdentifier: aws.String(targetDbName),
					LogFileName:          aws.String(logFileName),
				})
				if err != nil {
					panic(err)
				}

				var data string
				if out.LogFileData != nil {
					data = *out.LogFileData
				} else {
					data = ""
				}

				fmt.Print("Download log file succeed, continue writing to the file...\n")

				fileInfo := strings.Split(logFileName, "/")
				dest := fmt.Sprintf("%s/%s/%s/", outPath, targetDbName, fileInfo[0])
				fmt.Printf("Destination: %s\n", dest)
				if err = os.MkdirAll(dest, os.ModePerm); err != nil {
					fmt.Println(err)
				}

				f, err := os.Create(dest + fileInfo[1])
				if err != nil {
					fmt.Println(err)
				}
				defer f.Close()

				f.WriteString(data)

				receiver <- false
				wg.Done()
			}(filename)

		}

		wg.Wait()
		close(receiver)

	}()

	return receiver
}

func main() {

	cli.AppHelpTemplate = `NAME:
	   {{.Name}} - {{.Usage}}
	USAGE:
	   {{.HelpName}} {{if .VisibleFlags}}[options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}
	   {{if len .Authors}}
	AUTHOR:
	   {{range .Authors}}{{ . }}{{end}}
	   {{end}}{{if .Commands}}
	OPTIONS:
	   {{range .VisibleFlags}}{{.}}
	   {{end}}{{end}}{{if .Copyright }}
	COPYRIGHT:
	   {{.Copyright}}
	   {{end}}{{if .Version}}
	VERSION:
	   {{.Version}}
	   {{end}}
	`

	maxProcs := runtime.NumCPU() - 1
	runtime.GOMAXPROCS(maxProcs)
	app := cli.NewApp()

	app.Author = AUTHOR
	app.Name = "fetchrds"
	app.Usage = "Download AWS RDS error/slow logs."
	app.ArgsUsage = "[target_rds_name]"
	app.Version = APP_VERSION
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "out, o",
			Value: ".",
			Usage: "Output destination",
		},
		cli.StringFlag{
			Name:  "region, r",
			Value: DEFAULT_AWS_REGION,
			Usage: "Target region",
		},
	}

	app.Action = func(ctx *cli.Context) error {

		targetRds := ctx.Args().First()
		fmt.Printf("Target RDS Name: %s \n", ctx.Args().First())

		outPath = ctx.String("out")
		fmt.Printf("Output path: %s\n", outPath)

		region := ctx.String("region")
		fmt.Printf("Target region: %s\n", region)

		if targetRds == "" {
			return cli.NewExitError("Target RDS name must be specified!", 400)
		}

		sess, err := session.NewSession()
		if err != nil {
			return cli.NewExitError(err, 401)
		}
		config := &aws.Config{
			Region: aws.String(region),
		}
		service = rds.New(sess, config)

		data, err := service.DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{
			DBInstanceIdentifier: aws.String(targetRds),
		})
		if err != nil {
			return cli.NewExitError(err, 402)
		}

		receiver := worker(targetRds, data.DescribeDBLogFiles)
		for {
			_, ok := <-receiver
			if !ok {
				fmt.Print("Complete!")
				break
			}

		}
		return nil
	}

	app.Run(os.Args)
}
