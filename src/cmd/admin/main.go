// Command admin creates users and stores and stores both lists in log.json.
//
// The default path is log.json in the working directory. Run it from the
// repository root to write that file there. --file selects another path.
//
//	go run ./src/cmd/admin user add --name Ada
//	go run ./src/cmd/admin store add --name Central
//	go run ./src/cmd/admin list
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

const usageText = `Usage:
  admin [--file path] list
  admin [--file path] user add --name <name>
  admin [--file path] store add --name <name>

--file is the JSON log (default: log.json in the working directory).
With no command, admin prints the users and stores already in that file.
`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, errHelp) {
			fmt.Fprint(os.Stderr, usageText)
			return
		}
		fmt.Fprintf(os.Stderr, "admin: %s\n", err)
		if errors.Is(err, errUsage) {
			fmt.Fprint(os.Stderr, usageText)
		}
		os.Exit(1)
	}
}

// errUsage marks a bad invocation. main prints usage after the message.
var errUsage = errors.New("usage")

// errHelp marks -h. main prints usage and exits zero.
var errHelp = errors.New("help")

type usageError struct {
	err error
}

func (e *usageError) Error() string { return e.err.Error() }

func (e *usageError) Unwrap() error { return errUsage }

func usageErr(err error) error {
	return &usageError{err: err}
}

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("admin", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	file := fs.String("file", "log.json", "path of the JSON log")
	fs.Usage = func() {}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return errHelp
		}
		return usageErr(err)
	}

	rest := fs.Args()
	if len(rest) == 0 || rest[0] == "list" {
		if len(rest) > 1 {
			return usageErr(fmt.Errorf("unexpected argument %q", rest[1]))
		}
		return printLog(*file, stdout)
	}
	if rest[0] != "user" && rest[0] != "store" {
		return usageErr(fmt.Errorf("unknown command %q", rest[0]))
	}
	if len(rest) == 1 || (len(rest) == 2 && rest[1] == "list") {
		return printLog(*file, stdout)
	}
	if rest[1] != "add" {
		return usageErr(fmt.Errorf("unknown %s action %q", rest[0], rest[1]))
	}

	name, err := parseName(rest[0], rest[2:])
	if err != nil {
		return err
	}
	rec, err := add(*file, rest[0], name, time.Now())
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "created %s %s %s\n", rest[0], rec.ID, rec.Name)
	return nil
}

func parseName(kind string, args []string) (string, error) {
	fs := flag.NewFlagSet(kind+" add", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	name := fs.String("name", "", "name of the "+kind)
	fs.Usage = func() {}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", errHelp
		}
		return "", usageErr(err)
	}
	if fs.NArg() != 0 {
		return "", usageErr(fmt.Errorf("unexpected argument %q", fs.Arg(0)))
	}
	if *name == "" {
		return "", usageErr(fmt.Errorf("%s add: --name is required", kind))
	}
	return *name, nil
}

func printLog(path string, stdout io.Writer) error {
	data, err := load(path)
	if err != nil {
		return err
	}
	fmt.Fprint(stdout, formatLog(data))
	return nil
}
