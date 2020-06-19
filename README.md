# WI-FI 5GHz DATA VISUALIZER

This repository is to load the .mat files from the dataset into CSV or JSON files.

To run this scripts, `go` must be installed.

[Install Go](https://golang.org/doc/install)

``` cli
> cd path-to-project/
> go run ./main.go
```

In the `main.go` file, in the `main` function, must be set the path to the JSON files, and and ratio reduction

```go
func main() {
	transferJSON("path\\to\\json-files", 100)
}

```

Link to the [app](tfg.naulacambra.com)
