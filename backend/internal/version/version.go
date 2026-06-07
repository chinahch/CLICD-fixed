package version

var (
	Version = "1.1.0"
	Repo    = "chinahch/CLICD-fixed"
)

func Current() string {
	if Version == "" {
		return "dev"
	}
	return Version
}

