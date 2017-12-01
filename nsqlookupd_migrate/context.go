package nsqlookupd_migrate

type Context struct {
	ProxyHttpAddr     string  `flag:"http-address" cfg:"http-address"`
	ProxyHttpAddrTest string  `flag:"http-address-test" cfg:"http-address-test"`
	LookupAddrOri     string `flag:"origin-lookupd-http" cfg:"origin-lookupd-http"`
	LookupAddrTar     string `flag:"target-lookupd-http" cfg:"target-lookupd-http"`
	Env               string		`flag:"env" cfg:"env"`
	LogLevel          int32	`flag:"log-level" cfg:"log-level"`
	LogDir            string	`flag:"log_dir" cfg:"log-dir"`
	Migrate_key       string	`flag:"migrate-key" cfg:"migrate-key"`
	Logger            *MigrateLogger
}
