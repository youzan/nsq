package nsqlookupd_migrate

type Context struct {
	ProxyHttpAddr string  `flag:"http-address" cfg:"http-address"`
	ProxyHttpAddrTest string  `flag:"http-address-test" cfg:"http-address-test"`
	LookupAddrOri string `flag:"origin-lookupd-http" cfg:"origin-lookupd-http"`
	LookupAddrTar string `flag:"target-lookupd-http" cfg:"target-lookupd-http"`
	DccUrl	string		`flag:"dcc-url" cfg:"dcc-url"`
	DccBackupFile string	`flag:"dcc-backup-file" cfg:"dcc-backup-file"`
	Env string		`flag:"env" cfg:"env"`
	LogLevel	int32	`flag:"log-level" cfg:"log-level"`
	LogDir		string	`flag:"log_dir" cfg:"log-dir"`
	DCC_key		string	`flag:"migrate-dcc-key" cfg:"migrate-dcc-key"`
	Logger		*MigrateLogger
	Test            bool	`flag:"test" cfg:"test"`
	TestClientNum   int64    `flag:"test_client_num" cfg:"test-client-num"`
	MCTest		bool 	`flag:"mc_test" cfg:"mc-test"`
}
