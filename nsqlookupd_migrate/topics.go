package nsqlookupd_migrate

type Topics_old struct {
	StatusCode int	`json:"status_code"`
	StatusTxt  string	`json:"status_txt"`
	Data	*Topics	`json:"data"`
}

type Topics struct {
	Topics []string `json:"topics"`
}

