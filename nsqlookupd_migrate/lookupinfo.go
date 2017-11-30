package nsqlookupd_migrate


type Lookupinfo_old struct {
	Status_code	int	`json:"status_code"`
	Status_txt	string	`json:"status_txt"`
	Data		*Data	`json:"data,omitempty"`
}

type Data struct {
	Channels	[]string	`json:"channels"`
	Producers	[]*Producerinfo	`json:"producers"`
	Partitions	map[string]*Producerinfo	`json:"partitions,omitempty"`
	Meta		*Metainfo	`json:"meta,omitempty"`
}

type Producerinfo struct {
	Remote_address	string	`json:"remote_address"`
	Hostname	string	`json:"hostname"`
	Broadcast_address	string	`json:"broadcast_address"`
	Tcp_port	int	`json:"tcp_port"`
	Http_port	int	`json:"http_port"`
	Version		string	`json:"version"`
}

type Metainfo struct {
	Partition_num	int	`json:"partition_num"`
	Replica		int	`json:"replica"`
	ExtSupport      bool    `json:"extend_support"`
}

type Lookupinfo struct {
	Channels	[]string	`json:"channels,omitempty"`
	Producers	[]*Producerinfo	`json:"producers,omitempty"`
	Partitions	map[string]*Producerinfo	`json:"partitions,omitempty"`
	Meta		*Metainfo	`json:"meta,omitempty"`
	Message		string		`json:"message,omitempty"`
}

const (
	M_OFF = iota
	M_CSR
	M_CSR_PDR
	M_FIN
)

