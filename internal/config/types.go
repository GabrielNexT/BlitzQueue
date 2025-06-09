package config

type DatastoreType string

const (
	PostgresType DatastoreType = "postgres"
)

var dataStoreType = map[string]DatastoreType{
	"postgres": PostgresType,
}

type DatastoreConnection struct {
	Type     DatastoreType `mapstructure:"type"`
	Host     string        `mapstructure:"host"`
	Port     int           `mapstructure:"port"`
	User     string        `mapstructure:"user"`
	Password string        `mapstructure:"password"`
	Database string        `mapstructure:"database"`
}
