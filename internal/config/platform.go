package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// PlatformEnv is set to "true" by the chart when it hands igs the
// platform's credentials Secret (platform-credentials) as environment
// variables (REQ-008 fix 11a). Load then fills the stores' settings from
// the platform's variable names.
const PlatformEnv = "IGS_PLATFORM_CREDENTIALS"

// Platform is what the platform tells igs beyond the stores' credentials.
type Platform struct {
	// ContractVersion is the shared contract version the platform runs
	// (CONTRACT_VERSION); igs adopts the contract later (REQ-019).
	ContractVersion string `koanf:"contract_version"`
}

// applyPlatform fills the config from the platform's credential variables.
// A variable that is set wins over the config file and IGS_ variables; one
// that is not leaves its setting as it is. It reports the variables a
// production install needs that are missing, by name only.
func (c *Config) applyPlatform(getenv func(string) string) error {
	get := func(k string) string { return strings.TrimSpace(getenv(k)) }
	var missing []string
	need := func(keys ...string) {
		for _, k := range keys {
			if get(k) == "" {
				missing = append(missing, k)
			}
		}
	}

	// Postgres: the DSN is built from its parts, the password escaped.
	need("POSTGRES_HOST", "POSTGRES_DATABASE", "POSTGRES_USER", "POSTGRES_PASSWORD")
	if host := get("POSTGRES_HOST"); host != "" {
		port := get("POSTGRES_PORT")
		if port == "" {
			port = "5432"
		}
		u := url.URL{Scheme: "postgres", User: url.UserPassword(get("POSTGRES_USER"), getenv("POSTGRES_PASSWORD")),
			Host: net.JoinHostPort(host, port), Path: "/" + get("POSTGRES_DATABASE")}
		if mode := get("POSTGRES_SSLMODE"); mode != "" {
			u.RawQuery = url.Values{"sslmode": {mode}}.Encode()
		}
		c.Postgres.DSN = u.String()
		// The platform's Postgres is reached through a transaction-mode
		// PgBouncer only (docs/master/igs-production.md).
		c.Postgres.Pooled = true
	}
	// The servers' certificates may be signed by the platform's own CA
	// (POSTGRES_CA_CERT, KAFKA_CA_CERT, VALKEY_CA_CERT); an empty value
	// means none was given.
	if v := get("POSTGRES_CA_CERT"); v != "" {
		c.Postgres.CAPEM = v
	}

	// Kafka.
	need("KAFKA_BROKERS")
	if b := get("KAFKA_BROKERS"); b != "" {
		c.Kafka.Brokers = splitList(b)
	}
	if v := get("KAFKA_USER"); v != "" {
		c.Kafka.SASL.Username = v
	}
	if v := getenv("KAFKA_PASSWORD"); v != "" {
		c.Kafka.SASL.Password = v
	}
	if v := get("KAFKA_SASL_MECHANISM"); v != "" {
		c.Kafka.SASL.Mechanism = strings.ToLower(strings.ReplaceAll(v, "_", "-"))
	}
	if v := get("KAFKA_TLS"); v != "" {
		on, err := parseBool(v)
		if err != nil {
			return fmt.Errorf("platform: KAFKA_TLS: %w", err)
		}
		c.Kafka.TLS.Enabled = on
	}
	if v := get("KAFKA_CA_CERT"); v != "" {
		c.Kafka.TLS.CAPEM = v
	}
	if v := get("SCHEMA_REGISTRY_URL"); v != "" {
		c.Kafka.SchemaRegistry.URL = v
	}

	// Valkey: a URL, an address, or a host and port, whichever the
	// platform gives.
	switch {
	case get("VALKEY_URL") != "":
		u, err := url.Parse(get("VALKEY_URL"))
		if err != nil || u.Host == "" {
			return fmt.Errorf("platform: VALKEY_URL is not a redis:// or rediss:// URL")
		}
		c.Redis.Addr = u.Host
		if u.Port() == "" {
			c.Redis.Addr = net.JoinHostPort(u.Hostname(), "6379")
		}
		if p, ok := u.User.Password(); ok {
			c.Redis.Password = p
		}
		c.Redis.TLS.Enabled = u.Scheme == "rediss"
		if db := strings.TrimPrefix(u.Path, "/"); db != "" {
			n, err := strconv.Atoi(db)
			if err != nil {
				return fmt.Errorf("platform: VALKEY_URL's database is not a number")
			}
			c.Redis.DB = n
		}
	case get("VALKEY_ADDR") != "":
		c.Redis.Addr = get("VALKEY_ADDR")
	case get("VALKEY_HOST") != "":
		port := get("VALKEY_PORT")
		if port == "" {
			port = "6379"
		}
		c.Redis.Addr = net.JoinHostPort(get("VALKEY_HOST"), port)
	default:
		missing = append(missing, "VALKEY_HOST (or VALKEY_ADDR, VALKEY_URL)")
	}
	if v := getenv("VALKEY_PASSWORD"); v != "" {
		c.Redis.Password = v
	}
	if v := get("VALKEY_TLS"); v != "" {
		on, err := parseBool(v)
		if err != nil {
			return fmt.Errorf("platform: VALKEY_TLS: %w", err)
		}
		c.Redis.TLS.Enabled = on
	}
	if v := get("VALKEY_CA_CERT"); v != "" {
		c.Redis.TLS.CAPEM = v
	}
	if v := get("VALKEY_DB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("platform: VALKEY_DB is not a number")
		}
		c.Redis.DB = n
	}

	// S3: the endpoint may come as a URL; its scheme says whether to use
	// TLS.
	need("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET_RAW", "S3_BUCKET_MEDIA")
	if e := get("S3_ENDPOINT"); e != "" {
		if u, err := url.Parse(e); err == nil && (u.Scheme == "http" || u.Scheme == "https") {
			c.S3.Endpoint, c.S3.UseSSL = u.Host, u.Scheme == "https"
		} else {
			c.S3.Endpoint = e
		}
	}
	for k, dst := range map[string]*string{"S3_REGION": &c.S3.Region, "S3_ACCESS_KEY": &c.S3.AccessKey,
		"S3_BUCKET_RAW": &c.S3.RawBucket, "S3_BUCKET_MEDIA": &c.S3.MediaBucket, "S3_BUCKET_BACKUPS": &c.S3.BackupsBucket} {
		if v := get(k); v != "" {
			*dst = v
		}
	}
	if v := getenv("S3_SECRET_KEY"); v != "" {
		c.S3.SecretKey = v
	}

	if v := get("CONTRACT_VERSION"); v != "" {
		c.Platform.ContractVersion = v
	}
	if len(missing) > 0 {
		return fmt.Errorf("platform: %s set, but platform-credentials lacks %s", PlatformEnv, strings.Join(missing, ", "))
	}
	return nil
}

// platformOn reports whether the chart asked for the platform's variables.
func platformOn() bool {
	on, _ := parseBool(os.Getenv(PlatformEnv))
	return on
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off", "":
		return false, nil
	}
	return false, fmt.Errorf("%q is not true or false", s)
}
