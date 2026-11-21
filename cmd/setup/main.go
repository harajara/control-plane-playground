// setup creates or tops up .env so a checkout has stable, private local
// values. Existing entries are never rewritten: the database passwords the
// Postgres volumes were initialised with, and the signing and seal keys that
// already-sealed task commands refer to, must stay put. Only missing, empty or
// placeholder values are generated.
package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"

	"tenant-control-plane/internal/secure"
)

// managed lists the keys setup owns, in template order.
var managed = []string{
	"ADMIN_PASSWORD",
	"CP_DB_PASSWORD",
	"TENANT_DB_PASSWORD",
	secure.EnvM2MSecret,
	secure.EnvWorkerSealKey,
	secure.EnvControlPlaneReceiveKey,
}

// sealKeys form a single NaCl keypair and are therefore always written
// together.
var sealKeys = []string{secure.EnvWorkerSealKey, secure.EnvControlPlaneReceiveKey}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	_, err := os.Stat(".env")
	creating := os.IsNotExist(err)
	if err != nil && !creating {
		return err
	}
	var body []byte
	if creating {
		body, err = os.ReadFile(".env.example")
	} else {
		body, err = os.ReadFile(".env")
	}
	if err != nil {
		return err
	}
	sep := "\n"
	if strings.Contains(string(body), "\r\n") {
		sep = "\r\n"
	}
	raw := strings.Split(strings.TrimSuffix(string(body), sep), sep)
	lines := make([]string, len(raw))
	for i, l := range raw {
		// Normalise so a generated value can never carry a stray carriage
		// return into a connection string or a key.
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	want := map[string]bool{}
	at := map[string]int{}
	value := map[string]string{}
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		key, v, ok := strings.Cut(trimmed, "=")
		if !ok || key == "" {
			return fmt.Errorf("malformed .env line %q", line)
		}
		at[key], value[key] = i, v
	}
	for _, key := range managed {
		// A placeholder or an empty value is the only thing setup overwrites;
		// anything else is already the operator's chosen value.
		want[key] = value[key] == "" || strings.HasPrefix(value[key], "GENERATE_")
	}
	// Half a seal keypair is worse than none: URLs would be sealed to a key
	// nobody can open. An unusable pair is regenerated as a pair.
	if !want[sealKeys[0]] && !want[sealKeys[1]] {
		if _, err := secure.NewOpener(value[sealKeys[0]], value[sealKeys[1]]); err != nil {
			fmt.Println("Existing seal keys are not a usable pair; regenerating both.")
			want[sealKeys[0]], want[sealKeys[1]] = true, true
		}
	}
	if want[sealKeys[0]] || want[sealKeys[1]] {
		want[sealKeys[0]], want[sealKeys[1]] = true, true
	}
	filled := []string{}
	for _, key := range managed {
		if want[key] {
			filled = append(filled, key)
		}
	}
	if len(filled) == 0 {
		fmt.Println(".env is complete; nothing changed.")
		return nil
	}
	values, err := generate(want)
	if err != nil {
		return err
	}
	for _, key := range filled {
		if i, ok := at[key]; ok {
			lines[i] = key + "=" + values[key]
		} else {
			lines = append(lines, key+"="+values[key])
		}
	}
	out := strings.Join(lines, sep) + sep
	flags := os.O_WRONLY | os.O_CREATE
	if creating {
		flags |= os.O_EXCL
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(".env", flags, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.WriteString(out); err != nil {
		return err
	}
	if creating {
		fmt.Println("Created .env. Local admin credentials and keys are in that file.")
		return nil
	}
	fmt.Println("Generated into .env:", strings.Join(filled, ", "))
	fmt.Println("Existing values were preserved, so volumes and already-sealed commands stay valid.")
	return nil
}

// generate produces exactly the requested values. The seal keypair is created
// once and shared; two independent keypairs would never open each other's
// messages.
func generate(want map[string]bool) (map[string]string, error) {
	out := map[string]string{}
	for _, key := range managed {
		if !want[key] {
			continue
		}
		switch key {
		case secure.EnvM2MSecret:
			b, err := random(32)
			if err != nil {
				return nil, err
			}
			out[key] = base64.StdEncoding.EncodeToString(b)
		case secure.EnvWorkerSealKey, secure.EnvControlPlaneReceiveKey:
			pair, err := secure.GenerateSealKeyPair()
			if err != nil {
				return nil, err
			}
			out[secure.EnvWorkerSealKey] = pair.WorkerSecret
			out[secure.EnvControlPlaneReceiveKey] = pair.ControlPlaneOpen
		default:
			// Passwords stay hexadecimal: they are interpolated into
			// PostgreSQL URLs and must need no escaping.
			b, err := random(24)
			if err != nil {
				return nil, err
			}
			out[key] = hex.EncodeToString(b)
		}
	}
	return out, nil
}

func random(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}
