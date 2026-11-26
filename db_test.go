package main

import (
	"net/url"
	"strings"
	"testing"
)

func parseDSN(t *testing.T, dsn string) *url.URL {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("итоговый DSN не разбирается как URL: %v\nDSN: %s", err, dsn)
	}
	return u
}

func TestBuildOracleDSN_Standard(t *testing.T) {
	// Пароль "ti$ger!" в исходном URL закодирован, как это делает expandURLs.
	dsn, err := buildOracleDSN("oracle://scott:ti%24ger%21@dbhost:1522/ORCLPDB1")
	if err != nil {
		t.Fatal(err)
	}
	u := parseDSN(t, dsn)

	if u.Scheme != "oracle" {
		t.Errorf("scheme = %q", u.Scheme)
	}
	if got := u.User.Username(); got != "scott" {
		t.Errorf("user = %q", got)
	}
	if got, _ := u.User.Password(); got != "ti$ger!" {
		t.Errorf("пароль исказился по дороге: %q, ожидали ti$ger!", got)
	}
	if u.Host != "dbhost:1522" {
		t.Errorf("host = %q", u.Host)
	}
	if got := strings.TrimPrefix(u.Path, "/"); got != "ORCLPDB1" {
		t.Errorf("service = %q", got)
	}
	if got := u.Query().Get("client charset"); got != "UTF8" {
		t.Errorf("client charset = %q, ожидали UTF8 по умолчанию", got)
	}
}

func TestBuildOracleDSN_DefaultPort(t *testing.T) {
	dsn, err := buildOracleDSN("oracle://u:p@dbhost/svc")
	if err != nil {
		t.Fatal(err)
	}
	if got := parseDSN(t, dsn).Host; got != "dbhost:1521" {
		t.Errorf("host = %q, ожидали порт по умолчанию 1521", got)
	}
}

func TestBuildOracleDSN_KeepsOptions(t *testing.T) {
	t.Run("свои параметры сохраняются", func(t *testing.T) {
		dsn, err := buildOracleDSN("oracle://u:p@h:1521/svc?SSL=true")
		if err != nil {
			t.Fatal(err)
		}
		q := parseDSN(t, dsn).Query()
		if q.Get("SSL") != "true" {
			t.Errorf("SSL = %q, параметр потерян", q.Get("SSL"))
		}
		if q.Get("client charset") != "UTF8" {
			t.Errorf("client charset = %q", q.Get("client charset"))
		}
	})

	t.Run("явный client charset не перезаписывается", func(t *testing.T) {
		dsn, err := buildOracleDSN("oracle://u:p@h:1521/svc?client%20charset=CL8MSWIN1251")
		if err != nil {
			t.Fatal(err)
		}
		if got := parseDSN(t, dsn).Query().Get("client charset"); got != "CL8MSWIN1251" {
			t.Errorf("client charset = %q, ожидали значение пользователя", got)
		}
	})
}

func TestBuildOracleDSN_Errors(t *testing.T) {
	for _, in := range []string{
		"oracle://u:p@h:notaport/svc",
		"oracle://u:p@h:99999999999999999999/svc",
	} {
		if _, err := buildOracleDSN(in); err == nil {
			t.Errorf("buildOracleDSN(%q) должен вернуть ошибку", in)
		}
	}
}

const tnsDesc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=dbhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=svc)))"

// Регрессия: пароль в TNS-пути приходит закодированным (expandURLs) и должен
// быть раскодирован, иначе Oracle отвечает ORA-01017.
func TestBuildOracleDSN_TNSDecodesCredentials(t *testing.T) {
	dsn, err := buildOracleDSN("oracle+tns://sc%40tt:my%24pass%21word%3A1@" + tnsDesc)
	if err != nil {
		t.Fatal(err)
	}
	u := parseDSN(t, dsn)

	if got := u.User.Username(); got != "sc@tt" {
		t.Errorf("user = %q, ожидали sc@tt", got)
	}
	if got, _ := u.User.Password(); got != "my$pass!word:1" {
		t.Errorf("пароль = %q, ожидали my$pass!word:1 (раскодированный)", got)
	}
	if got := u.Query().Get("connStr"); got != tnsDesc {
		t.Errorf("connStr = %q, ожидали исходный дескриптор", got)
	}
	if got := u.Query().Get("client charset"); got != "UTF8" {
		t.Errorf("client charset = %q", got)
	}
}

func TestBuildOracleDSN_TNSVariants(t *testing.T) {
	tests := []struct{ name, suffix string }{
		{"сразу после @", tnsDesc},
		{"/?CONNSTR=", "/?CONNSTR=" + tnsDesc},
		{"/?", "/?" + tnsDesc},
		{"/", "/" + tnsDesc},
		{"пробелы внутри дескриптора удаляются",
			"(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = dbhost)(PORT = 1521))(CONNECT_DATA = (SERVICE_NAME = svc)))"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, err := buildOracleDSN("oracle+tns://u:p@" + tt.suffix)
			if err != nil {
				t.Fatal(err)
			}
			if got := parseDSN(t, dsn).Query().Get("connStr"); got != tnsDesc {
				t.Errorf("connStr = %q, ожидали %q", got, tnsDesc)
			}
		})
	}
}

func TestBuildOracleDSN_TNSErrors(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"нет @", "oracle+tns://userpass", "must contain @"},
		{"битая кодировка в пароле", "oracle+tns://u:%zz@" + tnsDesc, "invalid encoding in password"},
		{"битая кодировка в имени", "oracle+tns://%zz:p@" + tnsDesc, "invalid encoding in username"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildOracleDSN(tt.in)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ожидали ошибку с %q, получили: %v", tt.want, err)
			}
		})
	}
}
