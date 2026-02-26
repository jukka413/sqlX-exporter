package main

import (
	_ "github.com/go-sql-driver/mysql"  // registers "mysql"
	_ "github.com/jackc/pgx/v5/stdlib"  // registers "pgx"
	_ "github.com/microsoft/go-mssqldb" // registers "sqlserver"
	_ "github.com/sijms/go-ora/v2"      // registers "oracle"
)
