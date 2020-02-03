package pddb

import "errors"

// 数据库错误
var (
	// 打开数据库获取文件锁超时错误
	ErrTimeOut = errors.New("timeout")
	// 文件格式与数据库不匹配
	ErrInvalid = errors.New("invalid database")
	// 文件版本与数据库文件版本不匹配
	ErrVersionMismatch = errors.New("version mismatch")
	// 文件校验合错误
	ErrChecksum = errors.New("checksum error")
	// 确保数据库访问时数据库已经打开
	ErrDatabaseNotOpen = errors.New("database not open")
	// 数据库只读错误
	ErrDatabaseReadOnly = errors.New("database read only")
)

// 事务错误
var (
	// 事务已被关闭
	ErrTxClosed = errors.New("transaction closed")
	// 事务不可写
	ErrTxNotWriteable = errors.New("transaction not writeable")
)

// 数据错误
var (
	// bucket name 不可为空
	ErrBucketNameRequired = errors.New("error bucket name required")
	// key不可为空
	ErrKeyRequired = errors.New("error key required")
	// key太长
	ErrKeyTooLarge = errors.New("error key too large")
	// value太长
	ErrValueTooLarge = errors.New("error value too large")
	// 创建bucket时，不能重复创建
	ErrBucketExsits = errors.New("error bucket exsits")
	// ErrIncompatibleValue is returned when trying create or delete a bucket
	// on an existing non-bucket key or when trying to create or delete a
	// non-bucket key on an existing bucket key.
	ErrIncompatibleValue = errors.New("incompatible value")
)
