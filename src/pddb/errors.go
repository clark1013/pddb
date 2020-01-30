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
)
