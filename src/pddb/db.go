package pddb

import (
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// 数据库默认值
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	DefaultAllocSize         = 10 * 1024 * 1024
)

// 魔法数, 用来标识pddb文件
const magic uint32 = 0xEC0CDAED

// 数据库文件格式版本
const version = 2

// 数据库允许的最大大小
const maxMapSize = 0x7FFFFFFF // 2GB

// 数据库映射过程中, 最大允许步长
const maxMmapStep = 1 << 30 // 1GB

const maxAllocSize = 0xFFFFFFF

// 数据库对象
type DB struct {
	// 单个batch的大小上限, 如果<=0, 禁用batch
	MaxBatchSize int
	// 在batch开始前的最大延时, 如果<=0, 禁用batch
	MaxBatchDelay time.Duration
	// 当数据库需要创建新页的时候分配的空间
	AllocSize int

	// 数据库路径
	path string
	// 数据库文件指针
	file *os.File
	// 数据库是否已打开
	opened bool
	// 页大小, 受操作系统控制
	pageSize int
	// 缓存申请但未使用的item用于之后的重用，以减轻GC的压力
	pagePool sync.Pool
	dataref  []byte
	data     *[maxMapSize]byte
	datasz   int
	filesz   int
	meta0    *meta
	meta1    *meta
	freelist *freelist
	rwtx     *Tx
	txs      []*Tx
	stats    Stats

	// 用于保护数据库重新映射的过程
	mmaplock sync.RWMutex
	// 保护元数据写过程
	metalock sync.Mutex
	// 保护数据库状态
	statlock sync.RWMutex
	// 保护数据写过程
	rwlock sync.Mutex

	// 数据库只读选项
	readOnly bool
}

// 开启一个新事务
// 只允许一个写事务
func (db *DB) Begin(writeable bool) (*Tx, error) {
	if writeable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) Path() string {
	return db.path
}

// 关闭数据库, 所有的资源引用必须释放
func (db *DB) Close() error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	return db.close()
}

// 开始一个只读事务
func (db *DB) beginTx() (*Tx, error) {
	// 注意加锁顺序
	db.metalock.Lock()
	db.mmaplock.RLock()
	// 检查数据库是否已经打开
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}
	// 创建与数据库关联的事务
	t := &Tx{}
	t.init(db)

	// 记录数据库开启的事务
	db.txs = append(db.txs, t)
	n := len(db.txs)

	// 释放元数据锁
	db.metalock.Unlock()

	// 更新数据库状态
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

// 开始一个读写事务
func (db *DB) beginRWTx() (*Tx, error) {
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	db.rwlock.Lock()
	db.metalock.Lock()
	defer db.metalock.Unlock()

	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// 创建事务
	t := &Tx{writeable: true}
	t.init(db)
	db.rwtx = t

	// TODO: 释放Freelist

	return t, nil
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}
	db.opened = false
	db.freelist = nil
	if err := db.munmap(); err != nil {
		return err
	}
	// 释放文件引用
	if db.file != nil {
		if !db.readOnly {
			err := funlock(db)
			if err != nil {
				log.Printf("Database close; funlock error %s", err)
			}
		}
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close error: %s", err)
		}
		db.file = nil
	}
	db.path = ""

	return nil
}

// 初始化数据库, 主要是初始化meta page
func (db *DB) init() error {
	// 使用操作系统的内存页大小作为数据库的页大小
	db.pageSize = os.Getpagesize()

	// 创建2个meta page
	buf := make([]byte, 4*db.pageSize)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// TODO 初始化meta data
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// 创建空freelistPage
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// 创建空dataPage
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// 数据写入文件
	if _, err := db.file.WriteAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

// 根据当前的pageSize返回对于page的引用
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// 将数据库文件映射到内存
// Param minsz: 新mmap允许的最小大小
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// 至少需要确保最小大小
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// TODO: 取消映射前先取消引用

	// 取消映射
	if err := db.munmap(); err != nil {
		return err
	}

	// 将文件映射到内存
	if err := mmap(db, size); err != nil {
		return err
	}

	// 数据库元数据的引用
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// 两份元数据都无法通过验证才报错，因为两份元数据互为备份，可以用于备份
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// 取消数据库文件的内存映射
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("munmap error: %s", err)
	}
	return nil
}

// 数据库文件的最小大小为32KB, 每次将大小翻倍, 直至1GB
// 如果大小超过系统允许的最大内存, 则报错
func (db *DB) mmapSize(size int) (int, error) {
	// 从32KB开始翻倍
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// 不得超过最大允许大小
	if size < maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// 如果大小超过1GB, 每次以1GB为步长
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// TODO 继续处理mmap的大小
	return int(sz), nil
}

// 获取数据库的任意页
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// 数据库的元数据页, 返回事务id较大且通过验证的元数据
func (db *DB) meta() *meta {
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	panic("DB.meta: invalid meta pages")
}

func (db *DB) allocate(count int) (*page, error) {
	// 分配临时buffer
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// 如果freelist中有可用空间, 直接从freelist中取
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// 如果内存不够, 重新申请内存
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}
	// pgid移动到高水位
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// 将数据库大小增长到指定大小
func (db *DB) grow(sz int) error {
	if sz <= db.filesz {
		return nil
	}
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}
	db.filesz = sz
	return nil
}

// TODO
func (db *DB) removeTx(tx *Tx) {
	return
}

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     bucket
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

// 生成meta数据的校验和
func (m *meta) sum64() uint64 {
	h := fnv.New64()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// validate校验标志位确保文件格式匹配数据库要求的文件格式
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}

	return nil
}

// 元数据地址复制
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// 将元数据写入到page
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// page id可以被事务id确定
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	m.checksum = m.sum64()
	m.copy(p.meta())
}

// 打开数据库的可选项
type Options struct {
	// 获取文件锁的超时时间
	Timeout time.Duration
	// 只读模式打开数据库
	ReadOnly bool
	// 数据库内存映射的初始大小, <=0时无效
	InitialMmapSize int
}

var DefaultOptions = &Options{
	Timeout: 0,
}

type Stats struct {
	// Freelist 状态
	FreePageN     int
	PendingPageN  int
	FreeAlloc     int
	FreelistInuse int

	// 事务状态
	TxN     int
	OpenTxN int
}

// flock获取文件描述符的锁
func flock(db *DB, mode os.FileMode, exclusive bool, timeout time.Duration) error {
	var t time.Time
	for {
		// 超时
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeOut
		}

		// 尝试占锁
		flag := syscall.LOCK_SH
		if exclusive {
			flag = syscall.LOCK_EX
		}
		err := syscall.Flock(int(db.file.Fd()), flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		// 等待锁释放
		time.Sleep(50 * time.Millisecond)
	}
}

// funlock释放文件描述符的锁
func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

// fdatasync将数据写入文件描述符
func fdatasync(db *DB) error {
	return db.file.Sync()
}

// mmap将数据库文件映射到内存
func mmap(db *DB, sz int) error {
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	// 与内核交互, 采用随机访问模式
	// if err := syscall.Madvise(b, syscall.MADV_RANDOM); err != nil {
	// 	return fmt.Errorf("madvise error: %s", err)
	// }
	// 数据库对于内存的引用
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

// munmap取消数据库文件的内存映射
func munmap(db *DB) error {
	if db.dataref == nil {
		return nil
	}
	err := syscall.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}

func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	// 用户没有指定选项使用默认用户选项
	if options == nil {
		options = DefaultOptions
	}

	// 数据库操作的默认值
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// 创建数据库文件
	db.path = path
	var err error
	if db.file, err = os.OpenFile(path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// 给数据库加锁避免写冲突
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// 数据库不存在则进行初始化
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// 读取第一个meta page获取pageSize
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// 初始化pagePool
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// 数据库文件映射到内存
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// 读取freelist
	db.freelist = newFreelist()
	// TODO
	return db, nil
}
