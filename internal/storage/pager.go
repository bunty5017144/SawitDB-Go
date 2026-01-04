package storage

import (
	"encoding/binary"
	"errors"
	"os"
)

const (
	PAGE_SIZE = 4096
	MAGIC     = "WOWO"
)

// Pager handles 4KB page I/O
type Pager struct {
	FilePath string
	file     *os.File
}

func NewPager(filePath string) (*Pager, error) {
	p := &Pager{
		FilePath: filePath,
	}
	if err := p.open(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Pager) open() error {
	if _, err := os.Stat(p.FilePath); os.IsNotExist(err) {
		f, err := os.OpenFile(p.FilePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		p.file = f
		return p.initNewFile()
	}

	f, err := os.OpenFile(p.FilePath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	p.file = f
	return nil
}

func (p *Pager) initNewFile() error {
	buf := make([]byte, PAGE_SIZE)
	copy(buf[0:], MAGIC)
	binary.LittleEndian.PutUint32(buf[4:], 1) // Total Pages = 1
	binary.LittleEndian.PutUint32(buf[8:], 0) // Num Tables = 0

	_, err := p.file.WriteAt(buf, 0)
	return err
}

func (p *Pager) ReadPage(pageId uint32) ([]byte, error) {
	buf := make([]byte, PAGE_SIZE)
	offset := int64(pageId) * PAGE_SIZE
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (p *Pager) WritePage(pageId uint32, buf []byte) error {
	if len(buf) != PAGE_SIZE {
		return errors.New("buffer must be 4KB")
	}
	offset := int64(pageId) * PAGE_SIZE
	_, err := p.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	return p.file.Sync() // STABILITY UPGRADE equivalent
}

func (p *Pager) AllocPage() (uint32, error) {
	page0, err := p.ReadPage(0)
	if err != nil {
		return 0, err
	}

	totalPages := binary.LittleEndian.Uint32(page0[4:])
	newPageId := totalPages
	newTotal := totalPages + 1

	binary.LittleEndian.PutUint32(page0[4:], newTotal)
	if err := p.WritePage(0, page0); err != nil {
		return 0, err
	}

	newPage := make([]byte, PAGE_SIZE)
	// Go zeros memory by default, but to be explicit like JS:
	binary.LittleEndian.PutUint32(newPage[0:], 0) // Next Page = 0
	binary.LittleEndian.PutUint16(newPage[4:], 0) // Count = 0
	binary.LittleEndian.PutUint16(newPage[6:], 8) // Free Offset = 8

	if err := p.WritePage(newPageId, newPage); err != nil {
		return 0, err
	}

	return newPageId, nil
}

func GetPageSize() int {
	return PAGE_SIZE
}

func (p *Pager) Close() error {
	if p.file != nil {
		return p.file.Close()
	}
	return nil
}
