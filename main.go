package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gocarina/gocsv"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/config/groupby"
	"github.com/tobgu/qframe/function"
)

var version = "0.0.0"

const SuquadVersion = "2.0"

type CsvRow struct {
	Context      string `csv:"context"`
	Question     string `csv:"question"`
	Answer       string `csv:"answer"`
	AnswerStart  int64  `csv:"answer_start"`
	IsImpossible bool   `csv:"is_impossible"`
}

func (row *CsvRow) copy() *CsvRow {
	return &CsvRow{
		Context:      row.Context,
		Question:     row.Question,
		Answer:       row.Answer,
		AnswerStart:  row.AnswerStart,
		IsImpossible: row.IsImpossible,
	}
}

type Answer struct {
	Text        string `json:"text"`
	AnswerStart int64  `json:"answer_start"`
}

type QnA struct {
	ID               string    `json:"id"`
	Question         string    `json:"question"`
	Answers          []*Answer `json:"answers"`
	PlausibleAnswers []*Answer `json:"plausible_answers,omitempty"`
	IsImpossible     bool      `json:"is_impossible"`
}

func (q *QnA) toCsvRows(row *CsvRow) []*CsvRow {
	row.Question = toASCII(q.Question)
	row.IsImpossible = q.IsImpossible
	var answers []*Answer
	if q.IsImpossible {
		answers = q.PlausibleAnswers
	} else {
		answers = q.Answers
	}
	rows := make([]*CsvRow, len(answers))
	for i, answer := range answers {
		r := row.copy()
		r.Answer = toASCII(answer.Text)
		r.AnswerStart = answer.AnswerStart
		rows[i] = r
	}
	return rows
}

type Paragraph struct {
	Context string `json:"context"`
	Qas     []*QnA `json:"qas"`
}

func (p *Paragraph) toCsvRows() []*CsvRow {
	row := &CsvRow{
		Context: toASCII(p.Context),
	}
	rows := make([]*CsvRow, 0, len(p.Qas)*10)
	for _, qna := range p.Qas {
		r := row.copy()
		rows = append(rows, qna.toCsvRows(r)...)
	}
	return rows
}

type SuQUADData struct {
	Title      string       `json:"title"`
	Paragraphs []*Paragraph `json:"paragraphs"`
}

func (d *SuQUADData) toCsvRows() []*CsvRow {
	rows := make([]*CsvRow, 0, len(d.Paragraphs)*100)
	for _, p := range d.Paragraphs {
		rows = append(rows, p.toCsvRows()...)
	}
	return rows
}

type SuQUAD struct {
	Version string        `json:"version"`
	Data    []*SuQUADData `json:"data"`
}

func (s *SuQUAD) ToCsv(w io.Writer) {
	rows := make([]*CsvRow, 0, len(s.Data))
	for _, d := range s.Data {
		rows = append(rows, d.toCsvRows()...)
	}
	gocsv.Marshal(rows, w)
}

type AnswerGroup struct {
	Answers      []*Answer
	IsImpossible bool
}
type Env struct {
	Title        string
	Answers      map[string]*Answer
	AnswerGroups map[string]*AnswerGroup
	QAs          map[string]*QnA
	QAGroups     map[string][]*QnA
	Paragraphs   []*Paragraph
}

func (o *Env) createAnswer(a *string, b *string) *string {
	if b == nil {
		return nil
	}
	idx, err := strconv.ParseInt(*a, 0, 32)
	if err != nil {
		return nil
	}
	s := Answer{
		Text:        *b,
		AnswerStart: idx,
	}
	buf, err := json.Marshal(&s)
	if err != nil {
		return nil
	}
	id := fmt.Sprintf("%x", md5.Sum(buf))
	o.Answers[id] = &s
	return &id
}

func (o *Env) groupAnswers(answer_ids []*string) *string {
	a := make([]*Answer, 0, len(answer_ids))
	for _, answer_id := range answer_ids {
		if answer_id == nil {
			continue
		}
		if v, ok := o.Answers[*answer_id]; ok {
			a = append(a, v)
		}

	}

	b, err := json.Marshal(&a)
	if err != nil {
		return nil
	}
	id := fmt.Sprintf("%x", md5.Sum(b))
	o.AnswerGroups[id] = &AnswerGroup{Answers: a}
	return &id
}

func (o *Env) addIsImpossible(a *string, b *string) *string {
	group := o.AnswerGroups[*b]
	group.IsImpossible = *a == "true"
	return b
}

func (o *Env) collectQAs(answerID *string, b *string) *string {
	if ag, ok := o.AnswerGroups[*answerID]; ok {
		var s QnA
		if ag.IsImpossible {
			s = QnA{
				ID:               *answerID,
				Question:         *b,
				PlausibleAnswers: ag.Answers,
				Answers:          make([]*Answer, 0),
				IsImpossible:     ag.IsImpossible,
			}
		} else {
			s = QnA{
				ID:           *answerID,
				Question:     *b,
				Answers:      ag.Answers,
				IsImpossible: ag.IsImpossible,
			}
		}
		b, err := json.Marshal(&s)
		if err != nil {
			return nil
		}
		id := fmt.Sprintf("%x", md5.Sum(b))
		o.QAs[id] = &s
		return &id
	}
	return nil
}

func (o *Env) groupQAs(ids []*string) *string {
	a := make([]*QnA, 0, len(ids))
	for _, id := range ids {
		if v, ok := o.QAs[*id]; ok {
			a = append(a, v)
		}
	}
	b, err := json.Marshal(&a)
	if err != nil {
		return nil
	}
	id := fmt.Sprintf("%x", md5.Sum(b))
	o.QAGroups[id] = a
	return &id
}

func (o *Env) makeParagraph(answerID *string, context *string) *string {
	if qas, ok := o.QAGroups[*answerID]; ok {
		s := Paragraph{
			Context: *context,
			Qas:     qas,
		}
		o.Paragraphs = append(o.Paragraphs, &s)
	}
	return nil
}

func (o *Env) ToJSON(w io.Writer) error {
	data := &SuQUADData{
		Title:      o.Title,
		Paragraphs: o.Paragraphs,
	}
	s := SuQUAD{
		Version: "v2.0",
		Data:    []*SuQUADData{data},
	}
	b := new(bytes.Buffer)
	enc := json.NewEncoder(b)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&s); err != nil {
		return err
	}
	_, err := w.Write(b.Bytes())
	return err
}

func NewEnv(title string) *Env {
	return &Env{
		Title:        toASCII(title),
		Answers:      make(map[string]*Answer),
		QAs:          make(map[string]*QnA),
		AnswerGroups: make(map[string]*AnswerGroup),
		QAGroups:     make(map[string][]*QnA),
		Paragraphs:   make([]*Paragraph, 0),
	}
}

func toASCII(s string) string {
	q := strconv.QuoteToASCII(s)
	r := strings.ReplaceAll(q[1:len(q)-1], "\\\"", "\"")
	return r
}

func ASCIIStr(s *string) *string {
	re := regexp.MustCompile(`\\`)
	inp := *s
	idxs := re.FindAllIndex([]byte(inp), -1)
	if len(idxs) == 0 {
		return s
	}
	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(inp)/2)
	i := 0
	for _, idx := range idxs {
		buf = append(buf, inp[i:idx[0]]...)
		var m int
		switch inp[idx[0]+1] {
		case 'x':
			m = 2
		case 'u':
			m = 4
		case 'U':
			m = 8
		default:
			m = 0
		}
		if m == 0 {
			i = idx[0]
		} else {
			c, _, _, err := strconv.UnquoteChar(inp[idx[0]:], '"')
			if err != nil {
				log.Fatal(err.Error())
			}
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
			i = idx[0] + m + 2
		}

	}
	o := toASCII(string(buf))
	return &o
}

func main() {
	var showVersion bool
	var inFilePath string
	var outFilePath string
	var reverseFlag bool
	var title string
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.BoolVar(&reverseFlag, "r", false, "reverse mode flag")
	flag.StringVar(&inFilePath, "i", "", "infile path")
	flag.StringVar(&outFilePath, "o", "", "outfile path")
	flag.StringVar(&title, "t", "", "title field of outfile")
	flag.StringVar(&title, "title", "", "title field of outfile")
	flag.Parse()
	if showVersion {
		fmt.Println("version:", version)
		return
	}
	if inFilePath == "" || (!reverseFlag && title == "") {
		flag.Usage()
		os.Exit(1)
	}
	r, err := os.Open(inFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	if outFilePath == "" {
		if reverseFlag {
			outFilePath = "out.csv"
		} else {
			outFilePath = "out.json"
		}
	}
	w, err := os.Create(outFilePath)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer w.Close()
	if reverseFlag {
		var e SuQUAD
		err = json.NewDecoder(r).Decode(&e)
		if err != nil {
			log.Fatal(err.Error())
		}
		e.ToCsv(w)
	} else {
		env := NewEnv(title)
		qframe.ReadCSV(r).Drop("answer_check").Apply(
			qframe.Instruction{Fn: ASCIIStr, DstCol: "context", SrcCol1: "context"},
			qframe.Instruction{Fn: ASCIIStr, DstCol: "question", SrcCol1: "question"},
			qframe.Instruction{Fn: ASCIIStr, DstCol: "answer", SrcCol1: "answer"},
			qframe.Instruction{Fn: function.StrI, DstCol: "answer_start", SrcCol1: "answer_start"},
		).Drop("frag_text").
			Apply(qframe.Instruction{
				Fn:      env.createAnswer,
				SrcCol1: "answer_start",
				SrcCol2: "answer",
				DstCol:  "answer_id",
			}).Drop("answer", "answer_start").
			GroupBy(groupby.Columns("context", "question", "is_impossible")).
			Aggregate(qframe.Aggregation{
				Fn:     env.groupAnswers,
				Column: "answer_id",
				As:     "answer_id",
			}).
			Apply(qframe.Instruction{
				Fn:      function.StrB,
				SrcCol1: "is_impossible",
				DstCol:  "is_impossible",
			}, qframe.Instruction{
				Fn:      env.addIsImpossible,
				SrcCol1: "is_impossible",
				SrcCol2: "answer_id",
				DstCol:  "answer_id",
			}).
			Sort(qframe.Order{Column: "question"}).
			Apply(qframe.Instruction{
				Fn:      env.collectQAs,
				SrcCol1: "answer_id",
				SrcCol2: "question",
				DstCol:  "answer_id"},
			).
			GroupBy(groupby.Columns("context")).
			Aggregate(qframe.Aggregation{
				Fn:     env.groupQAs,
				Column: "answer_id",
				As:     "answer_id",
			}).Sort(qframe.Order{Column: "context"}).
			Apply(qframe.Instruction{
				Fn:      env.makeParagraph,
				SrcCol1: "answer_id",
				SrcCol2: "context",
				DstCol:  "dummy"},
			)
		if err := env.ToJSON(w); err != nil {
			log.Fatal(err)
		}
	}
}
