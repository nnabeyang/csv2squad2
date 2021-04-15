package main

import (
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/tobgu/qframe"
	"github.com/tobgu/qframe/config/groupby"
	"github.com/tobgu/qframe/function"
)

var version = "0.0.0"

const SuquadVersion = "2.0"

type Answer struct {
	Text        string `json:"text"`
	AnswerStart int64  `json:"answer_start"`
}
type QnA struct {
	ID           string    `json:"id"`
	Question     string    `json:"question"`
	Answers      []*Answer `json:"answers"`
	IsImpossible bool      `json:"is_impossible"`
}
type Paragraph struct {
	Context string `json:"context"`
	Qas     []*QnA `json:"qas"`
}
type SuQUADData struct {
	Title      string       `json:"title"`
	Paragraphs []*Paragraph `json:"paragraphs"`
}
type SuQUAD struct {
	Version string       `json:"version"`
	Data    []SuQUADData `json:"data"`
}
type Env struct {
	Title        string
	Answers      map[string]*Answer
	AnswerGroups map[string][]*Answer
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
	o.AnswerGroups[id] = a
	return &id
}

func (o *Env) collectQAs(answerID *string, b *string) *string {
	if as, ok := o.AnswerGroups[*answerID]; ok {
		s := QnA{
			ID:       *answerID,
			Question: *b,
			Answers:  as,
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
	data := SuQUADData{
		Title:      o.Title,
		Paragraphs: o.Paragraphs,
	}
	s := SuQUAD{
		Version: "v2.0",
		Data:    []SuQUADData{data},
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(&s)
}

func NewEnv(title string) *Env {
	return &Env{
		Title:        title,
		Answers:      make(map[string]*Answer),
		QAs:          make(map[string]*QnA),
		AnswerGroups: make(map[string][]*Answer),
		QAGroups:     make(map[string][]*QnA),
		Paragraphs:   make([]*Paragraph, 0),
	}
}
func main() {
	var showVersion bool
	var inFilePath string
	var outFilePath string
	var title string
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.StringVar(&inFilePath, "i", "", "infile path")
	flag.StringVar(&outFilePath, "o", "out.json", "outfile path")
	flag.StringVar(&title, "t", "", "title field of outfile")
	flag.StringVar(&title, "title", "", "title field of outfile")
	flag.Parse()
	if showVersion {
		fmt.Println("version:", version)
		return
	}
	if inFilePath == "" || title == "" {
		flag.Usage()
		os.Exit(1)
	}
	r, err := os.Open(inFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	env := NewEnv(title)
	qframe.ReadCSV(r).Drop("answer_check").Apply(
		qframe.Instruction{Fn: function.StrI, DstCol: "answer_start", SrcCol1: "answer_start"},
	).Drop("frag_text").
		Apply(qframe.Instruction{
			Fn:      env.createAnswer,
			SrcCol1: "answer_start",
			SrcCol2: "answer",
			DstCol:  "answer_id",
		}).Drop("answer", "answer_start").
		GroupBy(groupby.Columns("context", "question")).
		Aggregate(qframe.Aggregation{
			Fn:     env.groupAnswers,
			Column: "answer_id",
			As:     "answer_id",
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
	w, err := os.Create(outFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	if err := env.ToJSON(w); err != nil {
		log.Fatal(err)
	}
}
