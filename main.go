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
	"strconv"
	"strings"

	"github.com/google/uuid"

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
	ID           string `csv:"id"`
	Title        string `csv:"title"`
}

func (row *CsvRow) copy() *CsvRow {
	return &CsvRow{
		Context:      row.Context,
		Question:     row.Question,
		Answer:       row.Answer,
		AnswerStart:  row.AnswerStart,
		IsImpossible: row.IsImpossible,
		ID:           row.ID,
		Title:        row.Title,
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
	PlausibleAnswers []*Answer `json:"-"`
	IsImpossible     bool      `json:"is_impossible"`
}

func (q *QnA) MarshalJSON() ([]byte, error) {
	type Alias QnA

	if q.IsImpossible {
		return json.Marshal(&struct {
			*Alias
			AliasPlausibleAnswers []*Answer `json:"plausible_answers"`
		}{
			Alias:                 (*Alias)(q),
			AliasPlausibleAnswers: q.PlausibleAnswers,
		})
	} else {
		return json.Marshal((*Alias)(q))
	}
}

func (q *QnA) UnmarshalJSON(b []byte) error {
	type Alias QnA

	aux := &struct {
		*Alias
		AliasPlausibleAnswers []*Answer `json:"plausible_answers"`
	}{
		Alias: (*Alias)(q),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	q.PlausibleAnswers = aux.AliasPlausibleAnswers
	return nil
}
func (q *QnA) toCsvRows(row *CsvRow) []*CsvRow {
	row.Question = q.Question
	row.IsImpossible = q.IsImpossible
	row.ID = q.ID
	var answers []*Answer
	if q.IsImpossible {
		answers = q.PlausibleAnswers
	} else {
		answers = q.Answers
	}
	if len(answers) == 0 {
		r := row.copy()
		r.Answer = ""
		r.AnswerStart = -1
		return []*CsvRow{row}
	}
	rows := make([]*CsvRow, len(answers))
	for i, answer := range answers {
		r := row.copy()
		r.Answer = answer.Text
		r.AnswerStart = answer.AnswerStart
		rows[i] = r
	}
	return rows
}

type Paragraph struct {
	Context string `json:"context"`
	Qas     []*QnA `json:"qas"`
}

func (p *Paragraph) toCsvRows(title string) []*CsvRow {
	row := &CsvRow{
		Context: p.Context,
		Title:   title,
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
		rows = append(rows, p.toCsvRows(d.Title)...)
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
	ID           string
}
type ParagraphGroup struct {
	Paragraphs []*Paragraph
	Title      string
}
type Env struct {
	ContextIndex    int
	TitleIndex      int
	Answers         map[string]*Answer
	AnswerGroups    map[string]*AnswerGroup
	QAs             map[string]*QnA
	QAGroups        map[string][]*QnA
	Paragraphs      map[string]*Paragraph
	ParagraphGroups map[string]*SuQUADData
	Data            []*SuQUADData
}

func (o *Env) contextIndex(a *string) int {
	idx := o.ContextIndex
	o.ContextIndex++
	return idx
}
func (o *Env) titleIndex(a *string) int {
	idx := o.TitleIndex
	o.TitleIndex++
	return idx
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

	u, _ := uuid.NewRandom()
	id := u.String()
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

	b, err := json.Marshal(&answer_ids)
	if err != nil {
		return nil
	}
	id := fmt.Sprintf("%x", md5.Sum(b))
	o.AnswerGroups[id] = &AnswerGroup{Answers: a}
	return &id
}

func (o *Env) groupContextIdx(indices []int) int {
	return indices[0]
}
func (o *Env) addIsImpossible(a *string, b *string) *string {
	group := o.AnswerGroups[*b]
	group.IsImpossible = *a == "true"
	return b
}

func (o *Env) addID(a *string, b *string) *string {
	group := o.AnswerGroups[*b]
	if strings.TrimSpace(*a) == "" {
		group.ID = *b
	} else {
		group.ID = *a
	}
	return b
}

func (o *Env) collectQAs(answerID *string, b *string) *string {
	if ag, ok := o.AnswerGroups[*answerID]; ok {
		var s QnA
		if ag.IsImpossible {
			answers := make([]*Answer, 0, len(ag.Answers))
			for _, answer := range ag.Answers {
				if strings.TrimSpace(answer.Text) != "" {
					answers = append(answers, answer)
				}
			}
			s = QnA{
				ID:               ag.ID,
				Question:         *b,
				PlausibleAnswers: answers,
				Answers:          make([]*Answer, 0),
				IsImpossible:     ag.IsImpossible,
			}
		} else {
			s = QnA{
				ID:           ag.ID,
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
		u, _ := uuid.NewRandom()
		id := u.String()
		o.Paragraphs[id] = &s
		return &id
	}
	return nil
}

func (o *Env) groupParagraphs(ids []*string) *string {
	a := make([]*Paragraph, 0, len(ids))
	for _, id := range ids {
		if v, ok := o.Paragraphs[*id]; ok {
			a = append(a, v)
		}
	}
	u, _ := uuid.NewRandom()
	id := u.String()
	o.ParagraphGroups[id] = &SuQUADData{Paragraphs: a}
	return &id
}

func (o *Env) makeData(paraID *string, title *string) *string {
	if group, ok := o.ParagraphGroups[*paraID]; ok {
		group.Title = *title
		o.Data = append(o.Data, group)
	}
	return nil
}

func (o *Env) ToJSON(w io.Writer) error {
	s := SuQUAD{
		Version: "v2.0",
		Data:    o.Data,
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

func NewEnv() *Env {
	return &Env{
		TitleIndex:      0,
		ContextIndex:    0,
		Answers:         make(map[string]*Answer),
		QAs:             make(map[string]*QnA),
		AnswerGroups:    make(map[string]*AnswerGroup),
		QAGroups:        make(map[string][]*QnA),
		Paragraphs:      make(map[string]*Paragraph),
		ParagraphGroups: make(map[string]*SuQUADData),
		Data:            make([]*SuQUADData, 0),
	}
}

func main() {
	var showVersion bool
	var inFilePath string
	var outFilePath string
	var reverseFlag bool
	flag.BoolVar(&showVersion, "v", false, "show version")
	flag.BoolVar(&showVersion, "version", false, "show version")
	flag.BoolVar(&reverseFlag, "r", false, "reverse mode flag")
	flag.StringVar(&inFilePath, "i", "", "infile path")
	flag.StringVar(&outFilePath, "o", "", "outfile path")
	flag.Parse()
	if showVersion {
		fmt.Println("version:", version)
		return
	}
	if inFilePath == "" {
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
		env := NewEnv()
		qframe.ReadCSV(r).Drop("answer_check").Apply(
			qframe.Instruction{Fn: env.titleIndex, DstCol: "title_idx", SrcCol1: "title"},
			qframe.Instruction{Fn: env.contextIndex, DstCol: "context_idx", SrcCol1: "context"},
			qframe.Instruction{Fn: function.StrI, DstCol: "answer_start", SrcCol1: "answer_start"},
			qframe.Instruction{
				Fn:      env.createAnswer,
				SrcCol1: "answer_start",
				SrcCol2: "answer",
				DstCol:  "answer_id",
			},
		).Drop("frag_text", "answer", "answer_start").
			GroupBy(groupby.Columns("context", "question", "is_impossible", "id", "title")).
			Aggregate(qframe.Aggregation{
				Fn:     env.groupAnswers,
				Column: "answer_id",
				As:     "answer_id",
			}, qframe.Aggregation{
				Fn:     env.groupContextIdx,
				Column: "context_idx",
				As:     "context_idx",
			}, qframe.Aggregation{
				Fn:     env.groupContextIdx,
				Column: "title_idx",
				As:     "title_idx",
			}).
			Apply(qframe.Instruction{
				Fn:      function.StrB,
				SrcCol1: "is_impossible",
				DstCol:  "is_impossible",
			}, qframe.Instruction{
				Fn:      env.addIsImpossible,
				SrcCol1: "is_impossible",
				SrcCol2: "answer_id",
				DstCol:  "dummy1",
			}, qframe.Instruction{
				Fn:      env.addID,
				SrcCol1: "id",
				SrcCol2: "answer_id",
				DstCol:  "dummy2",
			}).Drop("dummy1", "dummy2").
			Sort(qframe.Order{Column: "title_idx"}, qframe.Order{Column: "id"}).
			Apply(qframe.Instruction{
				Fn:      env.collectQAs,
				SrcCol1: "answer_id",
				SrcCol2: "question",
				DstCol:  "question_id"},
			).
			Drop("id", "answer_id").
			GroupBy(groupby.Columns("context", "title")).
			Aggregate(qframe.Aggregation{
				Fn:     env.groupQAs,
				Column: "question_id",
				As:     "question_id",
			}, qframe.Aggregation{
				Fn:     env.groupContextIdx,
				Column: "context_idx",
				As:     "context_idx",
			}, qframe.Aggregation{
				Fn:     env.groupContextIdx,
				Column: "title_idx",
				As:     "title_idx",
			}).
			Sort(qframe.Order{Column: "context_idx"}).
			Drop("context_idx2").
			Apply(qframe.Instruction{
				Fn:      env.makeParagraph,
				SrcCol1: "question_id",
				SrcCol2: "context",
				DstCol:  "para_id"},
			).
			Drop("dummy").
			GroupBy(groupby.Columns("title")).
			Aggregate(qframe.Aggregation{
				Fn:     env.groupParagraphs,
				Column: "para_id",
				As:     "para_id",
			}, qframe.Aggregation{
				Fn:     env.groupContextIdx,
				Column: "title_idx",
				As:     "title_idx",
			}).
			Sort(qframe.Order{Column: "title_idx"}).
			Apply(qframe.Instruction{
				Fn:      env.makeData,
				SrcCol1: "para_id",
				SrcCol2: "title",
				DstCol:  "dummy"},
			)
		if err := env.ToJSON(w); err != nil {
			log.Fatal(err)
		}
	}
}
