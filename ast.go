package ultimate_db

import (
	"errors"
	"fmt"
	"strings"
)

type Query interface {
	Execute(s *SegmentSearcher) *RoaringBitmap
}

type TermQuery struct{ Term string }

func (q *TermQuery) Execute(s *SegmentSearcher) *RoaringBitmap {
	ids := s.FetchPostings(q.Term)
	bm := NewRoaringBitmap()
	for _, id := range ids {
		bm.Add(id)
	}
	return bm
}

type AndQuery struct{ Left, Right Query }

func (q *AndQuery) Execute(s *SegmentSearcher) *RoaringBitmap {
	return RoaringIntersect(q.Left.Execute(s), q.Right.Execute(s))
}

type OrQuery struct{ Left, Right Query }

func (q *OrQuery) Execute(s *SegmentSearcher) *RoaringBitmap {
	return RoaringUnion(q.Left.Execute(s), q.Right.Execute(s))
}

type NotQuery struct{ Left, Right Query }

func (q *NotQuery) Execute(s *SegmentSearcher) *RoaringBitmap {
	return RoaringDifference(q.Left.Execute(s), q.Right.Execute(s))
}

type Parser struct {
	tokens []string
	pos    int
	depth  int
}

func ParseQuery(input string) (Query, error) {
	input = strings.ReplaceAll(input, "(", " ( ")
	input = strings.ReplaceAll(input, ")", " ) ")
	p := &Parser{tokens: strings.Fields(input)}
	return p.parseExpression()
}

func (p *Parser) current() string {
	if p.pos >= len(p.tokens) { return "" }
	return p.tokens[p.pos]
}

func (p *Parser) consume() string {
	token := p.current()
	p.pos++
	return token
}

func (p *Parser) parseExpression() (Query, error) {
	p.depth++
	if p.depth > MaxQueryDepth { return nil, errors.New("query exceeded max nesting") }
	defer func() { p.depth-- }()

	left, err := p.parseTerm()
	if err != nil { return nil, err }

	for p.current() == "OR" {
		p.consume()
		right, err := p.parseTerm()
		if err != nil { return nil, err }
		left = &OrQuery{Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseTerm() (Query, error) {
	p.depth++
	if p.depth > MaxQueryDepth { return nil, errors.New("query exceeded max nesting") }
	defer func() { p.depth-- }()

	left, err := p.parseFactor()
	if err != nil { return nil, err }

	for p.current() == "AND" || p.current() == "NOT" {
		op := p.consume()
		right, err := p.parseFactor()
		if err != nil { return nil, err }

		if op == "AND" {
			left = &AndQuery{Left: left, Right: right}
		} else {
			left = &NotQuery{Left: left, Right: right}
		}
	}
	return left, nil
}

func (p *Parser) parseFactor() (Query, error) {
	p.depth++
	if p.depth > MaxQueryDepth { return nil, errors.New("query exceeded max nesting") }
	defer func() { p.depth-- }()

	token := p.current()
	if token == "(" {
		p.consume()
		expr, err := p.parseExpression()
		if err != nil { return nil, err }
		if p.consume() != ")" { return nil, fmt.Errorf("missing closing parenthesis") }
		return expr, nil
	}
	if token == "" || token == ")" || token == "AND" || token == "OR" || token == "NOT" {
		return nil, fmt.Errorf("unexpected token: %s", token)
	}
	p.consume()
	
	cleaned := Tokenize(token)
	if len(cleaned) == 0 {
		return &TermQuery{Term: ""}, nil
	}
	return &TermQuery{Term: cleaned[0]}, nil
}
