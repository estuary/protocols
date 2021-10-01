package sql

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

// Renderer is used for naming things inside of SQL. It can be used for fields or values to
// handle sanitization and quoting.
type Renderer struct {
	// If set, will sanitize field before rendering by passing it to this function
	Sanitizer SanitizerFunc // func(string) string
	// If set, will wrap value after optionally checking SkipWrapper
	Wrapper WrapperFunc // func(string) string
	// If set, will check *sanitized* value to see if it should wrap. If unset, always wraps.
	SkipWrapper SkipWrapperFunc // func(string) bool
}

// SanitizerFunc takes a string and returns a sanitized string
type SanitizerFunc func(string) string

// WrapperFunc takes a string and returns the string wrapped according to the functions rules
type WrapperFunc func(string) string

// SkipWrapperFunc takes a string and returns if the wrapper function should be skipped
type SkipWrapperFunc func(string) bool

var (
	// DefaultUnwrappedIdentifiers.MatchString is a SkipWrapper function that checks for identifiers that typically do not need wrapping
	DefaultUnwrappedIdentifiers = regexp.MustCompile(`^[_\pL]+[_\pL\pN]*$`)
)

// Write takes a writer and renders text based on it's configuration
func (r *Renderer) Write(w io.Writer, text string) (int, error) {
	if r == nil {
		return w.Write([]byte(text))
	}
	if r.Sanitizer != nil {
		text = r.Sanitizer(text)
	}
	if r.SkipWrapper != nil && r.SkipWrapper(text) {
		return w.Write([]byte(text))
	}
	if r.Wrapper == nil {
		w.Write([]byte(text))
	}
	return w.Write([]byte(r.Wrapper(text)))
}

// Render takes a string and renders text based on it's configuration
func (r *Renderer) Render(text string) string {
	var b = new(strings.Builder)
	_, _ = r.Write(b, text)
	return b.String()
}

// Sanitize uses a SanitizerFunc or returns the original string if it's nil
func (f SanitizerFunc) Sanitize(text string) string {
	if f == nil {
		return text
	}
	return f(text)
}

// Wrap uses a WrapperFunc or returns the original string if it's nil
func (f WrapperFunc) Wrap(text string) string {
	if f == nil {
		return text
	}
	return f(text)
}

// SkipWrapper uses a SkipWrapperFunc or returns false if it's nil
func (f SkipWrapperFunc) SkipWrapper(text string) bool {
	if f == nil {
		return false
	}
	return f(text)
}

// TokenPair is a generic way of representing strings that can be used to surround some text for
// quoting and commenting.
type TokenPair struct {
	Left  string
	Right string
}

// Wrap returns the given string surrounded by the strings in this TokenPair.
func (p *TokenPair) Wrap(text string) string {
	if p == nil {
		return text
	}
	return p.Left + text + p.Right
}

// Write takes an io.Writer and writes out the wrapped text.
// This function is leveraged for writing comments
func (p *TokenPair) Write(w io.Writer, text string) (int, error) {
	if p == nil {
		return w.Write([]byte(text))
	} else {
		// p.Left
		total, err := w.Write([]byte(p.Left))
		if err != nil {
			return total, err
		}
		// text
		next, err := w.Write([]byte(text))
		if err != nil {
			return total + next, err
		}
		total += next
		// p.Right
		next, err = w.Write([]byte(p.Right))
		if err != nil {
			return total + next, err
		}
		total += next

		return total, nil
	}
}

// NewTokenPair returns a TokenPair with the left and right tokens specified
func NewTokenPair(l, r string) *TokenPair {
	return &TokenPair{
		Left:  l,
		Right: r,
	}
}

// DoubleQuotes returns a TokenPair with a single double quote character on the both the Left and
// the Right.
func DoubleQuotes() *TokenPair {
	return &TokenPair{
		Left:  "\"",
		Right: "\"",
	}
}

// SingleQuotes returns a TokenPair with one single quote character on the both the Left and
// the Right.
func SingleQuotes() *TokenPair {
	return &TokenPair{
		Left:  "'",
		Right: "'",
	}
}

// Backticks returns a TokenPair with a single backtick character on the both the Left and
// the Right.
func Backticks() *TokenPair {
	return &TokenPair{
		Left:  "`",
		Right: "`",
	}
}

// CommentRenderer is used to render comments
type CommentRenderer struct {
	// Linewise determines whether to render line or block comments. If it is true, then each line
	// of comment text will be wrapped separately. If false, then the entire multi-line block of
	// comment text will be wrapped once.
	Linewise bool
	// Wrap holds the strings that will bound the beginning and end of the comment.
	Wrap *TokenPair
}

// Write renders a comment based on the rules
func (cr *CommentRenderer) Write(w io.Writer, text string, indent string) (int, error) {

	var scanner = bufio.NewScanner(strings.NewReader(text))

	// Inline function to handle writes and accumulate total bytes written.
	var total int
	var write = func(text string) error {
		n, err := w.Write([]byte(text))
		total += n
		return err
	}

	// Each line needs wrapped in a comment
	if cr.Linewise {
		var first = true
		for scanner.Scan() {
			if !first {
				// Write newline and indent
				if err := write("\n"); err != nil {
					return total, err
				}
				if err := write(indent); err != nil {
					return total, err
				}
			}
			first = false
			// Wrap.Left
			if err := write(cr.Wrap.Left); err != nil {
				return total, err
			}
			// text
			if err := write(scanner.Text()); err != nil {
				return total, err
			}
			// Wrap.Right
			if err := write(cr.Wrap.Right); err != nil {
				return total, err
			}
		}
	} else {
		// Wrap.Left
		if err := write(cr.Wrap.Left); err != nil {
			return total, err
		}
		var first = true
		for scanner.Scan() {
			if !first {
				// Write newline and indent
				if err := write("\n"); err != nil {
					return total, err
				}
				if err := write(indent); err != nil {
					return total, err
				}
			}
			first = false
			// text
			if err := write(scanner.Text()); err != nil {
				return total, err
			}
		}
		// Wrap.Right
		if err := write(cr.Wrap.Right); err != nil {
			return total, err
		}
	}

	// Comments always end with a newline
	if err := write("\n"); err != nil {
		return total, err
	}

	return total, nil

}

// Render takes a string and renders text based on it's configuration
func (cr *CommentRenderer) Render(text string) string {
	var b = new(strings.Builder)
	_, _ = cr.Write(b, text, "")
	return b.String()
}

// LineCommentRenderer returns a per line comment valid for standard SQL
func LineCommentRenderer() *CommentRenderer {
	return &CommentRenderer{
		Linewise: true,
		Wrap: &TokenPair{
			Left:  "-- ",
			Right: "",
		},
	}
}
