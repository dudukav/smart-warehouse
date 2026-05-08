package events

import "fmt"

type ValidationError struct {
	Code    string
	Message string
	Field   string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}

	return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Field)
}

func NewValidationError(code, field, message string) *ValidationError {
	return &ValidationError{
		Code:    code,
		Field:   field,
		Message: message,
	}
}
