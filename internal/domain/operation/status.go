package operation

type OperationType int16

const (
	OperationTypeUnknown          OperationType = iota // 0
	OperationTypeSendWelcomeEmail                      // 1
	OperationTypeRecalculateStats                      // 2
)

func (t OperationType) String() string {
	switch t {
	case OperationTypeSendWelcomeEmail:
		return "send_welcome_email"
	case OperationTypeRecalculateStats:
		return "recalculate_stats"
	default:
		return "unknown"
	}
}

type Status int16

const (
	StatusUnknown   Status = iota // 0
	StatusPlanned                 // 1
	StatusCompleted               // 2
	StatusFailed                  // 3
	StatusCancelled               // 4
)

func (s Status) String() string {
	switch s {
	case StatusPlanned:
		return "planned"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

func (s Status) IsPlanned() bool { return s == StatusPlanned }
