package store

type AppendMessageResut struct {
	appendMessageStatus int32
	wroteOffset         int32
	wroteBytes          int32
	msgId               string
	storeTimestamp      int64
	logicsOffset        int64
}

const (
	PUT_OK                = 0
	END_OF_FILE           = 1
	MESSAGE_SIZE_EXCEEDED = 2
	UNKNOWN_ERROR         = 3
)
